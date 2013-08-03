# Twitter Followers Script

# Standard Library
from itertools import chain
from datetime import datetime, timedelta

# woo. monkey patching
from gevent import monkey; monkey.patch_all()

# Vendor
from twitter import Api
from gevent import sleep, spawn
from gevent.queue import JoinableQueue, Empty
from pymongo import MongoClient

# Local
from twitter_config import api_keys as apis

# -- Configuration --
sleep_time = 60*15 # 15 reqs per 15 minutes
mongo_uri = 'mongodb://localhost'

# -- Primary Types --

def grab(id, api_args, inq, outq):
	'''Grab twitter followers from a queue of handles'''
	api = Api(**api_args)

	if api.VerifyCredentials().id is None:
		raise Exception('Grabber %s couldnt ensure connection' % id)

	try:
		while True:
			handle = inq.get()
			while True:
				try:
					# For each handle use 3 requests. Each most of the time will only be called once
					# and each is in its own bucket of rate limiting
					user = api.UsersLookup(screen_name=handle)[0]
					followers = api.GetFollowerIDs(screen_name=handle)
					friends = api.GetFriendIDs(screen_name=handle)
					print 'Grabbed followers for %s (%s)' % (handle, id)
					outq.put({
						'handle': handle,
						'user': {
							'id': user.id,
							'friends_count': user.friends_count,
							'followers_count': user.followers_count,
							'name': user.name,
							'time_zone': user.time_zone,
							'location': user.location,
							'favourites_count': user.favourites_count,
							'listed_count': user.listed_count,
							'statuses_count': user.statuses_count
						},
						'followers': followers,
						'friends': friends
					})
					break
				except Exception as e:
					try:
						if e.message[0][u'code'] == 88:
							print 'Rate Limited: Sleeping for 15 minutes. Waking up at %s' % (datetime.now() + timedelta(seconds=sleep_time))
							sleep(sleep_time)
						elif e.message[0][u'code'] == 34:
							print 'User %s does not exist.' % handle
							outq.put({
								'handle': handle,
								'delete': True
							})
							break
						else:
							raise Exception('fake')
					except:
						try:
							print 'Error: Grabber %s encountered exception. Prevented getting %s' % (id, handle), e
						except:
							pass
						finally:
							break
			inq.task_done()

	except Empty:
		print 'Grabber %s queue empty' % id

def get(mongo, q):
	n = 0
	for doc in mongo.rc.twitter.find({}, {'handle': 1, '_id': 0}):
		n += 1
		q.put(doc['handle'])

	print 'Getter found %d handles' % n

def save(mongo, q):
	try:
		while True:
			obj = q.get()
			try:
				if obj.get('delete', False):
					mongo.rc.twitter.remove({'handle': obj['handle']})
					print 'Removed twitter handle %s because its deleted.' % obj['handle']
				else:
					mongo.rc.twitter.graph.insert(obj)
					mongo.rc.twitter.remove({'handle': obj['handle']})
					print 'Saved twitter connections for %s' % obj['handle']
			except Exception as e:
				print 'Error saving connections for %s:' % obj['handle'], e, obj
			finally:
				q.task_done()
	except Empty:
		print 'Setter queue empty', e

def main(apis):
	inq = JoinableQueue()
	outq = JoinableQueue()
	mongo = MongoClient(mongo_uri)

	getter = spawn(get, mongo, inq)
	grabbers = [spawn(grab, name, api_args, inq, outq) for name, api_args in apis.items()]
	saver = spawn(save, mongo, outq)

	[greenlet.join() for greenlet in [getter,saver] + grabbers]
	print 'All done!'

# -- Main Code --
if __name__ == '__main__':
	main(apis)
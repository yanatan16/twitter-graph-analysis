# Twitter Followers Script

# Vendor
from pymongo import MongoClient

# -- Configuration --
mongo_uri = 'mongodb://localhost'
handle_file = 'unique-handles-cleaned.txt'

mongo = MongoClient(mongo_uri)

def insert(handle):
	print 'Inserting handle %s' % handle
	mongo.rc.twitter.update({'handle': handle}, {'handle': handle}, upsert=True)

def handles(fn):
	with open(fn) as f:
		for line in f:
			names = [s.strip() for s in line.split('@') if s.strip() != '']
			for handle in names:
				yield handle

def main():
	map(insert, handles(handle_file))

# -- Main Code --
if __name__ == '__main__':
	main()
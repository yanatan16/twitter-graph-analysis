# Twitter Create Graph Script

# Vendor
import graph_tool.all as gt
from pymongo import MongoClient

# Local
from util import class_memoize

# -- Configuration --
mongo_uri = 'mongodb://vps.joneisen.me'
directed = False
n = 10
filename = 'twitter-graph.xml.gz'

# -- Primary Types --
def main():
	mongo = MongoClient(mongo_uri)
	users = mongo.rc.twitter.graph.find(limit=n)
	print 'Retrieved %d users to create a graph with' % n
	g = GraphMaker(users, directed).graphicize()
	print 'Graph made, now saving as %s' % filename
	g.save(filename)


class GraphMaker:
	def __init__(self, users, directed=True):
		self.users = users
		self.g = gt.Graph(directed=directed)
		self.vid = self.g.new_vertex_property('string')
		self.g.vertex_properties['id'] = self.vid

	def graphicize(self):
		for user in self.users:
			v = self.vertex(user['user']['id'])
			out_neighbours = set(v.out_neighbours()) # empty if directed
			in_neighbours = set(v.in_neighbours()) # empty if directed

			for follower in user['followers']:
				vf = self.vertex(follower)

				if vf not in out_neighbours:
					self.g.add_edge(v, vf)

			for friend in user['friends']:
				vf = self.vertex(friend)

				if vf not in in_neighbours:
					self.g.add_edge(vf, v)

		return self.g

	@class_memoize
	def vertex(self, id):
		'''Create a vertex (with memoization)'''
		v = self.g.add_vertex()
		self.vid[v] = id
		return v

# -- Main Code --
if __name__ == '__main__':
	main()
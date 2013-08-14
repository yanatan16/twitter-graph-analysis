# Twitter Create Graph Script

# Builtin
from itertools import *
import random; random.seed()

# Vendor
import graph_tool.all as gt
from pymongo import MongoClient

# -- Configuration --
mongo_uri = 'mongodb://localhost'
directed = True
n = 10000
skip = 8
filename = 'twitter-graph.xml.gz'
imagename = 'twitter-graph-lg.pdf'
blocks = 20
iterations = 50000
mindegree = 5
show = False

# -- Primary Types --
def create():
	mongo = MongoClient(mongo_uri)
	users = mongo.rc.twitter.graph.find(limit=n * (1+skip), timeout=False)
	print 'Retrieved %d users to create a graph with' % n

	users = skipevery(users, skip)
	users = printevery(users, 10, 'On user %d')
	g = make_graph(users, directed)

	if show:
		print 'Filtering complete. Now calculating base layout'
		g.vp['pos'] = gt.sfdp_layout(g)

	print 'Graph %s made, now saving as %s' % (g, filename)
	g.save(filename)

def show():
	g = gt.load_graph(filename)
	kwargs = {}
	print 'Graph loaded, now drawing to %s' % imagename

	if 'blocks' in g.vp:
		kwargs['vertex_fill_color'] = g.vertex_properties['blocks']
	if 'rank' in g.vp:
		kwargs['vertex_size'] = gt.prop_to_size(g.vp['rank'], mi=5, ma=15)
	if 'pos' in g.vp:
		kwargs['pos'] = g.vp['pos']

	gt.graph_draw(g, vertex_shape=g.vp['rc'], output=imagename, **kwargs)

def community():
	g = gt.load_graph(filename)
	print 'Graph loaded, now finding community'
	# state = gt.BlockState(g, B=blocks)
	# for i in xrange(iterations):
	# 	if i < iterations / 2:
	# 		gt.mcmc_sweep(state)
	# 	else:
	# 		gt.mcmc_sweep(state, beta=float('inf'))

	# g.vp['blocks'] = state.get_blocks()

	spins = {}
	if 'blocks' in g.vp:
		spins = {'spins': g.vp['blocks']}

	g.vp['blocks'] = gt.community_structure(g, n_iter=iterations, n_spins=blocks, **spins)

	if 'pos' in g.vp:
		gt.sfdp_layout(g, groups=g.vp['blocks'], pos=g.vp['pos'])

	for i in xrange(blocks):
		print '%d nodes in block %d' % (len(gt.find_vertex(g, g.vp['blocks'], i)), i)

	g.save(filename)

def central():
	g = gt.load_graph(filename)
	print 'Graph loaded, now calculating centrality'
	pr = gt.pagerank(g)
	g.vp['rank'] = pr
	g.save(filename)

def top():
	g = gt.load_graph(filename)
	print 'Graph loaded, now calculating top nodes'
	vblocks = g.vp['blocks']
	largest_block = max(range(blocks), key=lambda b: len(gt.find_vertex(g, vblocks, b)))
	print 'Largest block is %d with %d nodes' % (largest_block, len(gt.find_vertex(g, vblocks, largest_block)))

	for tup in top_ids(g, largest_block):
		print tup

def top_ids(g, block, n=10):
	'''List the most central node ids in a given block'''
	vid = g.vp['id']
	vrank = g.vp['rank']
	vertices = gt.find_vertex(g, g.vp['blocks'], block)
	sorted_vertices = sorted(vertices, key=lambda v: vrank[v], reverse=True)
	mapped_vertices = imap(lambda v: (vid[v], vrank[v]), sorted_vertices)

	return take(n, mapped_vertices)


def make_graph(users, directed=True):
	g = gt.Graph(directed=directed)
	vid = g.new_vertex_property('string')
	vrc = g.new_vertex_property('bool')
	g.vp['id'] = vid
	g.vp['rc'] = vrc

	vertex = lambda id: get_vertex(g, vid, id)
	edge = lambda v1, v2: add_edge(g, v1, v2)

	for user in users:
		id = user['user']['id']
		v = vertex(id)

		for friend in user['friends']:
			edge(v, vertex(friend))

		for follower in user['followers']:
			edge(vertex(follower), v)

	return filter_graph(g)

def filter_graph(g):
	vdeg = g.new_vertex_property('bool')
	for v in g.vertices():
		vdeg[v] = v.in_degree() + v.out_degree() > mindegree

	print 'Setting vertex filter at %d' % mindegree
	g.set_vertex_filter(vdeg)
	g.purge_vertices()
	return g

# -- Helpers --
vertices = {}
def get_vertex(g, vid, id):
	try:
		# return gt.find_vertex(g, vid, id)[0]
		return vertices[id]
	except KeyError:
		v = g.add_vertex()
		vid[v] = id
		vertices[id] = v
		return v

def add_edge(g, v1, v2):
	# if not g.edge(v1, v2):
	g.add_edge(v1, v2)

def skipevery(iter, n):
	while True:
		for i in xrange(n):
			y = next(iter)
		yield y

def printevery(iter, n, stmt):
	for i, el in enumerate(iter):
		if i % n == 0:
			print stmt % i
		yield el

def take(n, iterable):
	"Return first n items of the iterable as a list"
	return list(islice(iterable, n))

# -- Main Code --
actions = {
	'show': show,
	'create': create,
	'community': community,
	'central': central,
	'top': top
}

if __name__ == '__main__':
	import sys
	if len(sys.argv) > 1:
		for action in sys.argv[1:]:
			if action in actions:
				actions[action]()
			else:
				print 'Unknown action %s' % action
	else:
		create()
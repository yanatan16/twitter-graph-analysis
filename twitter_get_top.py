# Twitter Get top ids script

# Builtin
from itertools import imap, islice
import random; random.seed()

# Vendor
import graph_tool.all as gt
from twitter import Api

# Local
from twitter_config import api_keys


# -- Configuration --
from twitter_create_graph import filename
from twitter_create_graph import blocks as nblocks
n = 10

# -- Primary Types --
api = lambda: Api(**random.choice(api_keys.values()))

def top():
	g = gt.load_graph(filename)
	print 'Graph loaded, now calculating top nodes'
	vblocks = g.vp['blocks']
	blocks = sorted(range(nblocks), key=lambda b: len(gt.find_vertex(g, vblocks, b)), reverse=True)

	for block in blocks:
		print 'Block %d with %d nodes' % (block, len(gt.find_vertex(g, vblocks, block)))
		tups = top_ids(g, block, n)
		ids = [t[0] for t in tups]
		names = get_names(ids)

		for tup, name in zip(tups, names):
			print name, tup[0], tup[1]

def top_ids(g, block, n=10):
	'''List the most central node ids in a given block'''
	vid = g.vp['id']
	vrank = g.vp['rank']
	vertices = gt.find_vertex(g, g.vp['blocks'], block)
	sorted_vertices = sorted(vertices, key=lambda v: vrank[v], reverse=True)
	mapped_vertices = imap(lambda v: (vid[v], vrank[v]), sorted_vertices)

	return take(n, mapped_vertices)

def get_names(ids):
	users = api().UsersLookup(user_id=ids):
	return [u.screen_name for u in users]


def take(n, iterable):
	"Return first n items of the iterable as a list"
	return list(islice(iterable, n))

if __name__ == '__main__':
	top()
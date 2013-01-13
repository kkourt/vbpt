#!/usr/bin/env python

from pprint import pprint

from pychart import *

from logparse import LogParser
from dict_hier import dhier_reduce, dhier_reduce_many
from kkstats import StatList
from xdict import xdict
from humanize_nr import humanize_nr

class ThreadStats(object):
	def __init__(self):
		self.st = {}
		self.tid = None

	def set_thread(self, tid):
		assert tid not in self.st
		self.st[tid] = {}
		self.tid     = tid

	def set_thread_prop(self, key, val):
		assert self.tid is not None
		self.st[self.tid][key] = val

	def reset(self):
		self.__init__()

	def get_st(self):
		return self.st

TS = ThreadStats()

parse_conf = r'''
/^nthreads:(\d+).*ticks_per_op:(\d+).*$/
	nthreads = int(_g1)
	ticks_per_op = int(_g2)

/^ *ALL_ticks.*ticks \[ *(\d+)\]$/
	all_ticks = int(_g1)

/^Using (\d+) threads \[cpus: ([\d ]+).*$/
	threads = int(_g1)
	cpus    = map(int, _g2.split())
	eval TS.reset()

/^PS> range_len:(\d+) ins0:(\d+) tx_keys:(\d+) tx_range:(\d+) ntxs:(\d+)$/
	range_len  = int(_g1)
	ins0       = int(_g2)
	tx_keys    = int(_g3)
	tx_range   = int(_g4)
	ntxs       = int(_g5)

/^T:\s+(\d+) \[tid:(\d+)\].*$/
	eval TS.set_thread(int(_g1))
	eval TS.set_thread_prop('tid', int(_g2))

/^ *(\w+): ticks:[^\[]*\[ *(\d+)\].*cnt:[^\[]*\[ *(\d+)\].*max:[^\[]*\[ *(\d+)\].*min:[^\[]*\[ *(\d+)\].*$/
	eval TS.set_thread_prop(_g1, xdict({'ticks': int(_g2), 'cnt': int(_g3), 'min': int(_g4), 'max': int(_g5)}))

/^ *(\w+): *(\d+)$/
	eval TS.set_thread_prop(_g1, int(_g2))

/^DONE$/
	thread_stats = TS.get_st()
	flush
'''


def do_parse(fname):
	f = open(fname)
	lp = LogParser(parse_conf, debug=False, globs={'TS':TS, 'xdict':xdict}, eof_flush=False)
	lp.go(f)
	pprint(lp.data)
	d = dhier_reduce_many(
		lp.data,
		("tx_keys", "nthreads"),
		map_fn=lambda lod: StatList((x['ticks_per_op'] for x in lod))
	)
	pprint(d)
	#return d


#TODO: 2 graphs
#  a number of different lines based on @tx_keys that show scalability (perf vs threads)
#  bar of commit time vs insert time for (threads/@tx_keys) combinations

def do_plot(d, fname="plot.pdf"):
	theme.use_color = True
	theme.get_options()
	canv = canvas.init(fname=fname, format="pdf")
	ar  = area.T(
		x_axis  = axis.X(label="/10{}cores", format="%d"),
		y_axis  = axis.Y(label="/10{}ticks per op", format="%d"),
		y_range = [0,None],
		size = (400,200)
	)

	for rles, rles_d in d.iteritems():
		for xarr, xarr_d in rles_d.iteritems():
			k = "%5s %5.0f%s" % ((xarr, ) + humanize_nr(rles))
			x = []
			for (n, n_d) in xarr_d.iteritems():
				n_d.setfn(lambda x : x /(1000*1000))
				x.append((n, n_d.avg, n_d.avg_minus, n_d.avg_plus))
			print x
			ar.add_plot(line_plot.T(
				data=x,
				label=k,
				y_error_minus_col=3,
				y_error_plus_col=3,
				error_bar = error_bar.error_bar2(tic_len=5, hline_style=line_style.gray50),
				tick_mark=tick_mark.X(size=3),
			))


	ar.draw()
	canv.close()

from sys import argv
if __name__ == '__main__':
	d = do_parse(argv[1])
	#do_plot(d, fname=argv[1] + "-plot.pdf")

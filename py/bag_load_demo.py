"""Demo loading cubes into a dask bag. Time loading with and without bag."""

import glob
from multiprocessing.pool import ThreadPool
import os
import time

import dask
import dask.bag as db
import dask.delayed as delayed
from distributed import Client

import iris


def timer(func, *funcargs):
    """
    Simply times the runtime of `func` by recording the time before and after
    the call.

    """
    t0 = time.time()
    func(*funcargs)
    t1 = time.time()
    return t1 - t0


def repeater(repeat, *timerargs):
    """
    Run a timer func `repeat` times.

    """
    if repeat <= 1:
        result = timer(*timerargs)
    else:
        result = [timer(*timerargs) for _ in range(repeat)]
    return result


def direct_load(fp, pattern):
    """Load datasets at the filepath `fp` using Iris."""
    iris.load(os.path.join(fp, pattern))


def withbag(seq):
    """
    Load a number of individual datasets in a sequence using Iris.

    This is a little more complex as we need to generate a sequence and map
    that sequence onto a load call. The dask bag is generated from that
    sequence.

    """
    cs = db.from_sequence(seq).map(lambda fn: iris.load(fn))
    iris.cube.CubeList(cs.compute())


def delay_wrapper(fp, pattern):
    """
    Wrap a direct load call in a delayed call and construct a CubeList from the
    delayed object.

    """
    dlyd = delayed(direct_load(fp, pattern))
    cs = db.from_delayed(dlyd)
    iris.cube.CubeList(cs.compute(get=dask.multiprocessing.get))
    # iris.cube.CubeList(cs.compute())


def main():
    # fp = '/project/applied/OECD/data/original_data/tas/rcp26'
    fp = '/project/euro4_hindcast/WIND-ATLAS_EURO4-RERUN/2015/06/18Z'
    # fn = 'EURO4_2015060[1-3].pp'
    # fn = 'EURO4_2015060*.pp'
    fn = '*.pp'
    seq = glob.glob(os.path.join(fp, fn))
    reps = 5

    # direct_nums = repeater(reps, direct_load, fp, fn)
    # print direct_nums
    # withbag_nums = repeater(reps, withbag, seq)
    # print withbag_nums
    # delay_nums = repeater(reps, delay_wrapper, fp, fn)
    # print delay_nums
    print timer(delay_wrapper, fp, fn)


if __name__ == '__main__':
    import sys
    try:
        args = sys.argv[1:]
    except ValueError:
        client = None
    else:
        if len(args) == 2:
            host, port = args
            client = Client('{}:{}'.format(str(host), str(port)))
        elif len(args) == 1:
            arg, = args
            client = Client('{}'.format(args[0]))
        elif len(args) == 0:
            client = None
        else:
            emsg = ('Expected either one ("host:port:) or two '
                    '("host port") args, got {}.')
            raise IOError(emsg.format(len(args)))
    if client is None:
        dask.set_options(pool=ThreadPool(8))
    print dask.context._globals
    main()

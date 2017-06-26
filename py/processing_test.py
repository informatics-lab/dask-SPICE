"""
Some example processing to test dask parallel processing.

Use EURO4 hindcast data of u&v wind taken from:
    `/project/euro4_hindcast/WIND-ATLAS_EURO4-RERUN/2015/06`,

which has shape:
    `time:31, model_level_number:22, grid_lat:1000, grid_lon:1100`,

one for each day in June 2015.

Take only the first timestep and the first five model levels for each day, and
calculate wind speed and direction.

Compare variance in wind speed and direction over model levels.

"""

from multiprocessing.pool import ThreadPool
import os
import time

import dask
import dask.array as da
import dask.bag as db
from dask.delayed import delayed
from distributed import Client
import iris


def timer(func):
    """
    Simply times the runtime of `func` by recording the time before and after
    the call.

    """
    t0 = time.time()
    func()
    t1 = time.time()
    return t1 - t0


@delayed
def load_subset(fp, fn):
    t_cstr = iris.Constraint(time=lambda cell: cell.point.hour == 6)
    mln_cstr = iris.Constraint(model_level_number=lambda v: v < 6)
    with iris.FUTURE.context(cell_datetime_objects=True):
        cube = iris.load(os.path.join(fp, fn), t_cstr & mln_cstr)
    return cube


def xy_to_wspd_and_dir(x_cube, y_cube):
    """
    Post-processing, part 1: mathematics.
    Converting x and y wind to speed and direction.

    """
    wspd_data = (x_cube.core_data()**2 + y_cube.core_data()**2) ** 0.5
    wspd_cube = x_cube.copy(data=wspd_data)
    wspd_cube.rename('wind_speed')
    wspd_cube.units = 'm s-1'

    theta_data = da.arctan(x_cube.core_data() / y_cube.core_data())
    theta_cube = y_cube.copy(data=theta_data)
    theta_cube.rename('wind_from_direction')
    theta_cube.units = 'degrees'

    return wspd_cube, theta_cube


def mln_variance(wspd_cube, wdir_cube):
    """
    Post-processing, part 2: statistical analysis.
    Calculate the variance in wind speed and direction over model levels.

    """
    wspd_var_cube = wspd_cube.collapsed('model_level_number',
                                        iris.analysis.VARIANCE)
    wdir_var_cube = wdir_cube.collapsed('model_level_number',
                                        iris.analysis.VARIANCE)
    return wspd_var_cube, wdir_var_cube


def main():
    fp = '/project/euro4_hindcast/WIND-ATLAS_EURO4-RERUN/2015/06/18Z/'
    # fn = 'EURO4_2015060[1-3].pp'
    fn = '*.pp'
    dlyd = load_subset(fp, fn)
    cs = db.from_delayed(dlyd)
    cubes = iris.cube.CubeList(cs.compute())
    # The x- and y-wind cubes are on different domains. This notwithstanding,
    # the x-wind cube also has one more latitude point than the y-wind cube,
    # which we arbitrarily chop off.
    x_wind_cube = cubes[0][..., :-1, :]
    y_wind_cube = cubes[1]

    wspd_cube, theta_cube = xy_to_wspd_and_dir(x_wind_cube, y_wind_cube)

    wspd_var_cube, wdir_var_cube = mln_variance(wspd_cube, theta_cube)
    print wspd_var_cube
    print wdir_var_cube
    wspd_var_data = wspd_var_cube.data
    wdir_var_data = wdir_var_cube.data


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
    # if client is None:
    #     dask.set_options(pool=ThreadPool(8), get=dask.threaded.get)
    print dask.context._globals
    print timer(main)

"""Create and run a local dask scheduler with worker(s) on SPICE."""

from contextlib import contextmanager
import os
import subprocess
from threading import Thread

from distributed import Scheduler
from tornado.ioloop import IOLoop


class LocalScheduler(object):
    """
    Set up a local distributed scheduler and linked workers on SPICE.

    """
    def __init__(self, n_workers, port=8786,
                 tmp_file='/var/tmp/spice_runner.sh'):
        self.n_workers = n_workers
        self.port = port
        self.tmp_file = tmp_file

        self._loop = None
        self._scheduler = None

    @property
    def scheduler(self):
        if self._scheduler is None:
            self.start_scheduler()
        return self._scheduler

    @scheduler.setter
    def scheduler(self, value):
        self._scheduler = value

    @property
    def endpoint(self):
        if self._scheduler is None:
            # Start the scheduler if there isn't one.
            self.start_scheduler()
        return '{}:{}'.format(self.scheduler.ip, self.scheduler.port)

    @contextmanager
    def _temp_file(self, content):
        try:
            with open(self.tmp_file, 'w') as otfh:
                otfh.writelines(content)
            os.chmod(self.tmp_file, 0744)
            with open(self.tmp_file, 'r') as runner:
                yield runner
        finally:
            os.remove(self.tmp_file)

    def stop(self):
        # XXX: This doesn't work!
        self.scheduler.close()
        self._loop.close()

    def start_scheduler(self, **dask_kw):
        """
        Start the scheduler. `dask_kw` are passed straight to the scheduler
        constructor.

        """
        self._loop = IOLoop.current()
        t = Thread(target=self._loop.start)
        t.start()

        s = Scheduler(loop=self._loop, **dask_kw)
        s.start(self.port)
        self._scheduler = s

    def start_workers(self, queue='normal', memory=1000, walltime=30,
                      err_file='job-%N-%j.err', log_file='job-%N-%j.err',
                      module_name='scitools',
                      worker_script='spice_run_worker.sh'):
        """
        Start a number of workers defined by `self.n_workers`.

        Workers run in a batch submission job on SPICE.

        """
        script_content = self._slurm_submit_file(queue, memory, walltime,
                                                 err_file, log_file,
                                                 module_name, worker_script)
        with self._temp_file(script_content) as runner_script:
            subprocess.check_call('sbatch', stdin=runner_script)

    def _slurm_submit_file(self, queue, memory, walltime, err_file, log_file,
                           module_name, worker_script):
        """
        Create a temporary batch submission document for running workers on
        SPICE.

        """
        here_doc = ("#!/bin/bash -l\n"
                    "#SBATCH --qos={queue}\n"
                    "#SBATCH --mem={mem}\n"
                    "#SBATCH --ntasks={self.n_workers}\n"
                    "#SBATCH --error={err}\n"
                    "#SBATCH --output={log}\n"
                    "#SBATCH --time={time}\n\n"
                    "module load {name}\n\n"
                    "srun -n{self.n_workers} {script} {self.endpoint}")

        return here_doc.format(self=self, queue=queue, mem=memory,
                               err=err_file, log=log_file, time=walltime,
                               name=module_name, script=worker_script)

"""
Start a local scheduler with workers on SPICE using the scheduler interface
defined in `spice_scheduler.py`.

"""

from spice_scheduler import LocalScheduler


def main():
    # Set up local scheduler.
    ls = LocalScheduler(8)
    ls.scheduler

    # Define SBATCH requirements.
    memory = 16000
    err_file = '/scratch/dkillick/SPICE/dask/job-%N-%j.err'
    log_file = '/scratch/dkillick/SPICE/dask/job-%N-%j.log'

    # Start workers!
    ls.start_workers(memory=memory, err_file=err_file, log_file=log_file)

    # Finally print details about the scheduler.
    print ls.endpoint


if __name__ == '__main__':
    main()

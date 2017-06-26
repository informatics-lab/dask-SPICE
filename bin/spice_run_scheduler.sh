#!/bin/bash -l
#SBATCH --qos=normal
#SBATCH --mem=4000
#SBATCH --ntasks=8
#SBATCH --error=/scratch/dkillick/SPICE/dask/job-%N-%j.err
#SBATCH --output=/scratch/dkillick/SPICE/dask/job-%N-%j.log
#SBATCH --time=90


# Start a dask scheduler process and run a number of dask worker processes
# on SPICE. Workers are associated with the scheduler by reference to the
# scheduler's IP address and port.

module load scitools

HOST=$(hostname -i)
PORT=8776
NWORKERS=48


# Start a distributed scheduler on SPICE.
echo "Scheduler: $(hostname) ${HOST}:${PORT} $$ $PPID"

dask-scheduler --port ${PORT} &

# Run one worker task for each of the tasks requested with `sbatch`.
srun -n ${NWORKERS} ./spice_run_worker.sh "${HOST}:${PORT}"
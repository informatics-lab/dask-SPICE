#!/bin/bash -l

# Spin up a distributed worker in each SPICE task.

# Globals.
SCHEDULER=${1}

echo "Worker: $(hostname) $(hostname -i) $$ $PPID scheduler - <${SCHEDULER}>"

# Run worker.
# Workers must have only a single thread for running Iris code as Iris is
# not thread-safe.
dask-worker ${SCHEDULER} --nthreads 1 --reconnect

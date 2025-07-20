# FirecrestExecutor: A supercomputer at your pythonic fingertips

Execute Python functions on a remote supercomputer. Sounds difficult? It is
not!

The package builds on
[FirecREST](https://eth-cscs.github.io/firecrest-v2/#firecrest-v2), a
lightweight REST API for accessing HPC resources and the associated
[pyFirecREST Python library](https://github.com/eth-cscs/pyfirecrest). It
abstracts these resources in the form of a standard [Python
executor](https://docs.python.org/3/library/concurrent.futures.html#executor-objects§).
Once the executor is created, which requires credentials and information about
the remote supercomputer, you can submit Python functions to be executed
remotely. The executor will transparently take care of the details, such as
creating a job script, submitting it to the scheduler, and waiting for the
results.

## A simple example

```python
from firecrest-executor import FirecrestExecutor
import logging


# functions to be executed remotely
def square_number(x):
    from math import pow

    return pow(x, 2)


def report_hostname():
    import subprocess
    import os

    cluster = os.environ.get("CLUSTER_NAME", "unknown")
    hostname = subprocess.check_output(["hostname"], text=True).strip()
    return f"Success testing execution on {hostname} of {cluster}!"


print("Executing remotely, stay tuned...")

# Use the executor to remotely execute (asynchronously) the functions defined above.
with FirecrestExecutor(
    working_dir="/users/vjoost/FirecrestExecutorDemo/",
    sbatch_options=[
        "--job-name=FirecrestExecutor",
        "--time=00:10:00",
        "--nodes=1",
        "--partition=normal",
    ],
    srun_options=["--environment=/users/vjoost/FirecrestExecutorDemo/demo.toml"],
    sleep_interval=5,
    logger_level=logging.ERROR,
) as executor:
    # A quick test to see if the executor is working
    print(executor.submit(report_hostname).result())
    # and let's compute the square of some numbers
    numbers = range(2, 5)
    print("Let's compute squares of 2..4: ", list(executor.map(square_number, numbers)))
```

This results in the expected output:

```text
$ python simple.py 
Executing remotely, stay tuned...
Success testing execution on nid005463 of clariden!
Let's compute squares of 2..4:  [4.0, 9.0, 16.0]
```

## Getting started

### Install the firecrest-executor

Directly from PyPI:

```bash
pip install firecrest-executor
```

or clone from github:

```bash
git clone https://github.com/vondele/firecrest-executor.git
cd firecrest-executor
pip install -e .[examples]
```

### Enable FirecREST

This package requires you have access to a supercomputer that supports the
firecREST API. Inquire with your HPC support team if you are unsure. For the
[Swiss national supercomputing center (CSCS)](https://www.cscs.ch/) this is
the case for all clusters see [their
documentation](https://docs.cscs.ch/services/firecrest/). Follow the process to
obtain the necessary clients, tokens, and credentials to be able to access the
system using the firecREST API and define the following environment variables:

```text
        - FIRECREST_CLIENT_ID
        - FIRECREST_CLIENT_SECRET
        - AUTH_TOKEN_URL
        - FIRECREST_URL
        - FIRECREST_SYSTEM
        - FIRECREST_ACCOUNT
```

### Ensure consistent environments

This package requires the same version of Python and of its dependencies (in
particular the package to serialize Python functions and variables
[dill](https://pypi.org/project/dill/)) to be available locally and remotely.
Containers to the rescue (or manage your environment carefully with any other
tool)! At CSCS the [container
engine](https://docs.cscs.ch/software/container-engine/) allows for passing the
`--environment=foo.toml` flag to srun, to start commands in a container with
the specified settings. Hence, use an equivalent container locally and
remotely. This container image should also contain the Python packages needed
by any of the functions you want to execute remotely.

### Code with remote execution in mind

Python functions that will be execute remotely should be serializable with dill
and be executable without extra context in a fresh Python shell. Avoid access
to global variables (in particular things like locks). Explicitly import all
modules and functions that are used in the remote function.

Currently, every function call creates a new job, allocates at least a full
node, and hence is not suitable for very small tasks. The overhead of starting
a job is a few seconds/minutes at least. This approach is thus suitable for
tasks that are computationally intensive.

The function arguments and return values are serialized and passed between the
systems. Currently, these can not exceed a few 100kB. The functions can not
rely on terminal input and output is stored remotely (typically
`slurm-<jobid>.out`). Wrap functions, capture, and return output if needed.

## Advanced usage and caveats

Containers allow for mounting filesystems, and as such these Python functions
can load and save large persistent datasets. The firecrest api allows for up and
downloading data.

The code prioritizes returning a result, even if e.g. node failure might cause
a job to fail. In these cases, the job is transparently resubmitted and the
result is returned when available. If the job modifies state on the remote
system, this might lead to unexpected results. Similarly, the executor handles
failure of the API by repeated calling, after a few seconds sleep.

Exceptions in the remotely executed function are currently not propagated to
the caller.

The executor has a few knobs for configuration, such as the logging level, the
maximum number of concurrent jobs, and the sleep interval between checks for
status. Passing an explicit environment variable, allows for bypassing the OS
environment variables, and e.g. for creating different executors for different
clusters.

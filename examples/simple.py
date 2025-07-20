from firecrest_executor import FirecrestExecutor
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

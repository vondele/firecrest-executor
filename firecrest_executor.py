import base64
import dill
import logging
import os
import sys
import threading
import time
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from typing import Any, Callable, TypeVar, cast, Iterator
from pathlib import Path

from firecrest.Authorization import ClientCredentialsAuth
from firecrest.v2 import Firecrest

X = TypeVar("X")


class FirecrestExecutor(Executor):
    """
    Executor for submitting and managing jobs on a remote Slurm cluster via Firecrest.

    This class wraps Firecrest API calls to submit python code, monitor job status, retrieve output,
    and convert results. It uses a thread pool for concurrent job execution.

    Important: For this to work, the python and dill version on the remote cluster
               must match the local version. Using the same container local and remote is recommended.

    Requires the following environment variables for Firecrest client initialization:
        - FIRECREST_CLIENT_ID
        - FIRECREST_CLIENT_SECRET
        - AUTH_TOKEN_URL
        - FIRECREST_URL
        - FIRECREST_SYSTEM
        - FIRECREST_ACCOUNT
    Unless these are explicitly passed as an environment dictionary.

    See:    https://docs.cscs.ch/services/firecrest/#creating-an-application
    FIRECREST_SYSTEM and FIRECREST_ACCOUNT, must specify the vCluster and account to use.

    Args:
        working_dir (str): Directory for job submission and output.
        sbatch_options (list): Options for sbatch command.
        srun_options (list): Options for srun command.
        logger (logging.Logger, optional): Logger instance.
        logger_level (int, optional): Logging level (default: logging.INFO).
        max_workers (int, optional): Maximum concurrent jobs.
        sleep_interval (int, optional): Polling and retry interval in seconds (default: 30).
        task_retries (int, optional): Number of times a task is retried if the output can't be read, e.g. canceled externally (default: 3).
        env (dict, optional): Specify the values of the environment variables to use for Firecrest client initialization, i.e. do not os calls (default: None).
    """

    def __init__(
        self,
        working_dir,
        sbatch_options,
        srun_options,
        logger=None,
        logger_level=logging.INFO,
        max_workers=None,
        sleep_interval=30,
        task_retries=3,
        env=None,
    ):
        self._working_dir = working_dir
        self._sbatch_options = sbatch_options
        self._srun_options = srun_options
        self._sleep_interval = sleep_interval
        self._task_retries = task_retries
        self._lock = threading.Lock()

        # Use provided logger or a default
        if logger is None:
            self._init_logger(logger_level)
        else:
            self._logger = logger

        self._logger.info("Initializing FirecrestExecutor...")

        # The underlying thread pool executor for running jobs
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

        # initialize the Firecrest client
        self._init_client(env)

        # Some bookkeeping of the number of handled jobs
        self._count_tasks = 0
        self._running_jobids = set()

    def _init_client(self, env=None):
        """
        Initialize the Firecrest client using environment variables.

        Loads credentials and configuration from environment, checks for missing variables,
        and creates the Firecrest client instance.
        """
        required_env_vars = [
            "FIRECREST_CLIENT_ID",
            "FIRECREST_CLIENT_SECRET",
            "AUTH_TOKEN_URL",
            "FIRECREST_URL",
            "FIRECREST_SYSTEM",
            "FIRECREST_ACCOUNT",
        ]

        # Build dict of {name: value} for all required variables
        self._env = (
            {var: os.environ.get(var) for var in required_env_vars}
            if env is None
            else env
        )

        # Check for missing environment variables, which would lead to None values
        missing = [var for var, val in self._env.items() if val is None]
        if missing:
            self._logger.error(
                f"Missing required environment variables: {', '.join(missing)}",
            )
            raise RuntimeError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        # use env variables to configure Class, casting to avoid warnings.
        self._vCluster = cast(str, self._env["FIRECREST_SYSTEM"])
        self._account = cast(str, self._env["FIRECREST_ACCOUNT"])

        auth = ClientCredentialsAuth(
            cast(str, self._env["FIRECREST_CLIENT_ID"]),
            cast(str, self._env["FIRECREST_CLIENT_SECRET"]),
            cast(str, self._env["AUTH_TOKEN_URL"]),
        )
        auth.timeout = 300  # Explicitly set timeout for auth requests
        self._client = Firecrest(
            cast(str, self._env["FIRECREST_URL"]), authorization=auth
        )
        self._client.timeout = 300  # Explicitly set timeout for all requests

    def _init_logger(self, level=logging.INFO):
        """
        Initialize the logger for the executor.

        Args:
            level (int): Logging level.
        """

        class FlushStreamHandler(logging.StreamHandler):
            def emit(self, record):
                super().emit(record)
                self.flush()

        logger = logging.getLogger("FirecrestExecutor")
        logger.setLevel(level)
        logger.propagate = False  # don't pass logs to parent/root logger

        if not logger.handlers:  # Prevent adding multiple handlers
            handler = FlushStreamHandler()
            formatter = logging.Formatter(
                "[%(asctime)s] [Thread 0x%(thread)016x] %(levelname)10s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        self._logger = logger

    def _slurm_state_completed(self, state):
        """
        Check if the Slurm job state indicates completion.

        Args:
            state (str): Slurm job state string.

        Returns:
            bool: True if job is in a completion state, False otherwise.
        """
        completion_states = {
            "BOOT_FAIL",
            "CANCELLED",
            "COMPLETED",
            "DEADLINE",
            "FAILED",
            "NODE_FAIL",
            "OUT_OF_MEMORY",
            "PREEMPTED",
            "TIMEOUT",
        }
        if state:
            # Make sure all the steps include one of the completion states
            return all(
                any(cs in s for cs in completion_states) for s in state.split(",")
            )

        return False

    def _generate_slurm_script(self, payload: bytes) -> str:
        """
        Generate a Slurm job script that runs the provided python code.

        Important: matching python and dill versions on the remote cluster are required.

        Args:
            payload (bytes): dill serialized payload containing the function and its arguments.

        Returns:
            str: Slurm job script.
        """

        # TODO: code a check for matching python and dill versions on the remote cluster.

        # Serialize the function and arguments, then base64 encode
        payload_b64 = base64.b64encode(payload).decode()

        # pack in some python code, which will deserialize,
        # execute the function, and add its result to the output buffer.
        remote_code = f"""
import base64
import sys

def report_error(msg):
    print("==== FirecrestExecutor: Error ====")
    print(msg)
    print("==== FirecrestExecutor: End Error ====", flush=True)
    sys.exit(1)

try:
    import dill
except Exception as e:
    report_error(f"Failed to import dill, ensure it is installed on the remote system: {{e}}")

# Version checks
expected_py_version = {sys.version_info[:3]!r}
expected_dill_version = {dill.__version__!r}
actual_py_version = sys.version_info[:3]
actual_dill_version = dill.__version__

if actual_py_version != expected_py_version:
    report_error(f"Python version mismatch between local and remote systems: local {{expected_py_version}}, remote {{actual_py_version}}")
if actual_dill_version != expected_dill_version:
    report_error(f"Dill version mismatch between local and remote systems: local {{expected_dill_version}}, remote {{actual_dill_version}}")

print("==== FirecrestExecutor: Executing remote code ====", flush=True)
try:
    payload_b64 = {payload_b64!r}
    func, args, kwargs = dill.loads(base64.b64decode(payload_b64))
    result = func(*args, **kwargs)
    payload_b64 = base64.b64encode(dill.dumps(result)).decode()
except Exception as e:
    report_error(f"Remote code execution failed with exception: {{e}}")

# Print marker and write base64-encoded dill result to buffer
print("==== FirecrestExecutor: Dill Encoded Result ====")
print(payload_b64, end="")
print("\\n==== FirecrestExecutor: End Dill Encoded Result ====", flush=True)
        """

        # pack in the form of a bash command, which will decode the
        # base64 encoded python code, and execute it.
        remote_code_b64 = base64.b64encode(remote_code.encode()).decode()
        bash_command = f'echo "{remote_code_b64}" | base64 -d | python3'

        # and present in the form of a Slurm script
        slurm_script = f"""#!/bin/bash -l
#SBATCH {" ".join(self._sbatch_options)}

srun {" ".join(self._srun_options)} bash -c '{bash_command}'
"""
        return slurm_script

    def _convert_output(self, output: str) -> bytes:
        # Catch error conditions
        error_marker_start = "==== FirecrestExecutor: Error ====\n"
        error_marker_end = "==== FirecrestExecutor: End Error ====\n"
        if error_marker_start in output:
            sections = output.split(error_marker_start, 1)
            error_section = sections[1].split(error_marker_end, 1)[0].strip()
            raise RuntimeError(f"Error in remote execution: {error_section}")

        # Split output at the marker and get the base64-encoded dill result
        marker_start = "==== FirecrestExecutor: Dill Encoded Result ====\n"
        marker_end = "==== FirecrestExecutor: End Dill Encoded Result ====\n"
        sections = output.split(marker_start, 1)
        if len(sections) != 2:
            raise RuntimeError("Result marker not found in subprocess output")
        result_section = sections[1].split(marker_end, 1)[0].strip()

        return base64.b64decode(result_section)

    def _run_remote_payload(self, payload: bytes) -> bytes:
        """
        Submits a python function as an serialized payload to a remote Slurm cluster,
        monitors its execution, and retrieves the serialized result.

        This method performs the following steps:
        1. Generates a Slurm job script from the provided payload.
        2. Submits the job to the cluster, retrying on failure.
        3. Polls the job status until completion, logging state changes.
        4. Retrieves the job's output path from metadata, retrying on failure.
        5. Fetches and converts the job output, retrying on failure.
        6. Returns the processed result.

        Args:
            payload (bytes): The dill serialized payload containing the function and its arguments.

        Returns:
            bytes: The dill serialized result of the executed function.

        Raises:
            Exception: If output conversion fails.
        """

        with self._lock:
            self._count_tasks += 1
            task_id = self._count_tasks
        self._logger.info(f"Starting task number {task_id}")

        slurm_script = self._generate_slurm_script(payload)
        job_id = None
        result = b""

        for attempt in range(self._task_retries + 1):
            try:
                job_id = self._submit_job_with_retries(slurm_script)
                self._poll_job_status(job_id)
                output_path = self._retrieve_output_path(job_id)
                output = self._retrieve_job_output(job_id, output_path)
                result = self._convert_output(output)
                self._logger.info(f"Job {job_id} produced result")
                break
            except Exception as e:
                msg = f"Failed to obtain result from {job_id} (attempt {attempt + 1} / {self._task_retries + 1}): {e}"
                if "version mismatch" in str(e):
                    self._task_retries = (
                        0  # Stop retrying if there's a version mismatch
                    )
                if attempt >= self._task_retries:
                    self._logger.error(msg)
                    raise
                else:
                    self._logger.warning(msg)

        self._logger.info(f"Finished task number {task_id}")
        return result

    def _submit_job_with_retries(self, slurm_script: str) -> str:
        while True:
            try:
                self._logger.info("Submitting ...")
                job = self._client.submit(
                    self._vCluster,
                    self._working_dir,
                    script_str=slurm_script,
                    account=self._account,
                )
                job_id = job["jobId"]
                self._logger.info(f"Job {job_id} submitted")
                self._running_jobids.add(job_id)
                return job_id
            except Exception as e:
                self._logger.warning(
                    f"Job submission failed: {e}. Retrying in {self._sleep_interval} seconds..."
                )
                time.sleep(self._sleep_interval)

    def _poll_job_status(self, job_id: str):
        old_state = None
        while True:
            time.sleep(self._sleep_interval)
            try:
                job = self._client.job_info(self._vCluster, job_id)
                state = job[0]["status"]["state"]
                if isinstance(state, list):
                    state = ",".join(state)
                if state != old_state:
                    self._logger.info(f"Job {job_id} current state: {state}")
                    old_state = state
                if self._slurm_state_completed(state):
                    self._running_jobids.remove(job_id)
                    break
            except Exception as e:
                self._logger.warning(
                    f"Failed to get job status for job {job_id}: {e}. Retrying in {self._sleep_interval} seconds..."
                )

    def _retrieve_output_path(self, job_id: str) -> str:
        fatal_retries = 0
        while fatal_retries < 50:
            try:
                job_metadata = self._client.job_metadata(self._vCluster, job_id)[0]
                output_path = job_metadata["standardOutput"]
                self._logger.info(f"Job {job_id} metadata retrieved")
                return output_path
            except Exception as e:
                # TODO. Somehow this one doesn't resolve over time, there ought to be a better solution.
                if "Job not found" in str(e):
                    fatal_retries += 1
                self._logger.warning(
                    f"Failed to get job metadata for job {job_id}: {e}. Retrying in {self._sleep_interval} seconds..."
                )
                time.sleep(self._sleep_interval)
        # guestimate the output path if metadata retrieval fails after retries. If that file doesn't exist, it will be handled in _retrieve_job_output.
        output_path = str(Path(self._working_dir) / f"slurm-{job_id}.out")
        self._logger.error(
            f"Failed to get job metadata for job {job_id}: assuming output path {output_path}."
        )
        return output_path

    def _retrieve_job_output(self, job_id: str, output_path: str) -> str:
        while True:
            try:
                output = self._client.view(self._vCluster, output_path)
                self._logger.info(f"Job {job_id} output retrieved from: {output_path}")
                return output
            except Exception as e:
                # TODO: analyze the exception to determine proper action
                if "No such file or directory" in str(e):
                    self._logger.warning(
                        f"Output file not found for job {job_id}: assuming output is empty string."
                    )
                    return ""
                else:
                    self._logger.warning(
                        f"Failed to retrieve output for job {job_id}: {e}. Retrying in {self._sleep_interval} seconds..."
                    )
                    time.sleep(self._sleep_interval)

    def submit(self, fn: Callable[..., X], *args: Any, **kwargs: Any) -> Future[X]:
        """
        Submit a callable, which will be executed remotely, and its result returned.
        The the function must be serializable with dill.

        Args:
            fn (Callable): Function that will be serialized annd executed remotely.
            *args: Arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            Future: Future object representing the asynchronous job execution.

        Raises:
            TypeError: If fn is not callable.
        """

        def wrapped() -> X:
            try:
                payload = dill.dumps((fn, args, kwargs))
                encodedResult = self._run_remote_payload(payload)
                return dill.loads(encodedResult)
            except Exception as e:
                self._logger.error(f"Exception in submitted task: {e}")
                raise

        return self._executor.submit(wrapped)

    def map(
        self, fn: Callable[..., X], *iterables, timeout=None, chunksize=1
    ) -> Iterator[X]:
        """
        Submit the function fn to be executed remotely for each set of arguments from the given iterables.

        Args:
            fn (Callable[..., X]): The function to execute remotely.
            *iterables: One or more iterables supplying positional arguments to fn.
            timeout (float, optional): Maximum number of seconds to wait for each result. Defaults to None.
            chunksize (int, optional): Present for API compatibility; not used.

        Returns:
            Iterator[X]: An iterator yielding results in the order corresponding to the input iterables.
        """
        if not iterables:
            return iter([])

        # part of the API, but not used in this implementation.
        _ = chunksize

        futures = [self.submit(fn, *args) for args in zip(*iterables)]
        return (future.result(timeout=timeout) for future in futures)

    def shutdown(self, wait=True, *, cancel_futures=False):
        """
        Shutdown the executor, optionally waiting for jobs to finish or cancelling running jobs.

        Args:
            wait (bool): If True, repeatedly attempt to cancel running jobs and wait until all are finished.
                         If False, attempt to cancel running jobs once and return immediately.
            cancel_futures (bool): If True, cancel all pending and running jobs.
        """

        self._logger.info("shutdown FirecrestExecutor...")

        # No more retries, we are shutting down and cancelling jobs.
        self._task_retries = 0

        if cancel_futures or not wait:
            while len(self._running_jobids) > 0:
                for job_id in list(self._running_jobids):
                    self._logger.warning(f"Cancelling running job {job_id}...")
                    try:
                        self._client.cancel_job(self._vCluster, job_id)
                    except Exception as e:
                        self._logger.error(f"Failed to cancel job {job_id}: {e}")
                if not wait:
                    break
                time.sleep(self._sleep_interval)

        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensure shutdown on context exit, including on exceptions (e.g. Ctrl+C).

        Calls shutdown with wait=True and cancel_futures=True.
        """
        _ = exc_type, exc_val, exc_tb  # Unused, but required for __exit__ signature

        self.shutdown(wait=True, cancel_futures=True)

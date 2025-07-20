from firecrest_executor import FirecrestExecutor
import nevergrad as ng


# demo minimization of a simple function using nevergrad's TBPSA optimizer
def objective_function(*args):
    """
    A simple function suitable for minimizing.
    """
    from math import pow

    return sum(pow(x, 2) for x in args)


# nevergrad setup, for two variables.
instrumentation = ng.p.Instrumentation(
    ng.p.Scalar(init=7).set_bounds(lower=-10, upper=10).set_mutation(sigma=4),
    ng.p.Scalar(init=7).set_bounds(lower=-10, upper=10).set_mutation(sigma=4),
)
budget = 16  # Total number of evaluations to perform
num_workers = 4  # Number of parallel workers to use

# Use TBPSA optimizer
optimizer = ng.optimizers.TBPSA(instrumentation, budget=budget, num_workers=num_workers)

# firecrestExecutor setup
# Use the executor to remotely execute the objective_function,
# to miminize the objective function using nevergrad's TBPSA optimizer.
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
) as executor:
    recommendation = optimizer.minimize(objective_function, executor=executor)

print("Final Recommended solution:", recommendation.value)

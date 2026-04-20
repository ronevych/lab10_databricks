import os
import time
import logging
import argparse
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def monitor_job_pro(w, run_id, timeout):
    """
    Monitor a Databricks job run until completion or timeout.
    This function handles SDK errors gracefully and returns True if the job succeeds.
    """
    logger.info(f"Starting job monitor. Run ID: {run_id}")
    start_time = time.time()
    
    # Job states that indicate the job is still running
    active_states = ['PENDING', 'RUNNING', 'BLOCKED', 'WAITING_FOR_RESOURCES']

    while (time.time() - start_time) < timeout:
        try:
            # Fetch the current run status from Databricks
            run = w.jobs.get_run(run_id=run_id)
            # Convert status object to string for safer processing
            state = str(run.state.life_cycle_state.value)
            
            logger.info(f"Current status: {state}")

            if state not in active_states:
                result = str(run.state.result_state.value) if run.state.result_state else "UNKNOWN"
                logger.info(f"Job finished. State: {state}, Result: {result}")
                return result == 'SUCCESS'

        except Exception as e:
            # Handle SDK exceptions. If it's a WAITING_FOR_RESOURCES error, we log and continue
            err_msg = str(e)
            if "WAITING_FOR_RESOURCES" in err_msg:
                logger.info("Status: WAITING_FOR_RESOURCES (caught as exception, skipping...)")
            else:
                logger.warning(f"Polling update: {err_msg}")
        
        time.sleep(20)
    
    logger.error("Monitor timeout reached.")
    return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-name", default="Internship_Pipeline_Lab10")
    parser.add_argument("--timeout", type=int, default=3600)
    args = parser.parse_args()
    load_dotenv()

    w = WorkspaceClient()
    base_path = os.getenv("WORKSPACE_BASE_PATH")
    cluster_id = os.getenv("DATABRICKS_EXISTING_CLUSTER_ID")

    # Stage 1: Create and trigger job
    # We catch configuration and setup errors here
    try:
        task_conf = {"existing_cluster_id": cluster_id}
        
        t1 = jobs.Task(task_key="Step_1", **task_conf, notebook_task=jobs.NotebookTask(notebook_path=f"{base_path}/01_changes_simulation"))
        t2 = jobs.Task(task_key="Step_2", **task_conf, depends_on=[jobs.TaskDependency(task_key="Step_1")], notebook_task=jobs.NotebookTask(notebook_path=f"{base_path}/03_scd_type2"))
        t3 = jobs.Task(task_key="Step_3", **task_conf, depends_on=[jobs.TaskDependency(task_key="Step_2")], notebook_task=jobs.NotebookTask(notebook_path=f"{base_path}/99_select_all"))

        logger.info("Preparing Job...")
        job = w.jobs.create(name=args.job_name, tasks=[t1, t2, t3])
        run_id = w.jobs.run_now(job_id=job.job_id).run_id
        logger.info(f"Job triggered. Run ID: {run_id}")
        
    except Exception as e:
        logger.error(f"Failed to setup or trigger job: {e}")
        exit(1)
    # Stage 2: Monitor job progress
    # The monitoring function is separate so errors during monitoring do not crash the main process
    time.sleep(10)
    success = monitor_job_pro(w, run_id, args.timeout)
    
    if not success:
        logger.error("Pipeline failed or timed out.")
        exit(1)
    
    logger.info("Pipeline finished successfully!")

if __name__ == "__main__":
    main()
import os
import time
import logging
import argparse
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def monitor_job_pro(w: WorkspaceClient, run_id: int, max_duration_seconds: int = 3600) -> bool:
    """
    Very patient monitor. It will wait through any temporary SDK exceptions 
    until the job explicitly succeeds or fails.
    """
    logger.info(f"Starting job monitor. Run ID: {run_id}. Timeout: {max_duration_seconds}s")
    
    active_states = [
        jobs.RunLifeCycleState.PENDING,
        jobs.RunLifeCycleState.RUNNING,
        jobs.RunLifeCycleState.BLOCKED,
        jobs.RunLifeCycleState.WAITING_FOR_RESOURCES
    ]

    start_time = time.time()

    while (time.time() - start_time) < max_duration_seconds:
        try:
            run = w.jobs.get_run(run_id=run_id)
            current_state = run.state.life_cycle_state
            
            logger.info(f"Current status: {current_state.value}")

            if current_state not in active_states:
                result = run.state.result_state
                logger.info(f"Job finished with state: {current_state.value}")
                
                if result == jobs.RunResultState.SUCCESS:
                    logger.info("Result: SUCCESS")
                    return True
                else:
                    logger.error(f"Result: {result.value if result else 'FAILED'}")
                    return False
                    
        except Exception as e:
            logger.info(f"Wait/Connection notice: {str(e)}. Retrying in 20s...")
            
        time.sleep(20) 
        
    logger.error("Timeout: Monitoring exceeded maximum duration.")
    return False

def main():
    parser = argparse.ArgumentParser(description="Databricks Pipeline Automation CLI")
    parser.add_argument("--job-name", type=str, default="Internship_Data_Pipeline_Lab", help="Job name")
    parser.add_argument("--timeout", type=int, default=3600, help="Timeout in seconds")
    args = parser.parse_args()

    load_dotenv()
    
    base_path = os.getenv("WORKSPACE_BASE_PATH")
    cluster_id = os.getenv("DATABRICKS_EXISTING_CLUSTER_ID")

    if not base_path or not cluster_id:
        logger.error("Missing WORKSPACE_BASE_PATH or DATABRICKS_EXISTING_CLUSTER_ID.")
        exit(1)

    try:
        w = WorkspaceClient()
        
        task_1 = jobs.Task(
            task_key="Step_01_Simulation",
            existing_cluster_id=cluster_id,
            notebook_task=jobs.NotebookTask(notebook_path=f"{base_path}/01_changes_simulation")
        )
        task_2 = jobs.Task(
            task_key="Step_02_SCD_Type2",
            existing_cluster_id=cluster_id,
            depends_on=[jobs.TaskDependency(task_key="Step_01_Simulation")],
            notebook_task=jobs.NotebookTask(notebook_path=f"{base_path}/03_scd_type2")
        )
        task_3 = jobs.Task(
            task_key="Step_03_Select_All",
            existing_cluster_id=cluster_id,
            depends_on=[jobs.TaskDependency(task_key="Step_02_SCD_Type2")],
            notebook_task=jobs.NotebookTask(notebook_path=f"{base_path}/99_select_all")
        )

        logger.info(f"Preparing Job: {args.job_name}...")
        created_job = w.jobs.create(name=args.job_name, tasks=[task_1, task_2, task_3])
        
        run_response = w.jobs.run_now(job_id=created_job.job_id)
        run_id = run_response.run_id
        logger.info(f"Job triggered. Run ID: {run_id}")

        logger.info("Initializing...")
        time.sleep(15)

        success = monitor_job_pro(w, run_id, args.timeout)
        
        if not success:
            exit(1)

    except Exception as e:
        logger.error(f"Critical error during setup: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
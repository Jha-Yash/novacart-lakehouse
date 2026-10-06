import time
import boto3
from datetime import datetime, timezone

class NovaCartOrchestrator:
    def __init__(self, region='ap-southeast-2'):
        self.region = region
        self.glue = boto3.client('glue', region_name=region)
        self.athena = boto3.client('athena', region_name=region)
        self.bucket = f'novacart-order-analytics-125992594465-{region}'

    def run_job(self, job_name, arguments=None):
        args = arguments or {}
        args['--BUCKET_NAME'] = self.bucket
        print(f"\n==================================================")
        print(f" Starting Glue Job: {job_name}")
        print(f" Arguments: {args}")
        print(f"==================================================")
        res = self.glue.start_job_run(JobName=job_name, Arguments=args)
        run_id = res['JobRunId']
        print(f">>> JobRunId: {run_id}")
        
        while True:
            status_res = self.glue.get_job_run(JobName=job_name, RunId=run_id)
            state = status_res['JobRun']['JobRunState']
            exec_time = status_res['JobRun'].get('ExecutionTime', 0)
            if state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                print(f">>> Job {job_name} finished in {exec_time}s with state: {state}")
                if state != 'SUCCEEDED':
                    err = status_res['JobRun'].get('ErrorMessage', 'Unknown error')
                    raise RuntimeError(f"Job {job_name} failed: {err}")
                return run_id
            time.sleep(5)

    def run_pipeline(self):
        print(f">>> Starting End-to-End NovaCart Lakehouse Pipeline at {datetime.now(timezone.utc).isoformat()}")
        
        # 1. Ingest Batch 1
        self.run_job('novacart-bronze-ingestion', {'--BATCH_ID': 'batch_1'})
        
        # 2. Ingest Batch 2
        self.run_job('novacart-bronze-ingestion', {'--BATCH_ID': 'batch_2'})
        
        # 3. Curate Batch 1 into Silver
        self.run_job('novacart-silver-curation', {'--BATCH_ID': 'batch_1'})
        
        # 4. Curate Batch 2 (Incremental Merge) into Silver
        self.run_job('novacart-silver-curation', {'--BATCH_ID': 'batch_2'})
        
        # 5. Process SCD Type 2 Customers
        self.run_job('novacart-scd2-customers')
        
        # 6. Aggregate Gold KPI Tables
        self.run_job('novacart-gold-analytics')
        
        print("\n>>> ALL PIPELINE STAGES COMPLETED SUCCESSFULLY!")

if __name__ == '__main__':
    orchestrator = NovaCartOrchestrator()
    print("NovaCart Orchestration Runner Ready.")

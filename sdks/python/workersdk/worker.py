import json
import time
import uuid
import psycopg2
from psycopg2.extras import DictCursor
from typing import Callable, Dict

class Worker:
    def __init__(self):
        self.handlers: Dict[str, Callable[[str], str]] = {}
        self.connection = None
        self.worker_id = f"python-worker-{str(uuid.uuid4())[:8]}"

    def register(self, action: str, handler: Callable[[str], str]):
        self.handlers[action] = handler

    def start(self, dsn: str = "postgresql://admin:admin@postgres:5432/wf_engine"):
        self._connect_with_retry(dsn)
        
        print(f"Worker {self.worker_id} started, listening for jobs via Postgres SKIP LOCKED...")
        
        try:
            while True:
                self._poll_and_process()
                time.sleep(0.5)  # Poll every 500ms
        except KeyboardInterrupt:
            print("Worker stopping...")
        finally:
            if self.connection:
                self.connection.close()

    def _poll_and_process(self):
        if not self.handlers:
            return

        actions = list(self.handlers.keys())
        
        with self.connection.cursor(cursor_factory=DictCursor) as cursor:
            try:
                claim_sql = """
                    UPDATE workflow_job 
                    SET status = 'PROCESSING', locked_by = %s
                    WHERE id = (
                        SELECT id FROM workflow_job 
                        WHERE status = 'PENDING' AND action = ANY(%s)
                        ORDER BY created_at ASC
                        LIMIT 1 
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, workflow_run_id, action, payload
                """
                
                cursor.execute(claim_sql, (self.worker_id, actions))
                job_row = cursor.fetchone()

                if not job_row:
                    self.connection.commit()
                    return # No jobs available

                job_id = job_row['id']
                run_id = job_row['workflow_run_id']
                action = job_row['action']
                payload = job_row['payload']

                print(f"Processing job: {action} for run: {run_id}")

                handler = self.handlers.get(action)
                status = "SUCCESS"
                
                try:
                    result = handler(payload)
                except Exception as e:
                    # Safely construct JSON error
                    result = json.dumps({"error": str(e)})
                    status = "FAILED"

                update_sql = """
                    UPDATE workflow_job 
                    SET status = 'COMPLETED', result = %s, result_status = %s
                    WHERE id = %s
                """
                cursor.execute(update_sql, (result, status, job_id))
                
                self.connection.commit()
                
            except Exception as e:
                self.connection.rollback()
                print(f"Error in polling transaction: {e}")

    def _connect_with_retry(self, dsn: str):
        for attempt in range(30):
            try:
                self.connection = psycopg2.connect(dsn)
                # Test connection
                with self.connection.cursor() as cur:
                    cur.execute('SELECT 1')
                return
            except Exception as e:
                print(f"Postgres not ready, retrying ({attempt + 1}/30)...")
                time.sleep(2)
        else:
            raise Exception("Could not connect to Postgres after 30 attempts")

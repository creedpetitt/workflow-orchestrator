import { Client } from 'pg';
import { randomUUID } from 'crypto';

export class Worker {
    private handlers: Map<string, (payload: string) => any>;
    private client: Client | null;
    private workerId: string;

    constructor() {
        this.handlers = new Map();
        this.client = null;
        this.workerId = 'node-worker-' + randomUUID().substring(0, 8);
    };

    register(action: string, handler: (payload: string) => any) {
        this.handlers.set(action, handler);
    };

    async start(connectionString: string = "postgres://admin:admin@postgres:5432/wf_engine"): Promise<void> {
        
        await this.connectWithRetry(connectionString);

        console.log(`Worker ${this.workerId} started, listening for jobs via Postgres SKIP LOCKED...`);

        // Start the polling loop
        const poll = async () => {
            try {
                await this.pollAndProcess();
            } catch (e) {
                console.error("Error in polling loop:", e);
            }
            setTimeout(poll, 500); // Poll every 500ms
        };

        await poll();
    };

    private async pollAndProcess(): Promise<void> {
        if (this.handlers.size === 0 || !this.client) {
            return;
        }

        const actions = Array.from(this.handlers.keys());

        const placeholders = actions.map((_, i) => `$${i + 1}`).join(', ');

        try {
            await this.client.query('BEGIN');

            const claimSql = `
                UPDATE workflow_job 
                SET status = 'PROCESSING', locked_by = '${this.workerId}'
                WHERE id = (
                    SELECT id FROM workflow_job 
                    WHERE status = 'PENDING' AND action IN (${placeholders})
                    ORDER BY created_at ASC
                    LIMIT 1 
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, workflow_run_id as "workflowRunId", action, payload
            `;

            const claimResult = await this.client.query(claimSql, actions);

            if (claimResult.rows.length === 0) {
                await this.client.query('COMMIT');
                return; // No jobs available
            }

            const job = claimResult.rows[0];
            console.log(`Processing job: ${job.action} for run: ${job.workflowRunId}`);

            const handler = this.handlers.get(job.action);
            let resultOutput: string;
            let status: string = "SUCCESS";

            try {
                // Support both sync and async handlers
                const result = await handler!(job.payload);
                resultOutput = typeof result === 'string' ? result : JSON.stringify(result);
            } catch (e) {
                status = "FAILED";
                resultOutput = JSON.stringify({ error: String(e) });
            }

            const updateSql = `
                UPDATE workflow_job 
                SET status = 'COMPLETED', result = $1, result_status = $2
                WHERE id = $3
            `;
            
            await this.client.query(updateSql, [resultOutput, status, job.id]);

            await this.client.query('COMMIT');

        } catch (e) {
            await this.client.query('ROLLBACK');
            throw e;
        }
    }

    private async connectWithRetry(connectionString: string): Promise<void> {
        for (let attempt = 1; attempt <= 30; attempt++) {
            try {
                this.client = new Client({ connectionString });
                await this.client.connect();
                await this.client.query('SELECT 1');
                return;
            } catch (e) {
                console.log(`Postgres not ready, retrying (${attempt}/30)...`);
                await new Promise(resolve => setTimeout(resolve, 2000));
                if (attempt === 30) {
                    throw new Error('Could not connect to Postgres after 30 attempts');
                }
            }
        }
    }
}

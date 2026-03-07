import { Worker } from 'workersdk';

const worker = new Worker();

worker.register('step1', (payload: string) => {
    return `step1-result: processed ${payload}`;
});

worker.register('step2', (payload: string) => {
    return `step2-result: finished with ${payload}`;
});

worker.start("postgresql://admin:admin@postgres:5432/wf_engine");

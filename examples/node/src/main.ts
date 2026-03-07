import { Worker } from 'workersdk';

const worker = new Worker();

worker.register('send-welcome-email', async (payload: string) => {
    console.log(`[NODE] Sending email based on payload: ${payload}`);
    await new Promise(resolve => setTimeout(resolve, 1000));
    return `{"email_sent": true, "timestamp": "${new Date().toISOString()}"}`;
});

worker.start('kafka:9092', 'workflow-workers-node');

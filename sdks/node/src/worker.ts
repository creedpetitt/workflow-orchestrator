import { Kafka, Consumer, Producer } from 'kafkajs';
import { JobMessage, ResultMessage } from './models';

export class Worker {
    private handlers: Map<string, (payload: string) => string>;
    private consumer: Consumer | null;
    private producer: Producer | null;

    constructor() {
        this.handlers = new Map();
        this.consumer = null;
        this.producer = null;
    };

    register(action: string, handler: (payload: string) => any) {
        this.handlers.set(action, handler);
    };

    async start(bootstrapServers : string = "kafka:9092", groupId: string = "workflow-workers") : Promise<void> {
        const kafka = new Kafka({
            clientId: 'workflow-worker',
            brokers: [bootstrapServers]
        });

        this.consumer = kafka.consumer({groupId: groupId});
        this.producer = kafka.producer();

        for (let attempt = 0; attempt < 30; attempt++) {
            try {
                await this.consumer.connect();
                await this.producer.connect();
                break;
            } catch (e) {
                console.log(`Kafka not ready, retrying (${attempt + 1}/30)...`);
                await new Promise(resolve => setTimeout(resolve, 2000));
                if (attempt === 29) {
                    throw new Error('Could not connect to Kafka after 30 attempts');
                }
            }
        }

        await this.consumer.subscribe({topic: 'workflow-jobs', fromBeginning: true});

        await this.consumer.run({
            eachMessage : async ({message}) => {

                let job: JobMessage;
                try {
                    job = JSON.parse(message.value!.toString());
                } catch (e) {
                    console.error("Failed to parse message as JSON, skipping poison pill:", e);
                    return;
                }

                const handler: any = this.handlers.get(job.action);

                if(!handler) {
                    // Silently ignore, this message might be meant for a different worker group type
                    return;
                }

                let result: string;
                let status: string = "SUCCESS";
                try {
                    result = handler(job.payload);
                } catch (e) {
                    status = "FAILED";
                    result = JSON.stringify({ error: String(e) });
                }

                const response: ResultMessage = {
                    workflowRunId: job.workflowRunId,
                    action: job.action,
                    result: result,
                    status: status
                }

                await this.producer!.send({
                    topic: 'workflow-results',
                    messages: [{ value : JSON.stringify(response) }]
                });
            }
        });
    };
}
# Workflow Orchestrator

A lightweight, distributed workflow orchestration engine designed to decouple state management from task execution. This project demonstrates a scalable "Stateful Event Queue" pattern where a central Orchestrator manages the lifecycle of a workflow, while distributed Workers (written in any language) execute the actual business logic via pure PostgreSQL `SKIP LOCKED` polling.

## Architecture

The system consists of two main parts:
1. **Orchestrator (Java/Spring Boot):** Exposes a REST API to define and trigger workflows. It manages all state and queuing inside a single PostgreSQL database.
2. **Workers (Polyglot):** Stateless services that listen for specific tasks, execute them, and return results. SDKs are provided for Java, Python, and Node.js. 

Instead of relying on heavy external message brokers like Kafka, the workers use their native SQL drivers to execute lightning-fast `SELECT ... FOR UPDATE SKIP LOCKED` queries against the Orchestrator's database. This provides robust, atomic job distribution with zero "Dual-Write" vulnerabilities.

## Getting Started

The entire infrastructure can be spun up using Docker Compose. A Makefile is provided for convenience.

### Prerequisites
* Docker & Docker Compose
* Make (optional, but recommended)

### Run the Stack

**Option 1: Using Make (Recommended)**

```bash
# Start the DB, Orchestrator, and all 3 Polyglot Workers
make all

# Stop everything
make down

# Wipe data (Database)
make clean
```

**Option 2: Using Docker Compose directly**

```bash
docker compose --profile all up -d --build
```

## Reliability and Fault Tolerance

This engine is built for resilience. It natively handles worker crashes, network timeouts, and duplicate requests.

### 1. Idempotency (Postgres Unique Constraints)
To prevent duplicate execution, the Orchestrator enforces idempotency natively at the database level.
* **Mechanism:** When triggering a workflow, clients can provide an `Idempotency-Key`.
* **Behavior:** The system checks the `workflow_run` table for this key. If another thread attempts to write the same key simultaneously, Postgres's ACID guarantees throw a safe constraint violation, returning the existing `runId` immediately.

### 2. Automatic Retries
If a worker fails (e.g., database glitch, API timeout) or explicitly throws an Exception:
* The Orchestrator automatically intercepts the failure.
* It increments a retry counter and safely returns the job to the `PENDING` queue.
* The system defaults to 3 max retries before failing the job permanently.

### 3. Dead Letter Queues (DLQ)
If a task fails after all retries (e.g., a "Poison Pill" message):
* The Orchestrator marks the job status as `DEAD_LETTER`.
* The row is left in the queue table for manual inspection and debugging, preventing it from infinitely looping or blocking other workers.

---

## Developing with the SDKs

The repository provides beautiful, native SDKs so developers never have to write raw HTTP requests or JSON.

### 1. Defining & Triggering Workflows (The Client SDK)
Use the `WorkflowClient` in your main application to interact with the Orchestrator.

```python
from workersdk import WorkflowClient

client = WorkflowClient("http://localhost:8080")

# 1. Define the Blueprint
client.define_workflow("checkout-pipeline", [
    "charge-credit-card", 
    "generate-invoice",
    "send-welcome-email"
])

# 2. Trigger an Instance securely
run_id = client.trigger_workflow("checkout-pipeline", '{"order_id": 123}', idempotency_key="order-123")

# 3. Check the Status
status = client.get_run_status(run_id)
print(status)
```

### 2. Processing Steps (The Worker SDK)
Deploy lightweight microservices using the `Worker` class to actually perform the actions.

#### Python Worker
```python
from workersdk import Worker

def main():
    worker = Worker()

    # Register handlers for specific actions
    worker.register("generate-invoice", lambda payload: f"PDF Generated for: {payload}")
    
    # Connects to Postgres and starts polling
    worker.start("postgresql://admin:admin@postgres:5432/wf_engine")

if __name__ == "__main__":
    main()
```

#### Node.js Worker
```typescript
import { Worker } from 'workersdk';

const worker = new Worker();

worker.register('send-welcome-email', async (payload: string) => {
    // Simulate async IO
    await new Promise(resolve => setTimeout(resolve, 1000));
    return `Email sent to ${payload}`;
});

worker.start("postgresql://admin:admin@postgres:5432/wf_engine");
```

#### Java Worker
```java
import com.creedpetitt.workersdk.Worker;

public class ExampleWorkerApp {
    public static void main(String[] args) {
        Worker worker = new Worker();

        worker.register("charge-credit-card", payload -> {
            return "Payment Processed: " + payload;
        });

        worker.start("jdbc:postgresql://postgres:5432/wf_engine", "admin", "admin");
    }
}
```

## Tech Stack

* **Core:** Java 21, Spring Boot 3
* **Database & Queue Engine:** PostgreSQL 16
* **SDK Languages:** Java (JDBI), Python 3.11 (psycopg2), Node.js 20 (pg)

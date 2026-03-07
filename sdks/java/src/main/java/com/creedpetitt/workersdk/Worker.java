package com.creedpetitt.workersdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jdbi.v3.core.Jdbi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class Worker {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<String, Handler> handlers = new HashMap<>();
    private Jdbi jdbi;
    private String workerId;

    public void register(String action, Handler handler) {
        handlers.put(action, handler);
    }

    public void start() {
        start("jdbc:postgresql://postgres:5432/wf_engine", "admin", "admin");
    }

    public void start(String jdbcUrl, String username, String password) {
        this.workerId = "java-worker-" + UUID.randomUUID().toString().substring(0, 8);
        
        connectWithRetry(jdbcUrl, username, password);

        System.out.println("Worker " + workerId + " started, listening for jobs via Postgres SKIP LOCKED...");

        while (true) {
            try {
                pollAndProcess();
                Thread.sleep(500); // Polling interval
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("Error in polling loop: " + e.getMessage());
            }
        }
    }

    private void pollAndProcess() {
        if (handlers.isEmpty()) {
            return;
        }

        List<String> registeredActions = List.copyOf(handlers.keySet());

        jdbi.useTransaction(handle -> {
            String claimSql = """
                UPDATE workflow_job 
                SET status = 'PROCESSING', locked_by = :workerId
                WHERE id = (
                    SELECT id FROM workflow_job 
                    WHERE status = 'PENDING' AND action IN (<actions>)
                    ORDER BY created_at ASC
                    LIMIT 1 
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, workflow_run_id, action, payload
            """;

            Optional<Map<String, Object>> claimedJob = handle.createQuery(claimSql)
                    .bindList("actions", registeredActions)
                    .bind("workerId", workerId)
                    .mapToMap()
                    .findFirst();

            if (claimedJob.isEmpty()) {
                return; // No jobs available
            }

            Map<String, Object> jobRow = claimedJob.get();
            Long jobId = (Long) jobRow.get("id");
            String runId = (String) jobRow.get("workflow_run_id");
            String action = (String) jobRow.get("action");
            String payload = (String) jobRow.get("payload");

            System.out.println("Processing job: " + action + " for run: " + runId);

            Handler handler = handlers.get(action);
            String output;
            String status = "SUCCESS";

            try {
                output = handler.handle(payload);
            } catch (Exception e) {
                status = "FAILED";
                Map<String, String> errorMap = new HashMap<>();
                errorMap.put("error", e.getMessage());
                try {
                    output = MAPPER.writeValueAsString(errorMap);
                } catch (Exception jsonEx) {
                    output = "{\"error\":\"Serialization failed\"}";
                }
            }

            String updateSql = """
                UPDATE workflow_job 
                SET status = 'COMPLETED', result = :result, result_status = :resultStatus
                WHERE id = :id
            """;
            
            handle.createUpdate(updateSql)
                    .bind("result", output)
                    .bind("resultStatus", status)
                    .bind("id", jobId)
                    .execute();
        });
    }

    private void connectWithRetry(String jdbcUrl, String username, String password) {
        int maxAttempts = 30;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                this.jdbi = Jdbi.create(jdbcUrl, username, password);
                this.jdbi.withHandle(handle -> handle.execute("SELECT 1"));
                return;
            } catch (Exception e) {
                System.out.println("Postgres not ready, retrying (" + attempt + "/" + maxAttempts + ")...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                if (attempt == maxAttempts) {
                    throw new RuntimeException("Could not connect to Postgres after " + maxAttempts + " attempts", e);
                }
            }
        }
    }
}

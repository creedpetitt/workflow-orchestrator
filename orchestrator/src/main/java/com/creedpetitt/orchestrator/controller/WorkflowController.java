package com.creedpetitt.orchestrator.controller;

import com.creedpetitt.orchestrator.dto.CreateWorkflowRequest;
import com.creedpetitt.orchestrator.dto.TriggerWorkflowRequest;
import com.creedpetitt.orchestrator.model.WorkflowDefinition;
import com.creedpetitt.orchestrator.model.WorkflowRun;
import com.creedpetitt.orchestrator.service.WorkflowService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/workflow")
public class WorkflowController {

    private final WorkflowService workflowService;

    public WorkflowController(WorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    @PostMapping
    public ResponseEntity<String> createWorkflow(@RequestBody CreateWorkflowRequest req) {
        String id = workflowService.createWorkflow(req);
        return ResponseEntity.status(HttpStatus.CREATED).body(id);
    }

    @GetMapping
    public ResponseEntity<List<WorkflowDefinition>> getAllWorkflows() {
        return ResponseEntity.ok(workflowService.getAllWorkflows());
    }

    @GetMapping("/{id}")
    public ResponseEntity<WorkflowDefinition> getWorkflow(@PathVariable String id) {
        return ResponseEntity.ok(workflowService.getWorkflow(id));
    }

    @PostMapping("/{id}/trigger")
    public ResponseEntity<String> triggerWorkflow(
            @PathVariable String id,
            @RequestBody TriggerWorkflowRequest req,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey
    ) {
        String runId = workflowService.triggerWorkflow(id, req, idempotencyKey);
        return ResponseEntity.ok(runId);
    }

    @GetMapping("/run/{runId}")
    public ResponseEntity<WorkflowRun> getRunStatus(@PathVariable String runId) {
        WorkflowRun run = workflowService.getRunStatus(runId);
        return ResponseEntity.ok(run);
    }

    @GetMapping("/{id}/runs")
    public ResponseEntity<List<WorkflowRun>> getRunsForWorkflow(@PathVariable String id) {
        return ResponseEntity.ok(workflowService.getRuns(id, null));
    }

    @GetMapping("/runs")
    public ResponseEntity<List<WorkflowRun>> getAllRuns(
            @RequestParam(required = false) String workflowId,
            @RequestParam(required = false) String status) {
        return ResponseEntity.ok(workflowService.getRuns(workflowId, status));
    }
}

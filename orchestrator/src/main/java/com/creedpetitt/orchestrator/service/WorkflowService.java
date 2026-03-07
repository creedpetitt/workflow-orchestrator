package com.creedpetitt.orchestrator.service;

import com.creedpetitt.orchestrator.dto.CreateWorkflowRequest;
import com.creedpetitt.orchestrator.dto.TriggerWorkflowRequest;
import com.creedpetitt.orchestrator.exception.ResourceNotFoundException;
import com.creedpetitt.orchestrator.exception.WorkflowExecutionException;
import com.creedpetitt.orchestrator.model.StepResult;
import com.creedpetitt.orchestrator.model.WorkflowDefinition;
import com.creedpetitt.orchestrator.model.WorkflowJob;
import com.creedpetitt.orchestrator.model.WorkflowRun;
import com.creedpetitt.orchestrator.model.WorkflowStep;
import com.creedpetitt.orchestrator.repository.WorkflowDefinitionRepository;
import com.creedpetitt.orchestrator.repository.WorkflowJobRepository;
import com.creedpetitt.orchestrator.repository.WorkflowRunRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
public class WorkflowService {

    private final WorkflowDefinitionRepository workflowRepo;
    private final WorkflowRunRepository workflowRunRepo;
    private final WorkflowJobRepository workflowJobRepo;

    public WorkflowService(
            WorkflowDefinitionRepository workflowRepo, 
            WorkflowRunRepository workflowRunRepo,
            WorkflowJobRepository workflowJobRepo
    ) {
        this.workflowRepo = workflowRepo;
        this.workflowRunRepo = workflowRunRepo;
        this.workflowJobRepo = workflowJobRepo;
    }

    @Transactional
    public String createWorkflow(CreateWorkflowRequest req) {
        WorkflowDefinition workflow = new WorkflowDefinition();
        workflow.setId(req.id());

        List<WorkflowStep> steps = req.steps().stream()
                .map(stepDto -> {
                    WorkflowStep step = new WorkflowStep();
                    step.setAction(stepDto.action());
                    step.setStepIndex(stepDto.stepIndex());
                    step.setWorkflowDefinition(workflow);
                    return step;
                }).toList();

        workflow.setSteps(steps);
        workflowRepo.save(workflow);

        return workflow.getId();
    }

    @Transactional
    public String triggerWorkflow(String id, TriggerWorkflowRequest req, String idempotencyKey) {
        
        if (idempotencyKey != null && !idempotencyKey.isEmpty()) {
            Optional<WorkflowRun> existingRun = workflowRunRepo.findByIdempotencyKey(idempotencyKey);
            if (existingRun.isPresent()) {
                log.info("Idempotency hit for key: {}", idempotencyKey);
                return existingRun.get().getRunId();
            }
        }

        String runId = UUID.randomUUID().toString();

        WorkflowDefinition workflow = workflowRepo.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Workflow definition not found: " + id));

        WorkflowRun run = initializeWorkflowRun(runId, id, req.input(), idempotencyKey);
        
        // This save will throw a DataIntegrityViolationException if another thread 
        // with the exact same idempotencyKey saves first, preventing race conditions.
        workflowRunRepo.save(run);

        WorkflowStep firstStep = workflow.getSteps().getFirst();
        createWorkflowJob(runId, firstStep.getAction(), req.input());

        return runId;
    }

    public List<WorkflowDefinition> getAllWorkflows() {
        return workflowRepo.findAll();
    }

    public WorkflowDefinition getWorkflow(String id) {
        return workflowRepo.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Workflow definition not found: " + id));
    }

    public List<WorkflowRun> getRuns(String workflowId, String status) {
        if (workflowId != null && status != null) {
            return workflowRunRepo.findByWorkflowIdAndStatus(workflowId, status);
        } else if (workflowId != null) {
            return workflowRunRepo.findByWorkflowId(workflowId);
        } else if (status != null) {
            return workflowRunRepo.findByStatus(status);
        } else {
            return workflowRunRepo.findAllByOrderByStartTimeDesc();
        }
    }

    public WorkflowRun getRunStatus(String runId) {
        return workflowRunRepo.findById(runId)
                .orElseThrow(() -> new ResourceNotFoundException("Workflow run not found: " + runId));
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void pollForCompletedJobs() {
        List<WorkflowJob> finishedJobs = workflowJobRepo.findByStatusIn(List.of("COMPLETED", "FAILED"));

        for (WorkflowJob job : finishedJobs) {
            boolean keepJobInDatabase = false;
            try {
                keepJobInDatabase = processJobResult(job);
            } catch (Exception e) {
                log.error("Error processing completed job {}: {}", job.getId(), e.getMessage());
            } finally {
                // Delete successful jobs, but keep DEAD_LETTER and retrying jobs
                if (!keepJobInDatabase && "COMPLETED".equals(job.getStatus())) {
                    workflowJobRepo.delete(job);
                }
            }
        }
    }

    private boolean processJobResult(WorkflowJob job) {
        WorkflowRun run = workflowRunRepo.findById(job.getWorkflowRunId())
                .orElse(null);
                
        if (run == null) {
            log.error("Workflow run not found in Database for job: {}", job.getWorkflowRunId());
            return false;
        }

        if (!"RUNNING".equals(run.getStatus())) {
            return false;
        }

        String workflowId = run.getWorkflowId();
        WorkflowDefinition workflow = workflowRepo.findById(workflowId)
                .orElseThrow(() -> new ResourceNotFoundException("Workflow definition not found for run: " + workflowId));

        recordStepResult(run, job.getAction(), job.getResult());

        if ("FAILED".equals(job.getResultStatus())) {
            
            if (job.getRetryCount() < job.getMaxRetries()) {
                // Retry Logic
                job.setRetryCount(job.getRetryCount() + 1);
                job.setStatus("PENDING");
                job.setResult(null);
                job.setResultStatus(null);
                job.setLockedBy(null);
                workflowJobRepo.save(job);
                log.warn("Job failed, retrying. Run: {}, Action: {}, Attempt: {}/{}", run.getRunId(), job.getAction(), job.getRetryCount(), job.getMaxRetries());
                
                return false;
            } else {
                // Dead Letter Queue Logic
                job.setStatus("DEAD_LETTER");
                workflowJobRepo.save(job);
                handleWorkflowFailure(run, job.getResult());
                return true;
            }
            
        } else {
            int totalSteps = workflow.getSteps().size();
            if (run.getCurrentStep() >= totalSteps - 1) {
                handleWorkflowCompletion(run, job.getResult());
            } else {
                handleNextStep(run, workflow, job.getResult());
            }
            return false; // Safely delete successful jobs
        }
    }

    private WorkflowRun initializeWorkflowRun(String runId, String workflowId, String input, String idempotencyKey) {
        WorkflowRun run = new WorkflowRun();
        run.setRunId(runId);
        run.setWorkflowId(workflowId);
        run.setCurrentStep(0);
        run.setStatus("RUNNING");
        run.setStartTime(LocalDateTime.now());
        run.setInput(input);
        run.setIdempotencyKey(idempotencyKey);
        return run;
    }

    private void createWorkflowJob(String runId, String action, String payload) {
        WorkflowJob job = new WorkflowJob();
        job.setWorkflowRunId(runId);
        job.setAction(action);
        job.setPayload(payload);
        job.setStatus("PENDING");
        workflowJobRepo.save(job);
        log.info("Created workflow job for action: {}", action);
    }

    private void recordStepResult(WorkflowRun run, String action, String result) {
        StepResult stepResult = new StepResult();
        stepResult.setWorkflowRun(run);
        stepResult.setAction(action);
        stepResult.setStepIndex(run.getCurrentStep());
        stepResult.setResult(result);
        stepResult.setTimestamp(LocalDateTime.now());
        run.getStepResults().add(stepResult);
    }

    private void handleWorkflowFailure(WorkflowRun run, String errorResult) {
        run.setStatus("FAILED");
        run.setEndTime(LocalDateTime.now());
        run.setFinalOutput(errorResult);
        
        workflowRunRepo.save(run);
        log.error("Workflow failed: {} with error: {}", run.getRunId(), errorResult);
    }

    private void handleWorkflowCompletion(WorkflowRun run, String finalResult) {
        run.setStatus("COMPLETE");
        run.setEndTime(LocalDateTime.now());
        run.setFinalOutput(finalResult);
        
        workflowRunRepo.save(run);
        log.info("Workflow completed: {}", run.getRunId());
    }

    private void handleNextStep(WorkflowRun run, WorkflowDefinition workflow, String currentResult) {
        run.setCurrentStep(run.getCurrentStep() + 1);
        workflowRunRepo.save(run);

        WorkflowStep nextStep = workflow.getSteps().get(run.getCurrentStep());
        createWorkflowJob(run.getRunId(), nextStep.getAction(), currentResult);
    }
}

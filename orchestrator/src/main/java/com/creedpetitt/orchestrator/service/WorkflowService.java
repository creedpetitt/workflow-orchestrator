package com.creedpetitt.orchestrator.service;

import com.creedpetitt.orchestrator.config.Topics;
import com.creedpetitt.orchestrator.dto.CreateWorkflowRequest;
import com.creedpetitt.orchestrator.dto.ResultMessage;
import com.creedpetitt.orchestrator.dto.TriggerWorkflowRequest;
import com.creedpetitt.orchestrator.dto.JobMessage;
import com.creedpetitt.orchestrator.exception.ResourceNotFoundException;
import com.creedpetitt.orchestrator.exception.WorkflowExecutionException;
import com.creedpetitt.orchestrator.model.StepResult;
import com.creedpetitt.orchestrator.model.WorkflowDefinition;
import com.creedpetitt.orchestrator.model.WorkflowRun;
import com.creedpetitt.orchestrator.model.WorkflowStep;
import com.creedpetitt.orchestrator.repository.WorkflowDefinitionRepository;
import com.creedpetitt.orchestrator.repository.WorkflowRunRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
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
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public WorkflowService(
            WorkflowDefinitionRepository workflowRepo, WorkflowRunRepository workflowRunRepo,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper
    ) {
        this.workflowRepo = workflowRepo;
        this.workflowRunRepo = workflowRunRepo;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
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

        workflowRunRepo.save(run);

        WorkflowStep firstStep = workflow.getSteps().getFirst();
        sendJobToKafka(new JobMessage(runId, firstStep.getAction(), req.input()));

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

    @Transactional
    @KafkaListener(topics = Topics.WORKFLOW_RESULTS)
    public void processResult(String message) {
        ResultMessage result = deserializeMessage(message);
        if (result == null) return;

        WorkflowRun run = workflowRunRepo.findById(result.workflowRunId())
                .orElse(null);
                
        if (run == null) {
            log.error("workflow run not found in Database: {}", result.workflowRunId());
            return;
        }

        String workflowId = run.getWorkflowId();
        WorkflowDefinition workflow = workflowRepo.findById(workflowId)
                .orElseThrow(() -> new ResourceNotFoundException("Workflow definition not found for run: " + workflowId));

        recordStepResult(run, result);

        if ("FAILED".equals(result.status())) {
            handleWorkflowFailure(run, result.result());
        } else {
            int totalSteps = workflow.getSteps().size();
            if (run.getCurrentStep() >= totalSteps - 1) {
                handleWorkflowCompletion(run, result.result());
            } else {
                handleNextStep(run, workflow, result.result());
            }
        }
    }

    private void handleWorkflowFailure(WorkflowRun run, String errorResult) {
        run.setStatus("FAILED");
        run.setEndTime(LocalDateTime.now());
        run.setFinalOutput(errorResult);
        
        workflowRunRepo.save(run);
        log.error("Workflow failed: {} with error: {}", run.getRunId(), errorResult);
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

    private void sendJobToKafka(JobMessage job) {
        try {
            String jobJson = objectMapper.writeValueAsString(job);
            kafkaTemplate.send(Topics.WORKFLOW_JOBS, jobJson);
            log.info("Sent job to Kafka: {}", job.action());
        } catch (Exception e) {
            throw new WorkflowExecutionException("Failed to send job to Kafka", e);
        }
    }

    private ResultMessage deserializeMessage(String message) {
        try {
            return objectMapper.readValue(message, ResultMessage.class);
        } catch (Exception e) {
            log.error("Could not deserialize workflow result: {}", message, e);
            return null;
        }
    }

    private void recordStepResult(WorkflowRun run, ResultMessage result) {
        StepResult stepResult = new StepResult();
        stepResult.setWorkflowRun(run);
        stepResult.setAction(result.action());
        stepResult.setStepIndex(run.getCurrentStep());
        stepResult.setResult(result.result());
        stepResult.setTimestamp(LocalDateTime.now());
        run.getStepResults().add(stepResult);
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
        JobMessage job = new JobMessage(run.getRunId(), nextStep.getAction(), currentResult);
        sendJobToKafka(job);
    }
}

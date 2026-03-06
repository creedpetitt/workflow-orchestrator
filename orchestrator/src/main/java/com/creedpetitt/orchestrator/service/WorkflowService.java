package com.creedpetitt.orchestrator.service;

import com.creedpetitt.orchestrator.config.Topics;
import com.creedpetitt.orchestrator.dto.CreateWorkflowRequest;
import com.creedpetitt.orchestrator.dto.ResultMessage;
import com.creedpetitt.orchestrator.dto.TriggerWorkflowRequest;
import com.creedpetitt.orchestrator.dto.JobMessage;
import com.creedpetitt.orchestrator.model.StepResult;
import com.creedpetitt.orchestrator.model.WorkflowDefinition;
import com.creedpetitt.orchestrator.model.WorkflowRun;
import com.creedpetitt.orchestrator.model.WorkflowStep;
import com.creedpetitt.orchestrator.repository.WorkflowDefinitionRepository;
import com.creedpetitt.orchestrator.repository.WorkflowRunRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class WorkflowService {

    private final WorkflowDefinitionRepository workflowRepo;
    private final WorkflowRunRepository workflowRunRepo;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    public WorkflowService(
            WorkflowDefinitionRepository workflowRepo, WorkflowRunRepository workflowRunRepo,
            KafkaTemplate<String, String> kafkaTemplate,
            RedisTemplate<String, String> redisTemplate,
            ObjectMapper objectMapper
    ) {
        this.workflowRepo = workflowRepo;
        this.workflowRunRepo = workflowRunRepo;
        this.kafkaTemplate = kafkaTemplate;
        this.redisTemplate = redisTemplate;
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

        String runId = UUID.randomUUID().toString();

        // Idempotency Check
        if (idempotencyKey != null && !idempotencyKey.isEmpty()) {
            Boolean isNew = redisTemplate.opsForValue().setIfAbsent("idempotency:" + idempotencyKey, runId, Duration.ofHours(24));
            if (Boolean.FALSE.equals(isNew)) {
                log.info("Idempotency hit for key: {}", idempotencyKey);
                return redisTemplate.opsForValue().get("idempotency:" + idempotencyKey);
            }
        }

        WorkflowDefinition workflow = workflowRepo.findById(id)
                .orElseThrow(() -> new RuntimeException("Workflow not found" + id));

        WorkflowRun run = new WorkflowRun();
        run.setRunId(runId);
        run.setWorkflowId(id);
        run.setCurrentStep(0);
        run.setStatus("RUNNING");
        run.setStartTime(LocalDateTime.now());
        run.setInput(req.input());

        workflowRunRepo.save(run);

        String key = "workflow:run:" + runId;
        try {
            String runJson = objectMapper.writeValueAsString(run);
            redisTemplate.opsForValue().set(key, runJson, Duration.ofHours(24));
        } catch (Exception e) {
            throw new RuntimeException("Failed to save workflow run to Redis", e);
        }

        WorkflowStep firstStep = workflow.getSteps().getFirst();

        JobMessage job =  new JobMessage(runId, firstStep.getAction(), req.input());

        try {
            String jobJson = objectMapper.writeValueAsString(job);
            kafkaTemplate.send(Topics.WORKFLOW_JOBS, jobJson);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send job to Kafka", e);
        }

        return runId;
    }

    public List<WorkflowDefinition> getAllWorkflows() {
        return workflowRepo.findAll();
    }

    public List<WorkflowRun> getAllRuns() {
        // Retrieve most recent runs first
        return workflowRunRepo.findAllByOrderByStartTimeDesc();
    }

    public WorkflowRun getRunStatus(String runId) {

        String key = "workflow:run:" + runId;

        String json = redisTemplate.opsForValue().get(key);

        if (json == null) {
            throw new RuntimeException("workflow run not found:" + runId);
        }

        try {
            return objectMapper.readValue(json, WorkflowRun.class);
        } catch (Exception e) {
            throw new RuntimeException("could not deserialize workflow run:" + runId, e);
        }
    }

    @Transactional
    @KafkaListener(topics = Topics.WORKFLOW_RESULTS)
    public void processResult(String message) {

        ResultMessage result;
        try {
            result = objectMapper.readValue(message, ResultMessage.class);
        } catch (Exception e) {
            log.error("Could not deserialize workflow result: {}", message, e);
            return;
        }

        String key = "workflow:run:" + result.workflowRunId();
        String runJson = redisTemplate.opsForValue().get(key);

        if (runJson == null) {
            log.error("workflow run not found: {}", result.workflowRunId());
            return;
        }

        WorkflowRun run = null;
        try {
            run = objectMapper.readValue(runJson, WorkflowRun.class);
        } catch (Exception e) {
            log.error("Could not deserialize workflow run: {}", runJson, e);
            return;
        }

        WorkflowDefinition workflow = workflowRepo.findById(run.getWorkflowId())
                .orElseThrow(() -> new RuntimeException("Workflow not found"));

        int totalSteps =  workflow.getSteps().size();

        StepResult stepResult = new StepResult();
        stepResult.setWorkflowRun(run);
        stepResult.setAction(result.action());
        stepResult.setStepIndex(run.getCurrentStep());
        stepResult.setResult(result.result());
        stepResult.setTimestamp(LocalDateTime.now());
        run.getStepResults().add(stepResult);

        if (run.getCurrentStep() >=  totalSteps - 1) {
            run.setStatus("COMPLETE");
            run.setEndTime(LocalDateTime.now());
            run.setFinalOutput(result.result());
            workflowRunRepo.save(run);

            try {
                String updatedJson =  objectMapper.writeValueAsString(run);
                redisTemplate.opsForValue().set(key, updatedJson, Duration.ofHours(24));
            } catch (Exception e) {
                throw new RuntimeException("Failed to update redis: " + e.getMessage(), e);
            }

            log.info("Workflow completed: {}", run.getRunId());

        } else {
            run.setCurrentStep(run.getCurrentStep() + 1);
            workflowRunRepo.save(run);

            try {
                String updatedJson = objectMapper.writeValueAsString(run);
                redisTemplate.opsForValue().set(key, updatedJson, Duration.ofHours(24));
            } catch (Exception e) {
                log.error("Failed to update Redis", e);
                return;
            }

            WorkflowStep nextStep = workflow.getSteps().get(run.getCurrentStep());

            JobMessage job = new JobMessage(
                run.getRunId(),
                nextStep.getAction(),
                result.result()
            );

            try {
                String jobJson = objectMapper.writeValueAsString(job);
                kafkaTemplate.send(Topics.WORKFLOW_JOBS, jobJson);
                log.info("Sent next job: {}", nextStep.getAction());
            } catch (Exception e) {
                log.error("Failed to send next job", e);
            }
        }
    }
}

package com.creedpetitt.orchestrator.repository;

import com.creedpetitt.orchestrator.model.WorkflowRun;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface WorkflowRunRepository
        extends JpaRepository<WorkflowRun, String> {

    Optional<WorkflowRun> findByIdempotencyKey(String idempotencyKey);

    List<WorkflowRun> findByWorkflowId(String workflowId);

    List<WorkflowRun> findByStatus(String status);

    List<WorkflowRun> findByWorkflowIdAndStatus(String workflowId, String status);

    List<WorkflowRun> findAllByOrderByStartTimeDesc();

    List<WorkflowRun> findByStartTimeBetween(LocalDateTime start, LocalDateTime end);
}

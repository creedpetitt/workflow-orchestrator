package com.creedpetitt.orchestrator.repository;

import com.creedpetitt.orchestrator.model.WorkflowJob;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface WorkflowJobRepository extends JpaRepository<WorkflowJob, Long> {
    List<WorkflowJob> findByStatusIn(List<String> statuses);
}

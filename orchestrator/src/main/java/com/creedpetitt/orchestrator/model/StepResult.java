package com.creedpetitt.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name = "step_result")
public class StepResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "workflow_run_id")
    @JsonIgnore
    private WorkflowRun workflowRun;

    @Column
    private String action;

    @Column
    private int stepIndex;

    @Column(columnDefinition = "TEXT")
    private String result;

    @Column
    private LocalDateTime timestamp;
}

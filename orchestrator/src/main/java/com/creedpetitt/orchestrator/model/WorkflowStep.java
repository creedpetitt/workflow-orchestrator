package com.creedpetitt.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "workflow_step")
public class WorkflowStep {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column
    private String action;

    @Column
    private int stepIndex;

    @ManyToOne
    @JoinColumn(name = "workflow_definition_id")
    @JsonIgnore
    private WorkflowDefinition workflowDefinition;

}

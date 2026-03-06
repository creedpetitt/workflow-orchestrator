package com.creedpetitt.orchestrator.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Entity
@Table(name = "workflow_definition")
public class WorkflowDefinition {

    @Id
    private String id;

    @OneToMany(mappedBy = "workflowDefinition", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    private List<WorkflowStep> steps = new ArrayList<>();
}

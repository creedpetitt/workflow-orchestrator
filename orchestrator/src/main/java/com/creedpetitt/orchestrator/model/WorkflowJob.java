package com.creedpetitt.orchestrator.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name = "workflow_job")
public class WorkflowJob {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column
    private String workflowRunId;

    @Column
    private String action;

    @Column(columnDefinition = "TEXT")
    private String payload;

    @Column
    private String status; // PENDING, PROCESSING, COMPLETED, FAILED

    @Column
    private String lockedBy; // For worker identification

    @Column(columnDefinition = "TEXT")
    private String result;

    @Column
    private String resultStatus; // SUCCESS, FAILED

    @Column
    private LocalDateTime createdAt;
    
    @Column
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (status == null) {
            status = "PENDING";
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}

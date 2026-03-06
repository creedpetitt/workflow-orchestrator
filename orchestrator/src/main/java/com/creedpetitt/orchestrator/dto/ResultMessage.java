package com.creedpetitt.orchestrator.dto;

public record ResultMessage(
     String workflowRunId,
     String action,
     String result,
     String status
) {}

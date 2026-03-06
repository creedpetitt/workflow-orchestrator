package com.creedpetitt.workersdk;

public record ResultMessage(
        String workflowRunId,
        String action,
        String result,
        String status
) {}
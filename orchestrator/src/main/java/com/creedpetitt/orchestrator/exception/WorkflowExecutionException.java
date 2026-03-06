package com.creedpetitt.orchestrator.exception;

public class WorkflowExecutionException extends RuntimeException {
    public WorkflowExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}

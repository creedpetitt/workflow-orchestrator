export interface JobMessage {
    workflowRunId: string;
    action: string;
    payload: string;
}

export interface ResultMessage {
    workflowRunId: string;
    action: string;
    result: string;
    status: string;
}
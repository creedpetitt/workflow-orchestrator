from dataclasses import dataclass


@dataclass
class JobMessage:
    workflow_run_id: str
    action: str
    payload: str

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            workflow_run_id=data['workflowRunId'],
            action=data['action'],
            payload=data['payload']
        )


@dataclass
class ResultMessage:
    workflow_run_id: str
    action: str
    result: str
    status: str

    def to_dict(self):
        return {
            'workflowRunId': self.workflow_run_id,
            'action': self.action,
            'result': self.result,
            'status': self.status
        }

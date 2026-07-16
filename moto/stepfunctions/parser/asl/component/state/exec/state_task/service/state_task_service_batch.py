from collections.abc import Callable
from typing import Any, Final

import botocore.session
from botocore.exceptions import ClientError

from moto.batch.utils import JobStatus
from moto.stepfunctions.parser.api import HistoryEventType, TaskFailedEventDetails
from moto.stepfunctions.parser.asl.component.common.error_name.custom_error_name import (
    CustomErrorName,
)
from moto.stepfunctions.parser.asl.component.common.error_name.failure_event import (
    FailureEvent,
    FailureEventException,
)
from moto.stepfunctions.parser.asl.component.common.error_name.states_error_name import (
    StatesErrorName,
)
from moto.stepfunctions.parser.asl.component.common.error_name.states_error_name_type import (
    StatesErrorNameType,
)
from moto.stepfunctions.parser.asl.component.state.exec.state_task.credentials import (
    StateCredentials,
)
from moto.stepfunctions.parser.asl.component.state.exec.state_task.service.resource import (
    ResourceCondition,
    ResourceRuntimePart,
)
from moto.stepfunctions.parser.asl.component.state.exec.state_task.service.state_task_service_callback import (
    StateTaskServiceCallback,
)
from moto.stepfunctions.parser.asl.eval.environment import Environment
from moto.stepfunctions.parser.asl.eval.event.event_detail import EventDetails
from moto.stepfunctions.parser.asl.utils.boto_client import boto_client_for
from moto.stepfunctions.parser.asl.utils.encoding import to_json_str

_SUPPORTED_INTEGRATION_PATTERNS: Final[set[ResourceCondition]] = {
    ResourceCondition.WaitForTaskToken,
    ResourceCondition.Sync,
}

_BATCH_JOB_TERMINATION_STATUS_SET: Final[set[JobStatus]] = {
    JobStatus.SUCCEEDED,
    JobStatus.FAILED,
}

_ENVIRONMENT_VARIABLE_MANAGED_BY_AWS: Final[str] = "MANAGED_BY_AWS"
_ENVIRONMENT_VARIABLE_MANAGED_BY_AWS_VALUE: Final[str] = "STARTED_BY_STEP_FUNCTIONS"

_SUPPORTED_API_PARAM_BINDINGS: Final[dict[str, set[str]]] = {
    "submitjob": {
        "ArrayProperties",
        "ContainerOverrides",
        "DependsOn",
        "JobDefinition",
        "JobName",
        "JobQueue",
        "Parameters",
        "RetryStrategy",
        "Timeout",
        "Tags",
    }
}


class StateTaskServiceBatch(StateTaskServiceCallback):
    _BOTO_SERVICE_MODEL: Final = botocore.session.get_session().get_service_model(
        "batch"
    )

    def __init__(self):
        super().__init__(supported_integration_patterns=_SUPPORTED_INTEGRATION_PATTERNS)

    def _get_supported_parameters(self) -> set[str] | None:
        return _SUPPORTED_API_PARAM_BINDINGS.get(self.resource.api_action.lower())

    @staticmethod
    def _operation_name_from_api_action(api_action: str) -> str:
        # Converts either the resource's lowerCamelCase api_action (e.g.
        # "submitJob") or a snake_case override (e.g. "describe_jobs", as
        # passed by callers of _normalise_response) into the PascalCase
        # operation name botocore's service model is keyed by (e.g.
        # "SubmitJob" / "DescribeJobs").
        if "_" in api_action:
            return "".join(part.capitalize() for part in api_action.split("_"))
        return api_action[0].upper() + api_action[1:]

    def _normalise_parameters(
        self,
        parameters: dict,
        boto_service_name: str | None = None,
        service_action_name: str | None = None,
    ) -> None:
        # Batch is a rest-json protocol service: botocore's generated client
        # methods expect lowerCamelCase kwargs (e.g. jobName/jobQueue), not
        # the PascalCase field names ASL Parameters use (JobName/JobQueue).
        # Without this conversion boto3 either rejects the call outright or
        # (with parameter validation disabled, as moto's boto_client_for
        # does) fails deeper inside botocore's request serializer.
        operation_name = self._operation_name_from_api_action(
            service_action_name or self.resource.api_action
        )
        operation_model = self._BOTO_SERVICE_MODEL.operation_model(operation_name)
        self._to_boto_request(parameters, operation_model.input_shape)

    def _normalise_response(
        self,
        response: Any,
        boto_service_name: str | None = None,
        service_action_name: str | None = None,
    ) -> None:
        operation_name = self._operation_name_from_api_action(
            service_action_name or self.resource.api_action
        )
        operation_model = self._BOTO_SERVICE_MODEL.operation_model(operation_name)
        self._from_boto_response(response, operation_model.output_shape)

    @staticmethod
    def _attach_aws_environment_variables(parameters: dict) -> None:
        # Attaches to the ContainerOverrides environment variables the AWS managed flags.
        container_overrides = parameters.get("ContainerOverrides")
        if container_overrides is None:
            container_overrides = {}
            parameters["ContainerOverrides"] = container_overrides

        environment = container_overrides.get("Environment")
        if environment is None:
            environment = []
            container_overrides["Environment"] = environment

        environment.append(
            {
                "name": _ENVIRONMENT_VARIABLE_MANAGED_BY_AWS,
                "value": _ENVIRONMENT_VARIABLE_MANAGED_BY_AWS_VALUE,
            }
        )

    def _before_eval_execution(
        self,
        env: Environment,
        resource_runtime_part: ResourceRuntimePart,
        raw_parameters: dict,
        state_credentials: StateCredentials,
    ) -> None:
        if self.resource.condition == ResourceCondition.Sync:
            self._attach_aws_environment_variables(parameters=raw_parameters)
        super()._before_eval_execution(
            env=env,
            resource_runtime_part=resource_runtime_part,
            raw_parameters=raw_parameters,
            state_credentials=state_credentials,
        )

    def _from_error(self, env: Environment, ex: Exception) -> FailureEvent:
        if isinstance(ex, ClientError):
            error_code = ex.response["Error"]["Code"]
            error_name = f"Batch.{error_code}"
            status_code = ex.response["ResponseMetadata"]["HTTPStatusCode"]
            error_message = ex.response["Error"]["Message"]
            request_id = ex.response["ResponseMetadata"]["RequestId"]
            response_details = "; ".join(
                [
                    "Service: AWSBatch",
                    f"Status Code: {status_code}",
                    f"Error Code: {error_code}",
                    f"Request ID: {request_id}",
                    "Proxy: null",
                ]
            )
            cause = f"Error executing request, Exception : {error_message}, RequestId: {request_id} ({response_details})"
            return FailureEvent(
                env=env,
                error_name=CustomErrorName(error_name),
                event_type=HistoryEventType.TaskFailed,
                event_details=EventDetails(
                    taskFailedEventDetails=TaskFailedEventDetails(
                        error=error_name,
                        cause=cause,
                        resource=self._get_sfn_resource(),
                        resourceType=self._get_sfn_resource_type(),
                    )
                ),
            )
        return super()._from_error(env=env, ex=ex)

    def _build_sync_resolver(
        self,
        env: Environment,
        resource_runtime_part: ResourceRuntimePart,
        normalised_parameters: dict,
        state_credentials: StateCredentials,
    ) -> Callable[[], Any] | None:
        batch_client = boto_client_for(
            service="batch",
            region=resource_runtime_part.region,
            state_credentials=state_credentials,
        )
        submission_output: dict = env.stack.pop()
        # _eval_execution normalises the submit_job response to PascalCase
        # (via _normalise_response) before this callback machinery runs.
        job_id = submission_output["JobId"]

        def _sync_resolver() -> dict | None:
            describe_jobs_response = batch_client.describe_jobs(jobs=[job_id])
            describe_jobs = describe_jobs_response["jobs"]
            if describe_jobs:
                describe_job = describe_jobs[0]
                describe_job_status: JobStatus = describe_job["status"]
                # Add raise error if abnormal state
                if describe_job_status in _BATCH_JOB_TERMINATION_STATUS_SET:
                    self._normalise_response(
                        response=describe_jobs_response,
                        service_action_name="describe_jobs",
                    )
                    if describe_job_status == JobStatus.SUCCEEDED:
                        return describe_job

                    raise FailureEventException(
                        FailureEvent(
                            env=env,
                            error_name=StatesErrorName(
                                typ=StatesErrorNameType.StatesTaskFailed
                            ),
                            event_type=HistoryEventType.TaskFailed,
                            event_details=EventDetails(
                                taskFailedEventDetails=TaskFailedEventDetails(
                                    resource=self._get_sfn_resource(),
                                    resourceType=self._get_sfn_resource_type(),
                                    error=StatesErrorNameType.StatesTaskFailed.to_name(),
                                    cause=to_json_str(describe_job),
                                )
                            ),
                        )
                    )
            return None

        return _sync_resolver

    def _eval_service_task(
        self,
        env: Environment,
        resource_runtime_part: ResourceRuntimePart,
        normalised_parameters: dict,
        state_credentials: StateCredentials,
    ):
        service_name = self._get_boto_service_name()
        api_action = self._get_boto_service_action()
        batch_client = boto_client_for(
            region=resource_runtime_part.region,
            service=service_name,
            state_credentials=state_credentials,
        )
        response = getattr(batch_client, api_action)(**normalised_parameters)
        response.pop("ResponseMetadata", None)
        env.stack.append(response)

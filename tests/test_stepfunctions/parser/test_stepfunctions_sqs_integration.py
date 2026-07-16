import json
from unittest import SkipTest
from uuid import uuid4

import boto3

from moto import mock_aws, settings

from . import sfn_role_policy, verify_execution_result
from .templates.templates import load_template


@mock_aws(config={"stepfunctions": {"execute_state_machine": True}})
def test_state_machine_calling_sqs_with_heartbeat():
    if not settings.TEST_DECORATOR_MODE:
        raise SkipTest("No point in testing this in ServerMode")
    sqs = boto3.client("sqs", "us-east-1")
    sfn = boto3.client("stepfunctions", "us-east-1")
    queue_name = f"queue{uuid4()}"
    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]

    message_txt = "test_message_txt"
    exec_input = {"QueueUrl": queue_url, "Message": message_txt}

    expected_status = "RUNNING"
    tmpl_name = "services/sqs_heartbeat"

    def _verify_result(client, execution, execution_arn):
        resp = sqs.receive_message(QueueUrl=queue_url)
        if "Messages" in resp:
            task_token = json.loads(resp["Messages"][0]["Body"])["TaskToken"]

            sfn.send_task_heartbeat(taskToken=task_token)
            sfn.send_task_failure(taskToken=task_token)
            return True

        return False

    verify_execution_result(
        _verify_result, expected_status, tmpl_name, exec_input=json.dumps(exec_input)
    )


@mock_aws(config={"stepfunctions": {"execute_state_machine": True}})
def test_start_execution_while_another_execution_is_waiting_for_task_token():
    # Regression test: StartExecution used to crash with
    # "TypeError: cannot pickle '_thread.RLock' object" whenever it ran
    # while another execution of the *same* state machine was still paused
    # on .waitForTaskToken. copy.deepcopy(state_machine) in start_execution
    # was walking that other execution's live threading.Event/Lock (held
    # by its still-open callback) via the state machine's own `executions`
    # list and its `backend` back-reference. See
    # StateMachineInstance.__deepcopy__ in moto/stepfunctions/models.py.
    if not settings.TEST_DECORATOR_MODE:
        raise SkipTest("No point in testing this in ServerMode")
    iam = boto3.client("iam", region_name="us-east-1")
    role_name = f"sfn_role_{str(uuid4())[0:6]}"
    sfn_role = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(sfn_role_policy),
        Path="/",
    )["Role"]["Arn"]

    sqs = boto3.client("sqs", "us-east-1")
    sfn = boto3.client("stepfunctions", "us-east-1")
    queue_url = sqs.create_queue(QueueName=f"queue{uuid4()}")["QueueUrl"]
    exec_input = json.dumps({"QueueUrl": queue_url, "Message": "hello"})

    definition = load_template("services/sqs_heartbeat")
    sm_arn = sfn.create_state_machine(
        name=f"sm{uuid4()}", definition=json.dumps(definition), roleArn=sfn_role
    )["stateMachineArn"]

    exec1_arn = None
    exec2_arn = None
    try:
        exec1_arn = sfn.start_execution(
            stateMachineArn=sm_arn, name="exec-1", input=exec_input
        )["executionArn"]

        # This used to raise TypeError before exec-1 ever got a chance to
        # resolve - exec-1 is deliberately left RUNNING (paused on
        # .waitForTaskToken) for this call.
        exec2_arn = sfn.start_execution(
            stateMachineArn=sm_arn, name="exec-2", input=exec_input
        )["executionArn"]
        assert exec2_arn != exec1_arn
        assert sfn.describe_execution(executionArn=exec1_arn)["status"] == "RUNNING"
    finally:
        for exec_arn in (exec1_arn, exec2_arn):
            if exec_arn is not None:
                try:
                    sfn.stop_execution(executionArn=exec_arn)
                except Exception:
                    pass
        sfn.delete_state_machine(stateMachineArn=sm_arn)
        iam.delete_role(RoleName=role_name)


@mock_aws(config={"stepfunctions": {"execute_state_machine": True}})
def test_state_machine_calling_sqs_with_task_success():
    if not settings.TEST_DECORATOR_MODE:
        raise SkipTest("No point in testing this in ServerMode")
    sqs = boto3.client("sqs", "us-east-1")
    sfn = boto3.client("stepfunctions", "us-east-1")
    queue_name = f"queue{uuid4()}"
    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]

    message_txt = "test_message_txt"
    exec_input = {"QueueUrl": queue_url, "Message": message_txt}

    expected_status = "RUNNING"
    tmpl_name = "services/sqs_heartbeat"

    def _verify_result(client, execution, execution_arn):
        resp = sqs.receive_message(QueueUrl=queue_url)
        if "Messages" in resp:
            task_token = json.loads(resp["Messages"][0]["Body"])["TaskToken"]

            sfn.send_task_success(taskToken=task_token, output="it's done")
            return True

        return False

    verify_execution_result(
        _verify_result, expected_status, tmpl_name, exec_input=json.dumps(exec_input)
    )

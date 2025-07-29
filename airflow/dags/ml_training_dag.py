import logging
import boto3
import time
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.ec2 import (
    EC2CreateInstanceOperator,
    EC2TerminateInstanceOperator,
)
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
import paramiko
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Airflow connexions
aws_conn = BaseHook.get_connection("aws_default")
snowflake_conn = BaseHook.get_connection("snowflake_rescue_predict_db")

# Jenkins Configuration: Load from Airflow Variables
JENKINS_URL = Variable.get("JENKINS_URL")
JENKINS_USER = Variable.get("JENKINS_USER")
JENKINS_TOKEN = Variable.get("JENKINS_TOKEN")
JENKINS_JOB_NAME = Variable.get("JENKINS_JOB_NAME")

# Get AWS connection details from Airflow
KEY_PAIR_NAME = Variable.get("KEY_PAIR_NAME")
KEY_PATH = Variable.get("KEY_PATH")  # Path to your private key inside the container
AMI_ID = Variable.get("AMI_ID")
SECURITY_GROUP_ID = Variable.get("SECURITY_GROUP_ID")
INSTANCE_TYPE = Variable.get("INSTANCE_TYPE")
aws_conn = BaseHook.get_connection("aws_default")  # Use the Airflow AWS connection
aws_access_key_id = aws_conn.login
aws_secret_access_key = aws_conn.password
region_name = aws_conn.extra_dejson.get(
    "region_name", "eu-west-3"
)  # Default to 'eu-west-3'

# Retrieve other env variables for MLFlow to run
MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_ID = Variable.get("MLFLOW_EXPERIMENT_ID")
MLFLOW_LOGGED_MODEL = Variable.get("MLFLOW_LOGGED_MODEL")
AWS_ACCESS_KEY_ID = aws_access_key_id
AWS_SECRET_ACCESS_KEY = aws_secret_access_key

SNOWFLAKE_USER = Variable.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = Variable.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = Variable.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = Variable.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_SCHEMA = Variable.get("SNOWFLAKE_SCHEMA")
SNOWFLAKE_DATABASE = Variable.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_ROLE = ""

# DB_USER = snowflake_conn.login
# DB_PASSWORD = snowflake_conn.password
# DB_HOST = snowflake_conn.host
# DB_PORT = snowflake_conn.port
# DB_NAME = snowflake_conn.schema


if not all([JENKINS_URL, JENKINS_USER, JENKINS_TOKEN]):
    raise ValueError("Missing one or more Jenkins configuration environment variables")

# DAG Configuration
DAG_ID = "jenkins_ec2_ml_training_dag"
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 0 31 12 *",
    default_args=default_args,
    description="Poll Jenkins, launch EC2, and run ML training",
    catchup=False,
    tags=["jenkins", "ec2", "ml-training"],
) as dag:
    # Optionnal add a jenkins to verify DAG before launching an EC2 instance

    with TaskGroup(group_id="training_branch") as training_branch:
        create_ec2_instance = EC2CreateInstanceOperator(
            task_id="create_ec2_instance",
            region_name=region_name,
            image_id=AMI_ID,
            max_count=1,
            min_count=1,
            config={  # Dictionary for arbitrary parameters to the boto3 `run_instances` call
                "InstanceType": INSTANCE_TYPE,
                "KeyName": KEY_PAIR_NAME,
                "SecurityGroupIds": [SECURITY_GROUP_ID],
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [{"Key": "Purpose", "Value": "ML-Training"}],
                    }
                ],
            },
            wait_for_completion=True,  # Wait for the instance to be running before proceeding
        )

        @task
        def check_ec2_status(instance_id):
            """Check if the EC2 instance has passed both status checks (2/2 checks passed)."""
            max_wait_time = timedelta(minutes=15)
            start_time = datetime.utcnow()

            ec2_client = boto3.client(
                "ec2",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
            )
            passed_checks = False

            while not passed_checks and (datetime.now() - start_time) < max_wait_time:
                # Get the instance status
                response = ec2_client.describe_instance_status(InstanceIds=instance_id)

                # Check if there is any status information returned
                if response["InstanceStatuses"]:
                    instance_status = response["InstanceStatuses"][0]

                    system_status = instance_status["SystemStatus"]["Status"]
                    instance_status_check = instance_status["InstanceStatus"]["Status"]

                    # Log the current status
                    logging.info(
                        f"System Status: {system_status}, Instance Status: {instance_status_check}"
                    )

                    # Check if both status checks are passed
                    if system_status == "ok" and instance_status_check == "ok":
                        logging.info(
                            f"Instance {instance_id} has passed 2/2 status checks."
                        )
                        passed_checks = True
                    else:
                        logging.info(
                            f"Waiting for instance {instance_id} to pass 2/2 status checks..."
                        )
                else:
                    logging.info(
                        f"No status available for instance {instance_id} yet. Waiting..."
                    )

                # Wait before polling again
                time.sleep(15)

            if not passed_checks:
                raise AirflowException(
                    f"EC2 instance {instance_id} did not pass status checks within {max_wait_time}."
                )

            return True

        @task
        def get_ec2_public_ip(instance_id):
            """Retrieve the EC2 instance public IP for SSH."""

            # Initialize the EC2 resource using boto3 with credentials from Airflow connection
            ec2 = boto3.resource(
                "ec2",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
            )

            # Access EC2 instance by ID
            instance = ec2.Instance(instance_id[0])

            # Wait for the instance to be running
            instance.wait_until_running()
            instance.reload()

            # Get the instance's public IP
            public_ip = instance.public_ip_address
            logging.info(f"Public IP of EC2 Instance: {public_ip}")

            # Return the public IP for the SSH task
            return public_ip

        ec2_public_ip = get_ec2_public_ip(create_ec2_instance.output)
        check_ec2_instance = check_ec2_status(create_ec2_instance.output)

        logging.info(f"ec2 instance output {create_ec2_instance.output}")

        def _generate_remote_training_command():
            """Generates the shell command to be executed on the remote EC2 instance."""

            # Environment variables for MLflow and Snowflake
            # TODO : change with connection variables
            # export SNOWFLAKE_USER={snowflake_conn.login}
            # export SNOWFLAKE_PASSWORD={snowflake_conn.password}
            # export SNOWFLAKE_WAREHOUSE={snowflake_conn.warehouse}
            # export SNOWFLAKE_SCHEMA={snowflake_conn.schema}
            # export SNOWFLAKE_DATABASE={snowflake_conn.database}
            # export SNOWFLAKE_ROLE={snowflake_conn.role}
            env_vars = {
                "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
                "MLFLOW_EXPERIMENT_ID": MLFLOW_EXPERIMENT_ID,
                "MLFLOW_LOGGED_MODEL": MLFLOW_LOGGED_MODEL,
                "MLFLOW_EXPERIMENT_NAME": "jedha-lead",
                "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
                "SNOWFLAKE_USER": SNOWFLAKE_USER,
                "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
                "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
                "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
                "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
                "SNOWFLAKE_ROLE": SNOWFLAKE_ROLE,
            }

            # Convert environment variables to export commands
            export_commands = [
                f'export {key}="{value}"' for key, value in env_vars.items()
            ]

            # Core training commands
            training_commands = [
                "git clone https://github.com/littlerobinson/ml-rescue-predict.git",
                "cd ml-rescue-predict/training",
                "docker build -t ml-rescue-predict-training .",
                "cp run.sh.example run.sh",
                "chmod +x run.sh",
                "./run.sh",
            ]

            # Combine all commands
            return "\n".join(export_commands + training_commands)

        @task
        def run_training_via_paramiko(public_ip):
            """Use Paramiko to SSH into the EC2 instance and run ML training."""

            logging.info(f"Connecting to EC2 instance at {public_ip}")
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            private_key = paramiko.RSAKey.from_private_key_file(KEY_PATH)

            try:
                ssh_client.connect(
                    hostname=public_ip, username="ubuntu", pkey=private_key
                )
                command = _generate_remote_training_command()

                # Run your training command via SSH
                stdin, stdout, stderr = ssh_client.exec_command(command)

                # Stream logs in real-time
                def stream_output(channel, log_level):
                    for line in iter(lambda: channel.readline(2048), ""):
                        logging.log(log_level, line.strip())

                # Log stdout and stderr as they are generated
                stream_output(stdout, logging.INFO)
                stream_output(stderr, logging.WARNING)

                # Wait for the command to complete and get the exit status
                exit_status = stdout.channel.recv_exit_status()

                if exit_status != 0:
                    error_message = f"Command failed with exit status: {exit_status}"
                    logging.error(error_message)
                    # Raise an AirflowException to ensure the task fails properly
                    raise AirflowException(error_message)
                else:
                    logging.info("Command executed successfully.")

            except Exception as e:
                logging.error(f"Error occurred during SSH: {str(e)}")
                raise
            finally:
                # Close the SSH connection
                logging.info("Close the SSH connection")
                ssh_client.close()

        run_training = run_training_via_paramiko(ec2_public_ip)

        terminate_instance = EC2TerminateInstanceOperator(
            task_id="terminate_ec2_instance",
            region_name=region_name,
            instance_ids=create_ec2_instance.output,
            wait_for_completion=True,
            trigger_rule=TriggerRule.ALL_DONE,  # Critical: Run this task even if upstream tasks fail
        )

        (
            create_ec2_instance
            >> ec2_public_ip
            >> check_ec2_instance
            >> run_training
            >> terminate_instance
        )

"""Unit tests for the ml_training_dag.py file."""

import importlib
import unittest
from unittest.mock import MagicMock, call, patch

from airflow.models import Connection

# The module to test
from airflow.dags import ml_training_dag


class TestMlTrainingDag(unittest.TestCase):
    """
    Unit tests for the ML Training DAG.
    It mocks Airflow Variables, Connections, and external libraries like boto3 and paramiko.
    """

    def setUp(self):
        """Set up the test environment by patching Airflow Variables and Connections."""
        self.mock_variable_patcher = patch("airflow.models.Variable.get")
        self.mock_hook_patcher = patch("airflow.hooks.base.BaseHook.get_connection")

        self.mock_variable = self.mock_variable_patcher.start()
        self.mock_hook = self.mock_hook_patcher.start()

        # Mock all the Airflow variables
        self.mock_variable.side_effect = self.get_variable

        # Mock the AWS and Snowflake connections
        self.mock_hook.side_effect = self.get_connection

        # Reload the DAG module to apply patches during DAG parsing
        importlib.reload(ml_training_dag)

    def tearDown(self):
        """Stop the patchers."""
        self.mock_variable_patcher.stop()
        self.mock_hook_patcher.stop()

    def get_variable(self, key, default_var=None):
        """Mock for Variable.get()."""
        vars = {
            "JENKINS_URL": "http://fake-jenkins.com",
            "JENKINS_USER": "fake_user",
            "JENKINS_TOKEN": "fake_token",
            "JENKINS_JOB_NAME": "fake_job",
            "KEY_PAIR_NAME": "test-key",
            "KEY_PATH": "/tmp/test_key.pem",
            "AMI_ID": "ami-12345",
            "SECURITY_GROUP_ID": "sg-12345",
            "INSTANCE_TYPE": "t2.micro",
            "MLFLOW_TRACKING_URI": "http://fake-mlflow:5000",
            "MLFLOW_EXPERIMENT_ID": "1",
            "MLFLOW_LOGGED_MODEL": "projects/ml-rescue-predict/training/run.py",
            "SNOWFLAKE_USER": "sf_user",
            "SNOWFLAKE_PASSWORD": "sf_password",
            "SNOWFLAKE_ACCOUNT": "sf_account",
            "SNOWFLAKE_WAREHOUSE": "sf_warehouse",
            "SNOWFLAKE_SCHEMA": "sf_schema",
            "SNOWFLAKE_DATABASE": "sf_db",
        }
        return vars.get(key, default_var)

    def get_connection(self, conn_id):
        """Mock for BaseHook.get_connection()."""
        if conn_id == "aws_default":
            return Connection(
                conn_id="aws_default",
                conn_type="aws",
                login="test_access_key",
                password="test_secret_key",
                extra='{"region_name": "eu-west-3"}',
            )
        if conn_id == "snowflake_rescue_predict_db":
            return Connection(
                conn_id="snowflake_rescue_predict_db",
                conn_type="snowflake",
                login="sf_user",
                password="sf_password",
                host="sf_host",
                schema="sf_schema",
                extra='{"account": "sf_account", "warehouse": "sf_warehouse", "database": "sf_db", "role": "sf_role"}',
            )
        return None

    def test_dag_integrity(self):
        """Check DAG integrity, task count, and basic structure."""
        dag = ml_training_dag.dag
        self.assertIsNotNone(dag)
        self.assertEqual(dag.dag_id, "jenkins_ec2_ml_training_dag")
        self.assertEqual(len(dag.tasks), 1)  # The TaskGroup is one task
        self.assertEqual(len(dag.task_group.children), 5)  # 5 tasks inside the group

    @patch("boto3.client")
    @patch("time.sleep", return_value=None)
    def test_check_ec2_status(self, mock_sleep, mock_boto_client):
        """Test the check_ec2_status task function."""
        mock_ec2 = MagicMock()
        mock_boto_client.return_value = mock_ec2

        # Scenario: First call shows not ready, second call shows ready
        mock_ec2.describe_instance_status.side_effect = [
            {
                "InstanceStatuses": [
                    {
                        "SystemStatus": {"Status": "initializing"},
                        "InstanceStatus": {"Status": "initializing"},
                    }
                ]
            },
            {
                "InstanceStatuses": [
                    {
                        "SystemStatus": {"Status": "ok"},
                        "InstanceStatus": {"Status": "ok"},
                    }
                ]
            },
        ]

        instance_id = ["i-1234567890abcdef0"]
        result = ml_training_dag.check_ec2_status.function(instance_id)

        self.assertTrue(result)
        self.assertEqual(mock_ec2.describe_instance_status.call_count, 2)
        mock_boto_client.assert_called_with(
            "ec2",
            aws_access_key_id="test_access_key",
            aws_secret_access_key="test_secret_key",
            region_name="eu-west-3",
        )

    @patch("boto3.resource")
    def test_get_ec2_public_ip(self, mock_boto_resource):
        """Test the get_ec2_public_ip task function."""
        mock_instance = MagicMock()
        mock_instance.public_ip_address = "192.0.2.1"
        mock_instance.wait_until_running.return_value = None
        mock_instance.reload.return_value = None

        mock_ec2_resource = MagicMock()
        mock_ec2_resource.Instance.return_value = mock_instance
        mock_boto_resource.return_value = mock_ec2_resource

        instance_id = ["i-1234567890abcdef0"]
        public_ip = ml_training_dag.get_ec2_public_ip.function(instance_id)

        self.assertEqual(public_ip, "192.0.2.1")
        mock_boto_resource.assert_called_with(
            "ec2",
            aws_access_key_id="test_access_key",
            aws_secret_access_key="test_secret_key",
            region_name="eu-west-3",
        )
        mock_ec2_resource.Instance.assert_called_with(instance_id[0])
        mock_instance.wait_until_running.assert_called_once()
        mock_instance.reload.assert_called_once()

    def test_generate_remote_training_command(self):
        """Test the _generate_remote_training_command helper function."""
        # Call the private helper function directly
        command = ml_training_dag._generate_remote_training_command()

        # Assert that the command contains the expected environment variables and commands
        self.assertIn('export MLFLOW_TRACKING_URI="http://fake-mlflow:5000"', command)
        self.assertIn('export MLFLOW_EXPERIMENT_ID="1"', command)
        self.assertIn(
            'export MLFLOW_LOGGED_MODEL="projects/ml-rescue-predict/training/run.py"',
            command,
        )
        self.assertIn('export MLFLOW_EXPERIMENT_NAME="jedha-lead"', command)
        self.assertIn('export AWS_ACCESS_KEY_ID="test_access_key"', command)
        self.assertIn('export AWS_SECRET_ACCESS_KEY="test_secret_key"', command)
        self.assertIn('export SNOWFLAKE_ACCOUNT="sf_account"', command)
        self.assertIn('export SNOWFLAKE_USER="sf_user"', command)
        self.assertIn('export SNOWFLAKE_PASSWORD="sf_password"', command)
        self.assertIn('export SNOWFLAKE_WAREHOUSE="sf_warehouse"', command)
        self.assertIn('export SNOWFLAKE_SCHEMA="sf_schema"', command)
        self.assertIn('export SNOWFLAKE_DATABASE="sf_db"', command)
        self.assertIn('export SNOWFLAKE_ROLE=""', command)
        self.assertIn(
            "git clone https://github.com/littlerobinson/ml-rescue-predict.git", command
        )
        self.assertIn("cd ml-rescue-predict/training", command)
        self.assertIn("docker build -t ml-rescue-predict-training .", command)
        self.assertIn("cp run.sh.example run.sh", command)
        self.assertIn("chmod +x run.sh", command)
        self.assertIn("./run.sh", command)

    @patch("paramiko.RSAKey.from_private_key_file")
    @patch("paramiko.SSHClient")
    def test_run_training_via_paramiko_success(
        self, mock_ssh_client_class, mock_rsa_key
    ):
        """Test the run_training_via_paramiko task on success."""
        # Mock the _generate_remote_training_command to control the command string
        with patch(
            "airflow.dags.ml_training_dag._generate_remote_training_command"
        ) as mock_generate_command:
            mock_generate_command.return_value = "echo 'mocked command'"

            mock_ssh_instance = MagicMock()
            mock_ssh_client_class.return_value = mock_ssh_instance

            mock_stdin, mock_stdout, mock_stderr = MagicMock(), MagicMock(), MagicMock()
            mock_stdout.channel.recv_exit_status.return_value = 0
            mock_stdout.read.return_value = b"Training successful"
            mock_stderr.read.return_value = b""
            mock_ssh_instance.exec_command.return_value = (
                mock_stdin,
                mock_stdout,
                mock_stderr,
            )

            public_ip = "192.0.2.1"
            ml_training_dag.run_training_via_paramiko.function(public_ip)

            mock_rsa_key.assert_called_with("/tmp/test_key.pem")
            mock_ssh_instance.connect.assert_called_with(
                hostname=public_ip, username="ubuntu", pkey=mock_rsa_key.return_value
            )

            mock_generate_command.assert_called_once()
            mock_ssh_instance.exec_command.assert_called_with("echo 'mocked command'")
            mock_ssh_instance.close.assert_called_once()

    @patch("paramiko.RSAKey.from_private_key_file")
    @patch("paramiko.SSHClient")
    @patch("airflow.dags.ml_training_dag._generate_remote_training_command")
    def test_run_training_via_paramiko_failure(
        self, mock_generate_command, mock_ssh_client_class, mock_rsa_key
    ):
        """Test the run_training_via_paramiko task on failure (non-zero exit status)."""
        mock_generate_command.return_value = "echo 'mocked command'"
        mock_ssh_instance = MagicMock()
        mock_ssh_client_class.return_value = mock_ssh_instance

        mock_stdin, mock_stdout, mock_stderr = MagicMock(), MagicMock(), MagicMock()
        mock_stdout.channel.recv_exit_status.return_value = 1  # Simulate failure
        mock_stdout.read.return_value = b"Error output"
        mock_stderr.read.return_value = b"Something went wrong"
        mock_ssh_instance.exec_command.return_value = (
            mock_stdin,
            mock_stdout,
            mock_stderr,
        )

        public_ip = "192.0.2.1"
        with self.assertRaisesRegex(Exception, "Command failed with exit status: 1"):
            ml_training_dag.run_training_via_paramiko.function(public_ip)

        mock_generate_command.assert_called_once()
        mock_ssh_client_class.return_value.exec_command.assert_called_with(
            "echo 'mocked command'"
        )
        mock_ssh_client_class.return_value.close.assert_called_once()

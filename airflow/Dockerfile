FROM apache/airflow:2.10.4

# Add an argument for the user ID, with a default value if not provided
ARG AIRFLOW_UID=50000

# Switch to root to install and modify permissions
USER root

# Change the UID of airflow user to 1000 BEFORE copying anything
RUN usermod -u ${AIRFLOW_UID} airflow

# Copy the PEM key
COPY secrets/jedha-lead-certification-project.pem /opt/airflow/

# Set appropriate permissions (read-only for airflow user)
RUN chmod 400 /opt/airflow/jedha-lead-certification-project.pem \
    && chown ${AIRFLOW_UID}:0 /opt/airflow/jedha-lead-certification-project.pem

# Switch back to airflow user
USER airflow

# Install dependencies
COPY requirements.txt requirements.txt 
RUN pip install -r requirements.txt

# TODO : Create initiallization script for airflow
# COPY scripts/ /opt/airflow/scripts/
# RUN chmod +x /opt/airflow/scripts/init_airflow.sh

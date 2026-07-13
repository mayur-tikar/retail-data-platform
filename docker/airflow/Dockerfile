FROM apache/airflow:2.8.1

USER root

# Install Docker CLI (matching daemon version)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


USER airflow

# Bake all Python dependencies into the image
# WHY bake instead of pip install at startup?
#   Startup installs run every time the container starts — slow.
#   A custom image builds once, then every startup is instant.
#   This is the production pattern — never install at runtime.

RUN pip3 install --no-cache-dir \
    pyspark==3.3.0 \
    pg8000==1.30.1 \
    pyarrow==12.0.0 \
    pandas==2.0.3 \
    boto3==1.28.0 \
    apache-airflow-providers-amazon==8.0.0
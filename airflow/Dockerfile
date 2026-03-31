FROM apache/airflow:2.7.3-python3.9

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Проверка версий
RUN python -c "import sqlalchemy; print(f'SQLAlchemy version: {sqlalchemy.__version__}')"
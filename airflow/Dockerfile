FROM eclipse-temurin:17-jre-focal AS java-layer

FROM apache/airflow:2.4.1-python3.10

USER root
# 1. Copy self-contained OpenJDK 17 runtime from official Eclipse Temurin image
# (Bypasses Debian apt-get 404 mirror errors for stale libnss3 packages)
COPY --from=java-layer /opt/java/openjdk /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV JDK_JAVA_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

# 2. Install procps (required for PySpark worker process control)
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# 3. Install PySpark matching our data transformations, DuckDB, and AWS SDK
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    duckdb>=0.9.0 \
    boto3>=1.26.0 \
    pandas>=1.5.0 \
    pyarrow>=10.0.0 \
    fastparquet>=2023.1.0 \
    python-dotenv>=1.0.0

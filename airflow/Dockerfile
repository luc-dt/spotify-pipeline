FROM eclipse-temurin:17-jre-focal@sha256:d7ec0d23cc1675eb16a266af1d4f5d41db60260af3fcd81c42de2e6ed3dc4dba AS java-layer

FROM apache/airflow:2.4.1-python3.10@sha256:3e7251846d5d0f3374c2573fca00b81e181c4f4fc29456a50e40a1a84a5bef1f

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
# Versions pinned to exact releases to satisfy SonarCloud docker:S8544
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    duckdb==1.2.2 \
    boto3==1.38.34 \
    pandas==2.2.3 \
    pyarrow==19.0.1 \
    fastparquet==2024.11.0 \
    python-dotenv==1.1.0

# ==============================================================================
# Spotify Music Intelligence Platform — Streamlit Analytics Container
# ==============================================================================
# Pinned to SHA256 digest to satisfy SonarCloud docker:S8544 (unpinned base image)
FROM python:3.10-slim@sha256:fd76ade0c607f27677bc04be3c60749f400eedc941d9e72967e19a4cedff80c2

# Set working directory inside the container
WORKDIR /app

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install lightweight system dependencies (curl for container healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Layer caching: copy pinned runtime deps first so code changes do not invalidate pip cache
COPY requirements-app.txt .
RUN pip install --no-cache-dir --only-binary :all: -r requirements-app.txt

# Copy application source code, SQL mart definitions, and Gold Parquet layer
COPY sql/ ./sql/
COPY streamlit/ ./streamlit/
COPY .streamlit/ ./.streamlit/
COPY data/gold/ ./data/gold/

# Create a dedicated non-root application user for container security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose default Streamlit port
EXPOSE 8501

# Healthcheck to verify Streamlit server responds
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Launch the Streamlit application
ENTRYPOINT ["streamlit", "run", "streamlit/app.py", "--server.port=8501", "--server.address=0.0.0.0"]

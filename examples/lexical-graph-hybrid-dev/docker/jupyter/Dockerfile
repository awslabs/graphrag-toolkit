FROM jupyter/scipy-notebook:python-3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER jovyan

# Upgrade pip and fix setuptools/backports.tarfile compatibility
RUN pip install --no-cache-dir --upgrade pip && \
    pip uninstall -y backports.tarfile backports || true && \
    pip install --no-cache-dir --upgrade setuptools==68.0.0 wheel && \
    pip install --no-cache-dir backports.tarfile==1.0.0

# Optional: upgrade JupyterLab and install kernel gateway
RUN pip install --no-cache-dir --upgrade jupyterlab jupyter_kernel_gateway

# Install DB Support & Analytics
RUN pip install --no-cache-dir \
    psycopg2-binary \
    pgvector \
    neo4j \
    matplotlib \
    plotly

# Install core packages
RUN pip install --no-cache-dir \
    nest_asyncio \
    python-dotenv

# Install ML packages
RUN pip install --no-cache-dir \
    transformers \
    torch \
    sentence_transformers

# Install LlamaIndex readers
RUN pip install --no-cache-dir \
    llama-index-readers-web \
    llama-index-readers-file \
    llama-index-readers-github \
    llama-index-readers-json \
    llama-index-readers-structured-data \
    llama-index-readers-s3 \
    pymupdf \
    youtube-transcript-api \
    wikipedia

# Download NLTK data
RUN python -c "import nltk; nltk.download('punkt', quiet=True); nltk.download('stopwords', quiet=True)"

# Create directories
RUN mkdir -p /home/jovyan/notebooks /home/jovyan/data

WORKDIR /home/jovyan

EXPOSE 8888

# Start Jupyter Lab without authentication
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''", "--NotebookApp.disable_check_xsrf=True"]
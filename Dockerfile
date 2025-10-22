FROM python:3.12-slim

WORKDIR /app

# Copy requirements
COPY pyproject.toml poetry.lock* ./

# Install dependencies
RUN pip install --no-cache-dir requests

# Copy the script
COPY t2wee.py .

# Run the script
ENTRYPOINT ["python", "t2wee.py"]


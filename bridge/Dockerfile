FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for Docker caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY main.py .

# Run the bridge
CMD ["python", "-u", "main.py"] 
# (The -u flag forces Python to print logs immediately instead of buffering them)

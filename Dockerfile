# Base image
FROM python:3.9

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .

# Expose HTTP server port
EXPOSE 5000

# Run the script
CMD ["python", "app.py"]


FROM python:3.10-slim

WORKDIR /app

# Copy the producer script
COPY producers/simulate_logs.py /app/

# The script expects a logs directory to exist in the CWD
RUN mkdir -p /app/logs

# Run the producer
CMD ["python", "simulate_logs.py"]

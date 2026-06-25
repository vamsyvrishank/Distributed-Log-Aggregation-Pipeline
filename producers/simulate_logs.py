import json
import time
import random
import uuid
from datetime import datetime, timezone

# Define our fake microservices and event scenarios
services = ["auth-service", "payment-service", "shipping-service", "user-profile-service"]
log_levels = ["INFO", "DEBUG", "ERROR", "WARNING"]
messages = [
    "User logged in successfully",
    "Database connection timeout",
    "Item added to cart",
    "Payment processed"
]

def generate_log_event():
    """
    Generates a single structured JSON log event representing a microservice action.
    Includes distributed tracing identifiers (trace_id) and timestamps.
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    service = random.choice(services)
    level = random.choice(log_levels)
    trace_id = uuid.uuid4()
    message = random.choice(messages)
    latency_ms = random.randint(10, 500)

    log_event = {
        "timestamp": timestamp,
        "service": service,
        "level": level,
        "trace_id": str(trace_id),
        "message": message,
        "latency_ms": latency_ms
    }

    return log_event

def main():
    # Write to a shared file inside the Docker container
    log_file_path = "logs/app.log"
    
    print(f"Starting log generation... Streaming to {log_file_path}")
    
    # Open the log file in append mode ("a") to stream logs continuously
    with open(log_file_path, "a") as file:
        # Infinite loop to simulate continuous live traffic
        while True:
            event_dict = generate_log_event()
            event_json = json.dumps(event_dict)
            
            # Write to the file and immediately flush to disk
            # so Fluent Bit can tail it instantly.
            file.write(event_json + "\n")
            file.flush()
            
            # Pause randomly between 0.1 and 1.0 seconds to simulate real user traffic
            time.sleep(random.uniform(0.1, 1.0))

if __name__ == "__main__":
    main()
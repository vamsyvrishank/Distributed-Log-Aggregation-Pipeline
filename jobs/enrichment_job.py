import json
from elasticsearch import Elasticsearch
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer

def extract_real_dict(event_string):
    """
    Helper function to parse the incoming JSON string.
    Fluent Bit's 'tail' plugin wraps raw file lines in a {"log": "..."} object.
    This safely unwraps the inner log object if it exists.
    """
    event_dict = json.loads(event_string)
    if 'log' in event_dict:
        try:
            return json.loads(event_dict['log'])
        except Exception:
            pass
    return event_dict

def is_not_debug(event_string):
    """
    Flink Filter Function:
    Returns False if the log level is DEBUG (dropping it from the stream),
    True otherwise (keeping it in the stream).
    """
    try:
        real_dict = extract_real_dict(event_string)
        if real_dict.get('level') == 'DEBUG':
            return False
        return True
    except Exception:
        # If parsing fails, keep the log rather than dropping it
        return True

def enrich_log(event_string):
    """
    Flink Map Function:
    Takes a JSON string, extracts the raw log, injects an 'enriched' flag, 
    and returns the updated JSON string.
    """
    try:
        real_dict = extract_real_dict(event_string)
        real_dict['enriched'] = True
        return json.dumps(real_dict)
    except Exception as e:
        return event_string

def send_to_elasticsearch(event_string):
    """
    Flink Map Function (acting as a Sink):
    Pushes the final, enriched JSON string to Elasticsearch.
    """
    try:
        # Connect to our local Elasticsearch container via the Docker network
        es = Elasticsearch("http://elasticsearch:9200")
        doc = json.loads(event_string)
        
        # Write to the index that Kibana will visualize
        es.index(index="flink-enriched-logs", document=doc)
    except Exception as e:
        print(f"Failed to send to ES: {e}")
    return event_string

def main():
    print("Starting Flink Enrichment Job...")
    
    # 1. Set up the PyFlink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 2. Add the required Java Connectors so Flink can talk to Kafka
    env.add_jars(
        "file:///opt/flink/jars/flink-sql-connector-kafka.jar",
        "file:///opt/flink/jars/flink-sql-connector-elasticsearch7.jar"
    )
    
    # 3. Create a Kafka Consumer to read from the 'raw-events' topic
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-enrichment-group'
    }
    kafka_source = FlinkKafkaConsumer(
        topics='raw-events',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    # 4. Attach the source to the environment
    stream = env.add_source(kafka_source)
    
    # 5. Define the processing pipeline (The Conveyor Belt)
    # Filter out DEBUG logs -> Add 'enriched' flag -> Send to Elasticsearch
    stream.filter(is_not_debug) \
          .map(enrich_log) \
          .map(send_to_elasticsearch)
    
    # 6. Execute the streaming job
    print("Executing Flink Job...")
    env.execute("Distributed Log Enrichment Pipeline")

if __name__ == '__main__':
    main()

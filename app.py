import pika
import json
import psycopg2
import time
from datetime import datetime
import os

# Read RabbitMQ and PostgreSQL configurations from environment variables
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "json_queue")  # Default queue name

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Function to transform JSON to SOAP
def json_to_soap(json_data):
    soap_template = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Header/>
    <soapenv:Body>
        <event>
            <name>{event}</name>
            <user_id>{user_id}</user_id>
            <timestamp>{timestamp}</timestamp>
        </event>
    </soapenv:Body>
</soapenv:Envelope>"""
    return soap_template.format(
        event=json_data["event"],
        user_id=json_data["user_id"],
        timestamp=json_data["timestamp"]
    )

# Function to process messages and insert into PostgreSQL
def process_message(ch, method, properties, body):
    start_time = time.time()
    status = "Completed"
    error_message = None

    try:
        # Decode JSON message
        message_json = json.loads(body.decode("utf-8"))

        # Transform JSON to SOAP
        message_soap = json_to_soap(message_json)

        # Insert into PostgreSQL
        with psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO messages (message_json, message_soap, processing_duration_ms, status, error_message)
                    VALUES (%s, %s, %s, %s, %s);
                """, (
                    json.dumps(message_json),  # message_json
                    message_soap,              # message_soap
                    int((time.time() - start_time) * 1000),  # processing_duration_ms
                    status,                    # status
                    error_message              # error_message
                ))
        conn.commit()

    except Exception as e:
        # Handle errors and update status
        status = "Failed"
        error_message = str(e)
        print(f"Error processing message: {error_message}")

    # Acknowledge message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Main function to consume messages
def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=process_message)

    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()

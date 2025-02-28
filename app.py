import pika
import json
import psycopg2
import time
import math
import os
from datetime import datetime, timedelta
from flask import Flask, jsonify

# RabbitMQ and PostgreSQL configurations
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "json_queue")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "your_db_name")
POSTGRES_USER = os.getenv("POSTGRES_USER", "your_db_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "your_db_password")

SOAP_MESSAGE_DIR = os.getenv("SOAP_MESSAGE_DIR", "/mnt/soap_messages/")  # Directory to store SOAP messages

# Ensure the directory exists
os.makedirs(SOAP_MESSAGE_DIR, exist_ok=True)

# Initialize Flask
app = Flask(__name__)

# Simulate CPU Load Function
def simulate_cpu_load():
    """Artificially increase CPU usage to trigger HPA"""
    print("⚡ Simulating CPU Load...")
    num_operations = 10**6  # Increase this value if needed
    _ = [math.sqrt(i) for i in range(num_operations)]  # CPU-intensive operation
    
# Function to transform JSON to SOAP format
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
        event=json_data.get("event", "unknown"),
        user_id=json_data.get("user_id", "unknown"),
        timestamp=json_data.get("timestamp", datetime.utcnow().isoformat())
    )

# Function to process messages from RabbitMQ and insert into PostgreSQL
def process_message(ch, method, properties, body):
    start_time = time.time()  # Start timing when message is received
    status = "Completed"
    error_message = None

    try:
        # Decode JSON message
        message_json = json.loads(body.decode("utf-8"))

        # Simulate CPU load
        simulate_cpu_load()

        # Convert JSON to SOAP
        message_soap = json_to_soap(message_json)

        # Generate a meaningful filename
        timestamp_str = (datetime.now()+timedelta(hours=1)).strftime("%Y%m%d%H%M%S")
        user_id = message_json.get("user_id", "unknown")
        soap_filename = f"soap_message_{timestamp_str}_{user_id}.xml"
        soap_filepath = os.path.join(SOAP_MESSAGE_DIR, soap_filename)

        # Save SOAP message to Persistent Volume
        with open(soap_filepath, "w") as soap_file:
            soap_file.write(message_soap)
        print(f"📄 SOAP message saved to {soap_filepath}")

        # Insert into PostgreSQL
        with psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        ) as conn:
            with conn.cursor() as cursor:
                processing_duration_ms = int((time.time() - start_time) * 1000)  # End timing after insertion
                cursor.execute("""
                    INSERT INTO messages (message_json, message_soap, processing_duration_ms, status, error_message, creation_datetime)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                """, (
                    json.dumps(message_json),  # JSON data
                    message_soap,              # SOAP message
                    processing_duration_ms,    # Processing duration in ms
                    status,                    # Status
                    error_message              # Error message (None if no error)
                ))
                conn.commit()

    except Exception as e:
        # Handle errors and update status
        status = "Failed"
        error_message = str(e)
        print(f"❌ Error processing message: {error_message}")

        # Insert failed message into PostgreSQL
        with psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO messages (message_json, message_soap, processing_duration_ms, status, error_message, creation_datetime)
                    VALUES (%s, NULL, NULL, %s, %s, CURRENT_TIMESTAMP);
                """, (
                    json.dumps(message_json),  # JSON data
                    status,                    # Status
                    error_message              # Error message
                ))
                conn.commit()

    # Acknowledge message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Function to start RabbitMQ consumer
def start_rabbitmq_consumer():
    try:
        print("🔄 Connecting to RabbitMQ...")
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
        )
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=process_message)

        print("✅ RabbitMQ Consumer is Running")
        channel.start_consuming()

    except Exception as e:
        print(f"❌ Error connecting to RabbitMQ: {e}")

# Function to fetch data from PostgreSQL
@app.route("/data", methods=["GET"])
def get_data():
    try:
        with psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM messages ORDER BY creation_datetime DESC LIMIT 10;")
                rows = cursor.fetchall()

                messages = [
                    {
                        "id": row[0],
                        "message_json": row[1],
                        "message_soap": row[2],
                        "creation_datetime": row[3].isoformat(),
                        "processing_duration_ms": row[4],
                        "status": row[5],
                        "error_message": row[6]
                    }
                    for row in rows
                ]

        return jsonify(messages), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Main function to start everything
if __name__ == "__main__":
    from threading import Thread

    # Start RabbitMQ consumer in a background thread
    rabbitmq_thread = Thread(target=start_rabbitmq_consumer, daemon=True)
    rabbitmq_thread.start()

    # Start Flask server
    print("🚀 Starting Flask server on port 5000")
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)


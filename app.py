import pika
import json
import psycopg2
import time
import os
from datetime import datetime
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

# Initialize Flask
app = Flask(__name__)

# Function to process messages from RabbitMQ and insert into PostgreSQL
def process_message(channel, method, properties, body):
    try:
        message_json = json.loads(body)
        print(f"📥 Received message: {message_json}")

        with psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO messages (message_json, creation_datetime) VALUES (%s, %s)",
                    (json.dumps(message_json), datetime.now()),
                )
                conn.commit()
                print("✅ Message saved to PostgreSQL")

        # Acknowledge the message
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"❌ Error processing message: {e}")

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
                        "creation_datetime": row[2].isoformat(),
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

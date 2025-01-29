import pika
import json
import psycopg2
import time
from datetime import datetime
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

# RabbitMQ and PostgreSQL configurations from environment variables
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "json_queue")  # Default queue name

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Function to transform JSON to SOAP (Placeholder - Implement as needed)
def json_to_soap(json_data):
    return f"<SOAP>{json.dumps(json_data)}</SOAP>"

# Function to process messages and insert into PostgreSQL
def process_message(ch, method, properties, body):
    try:
        # Acknowledge message immediately to prevent requeueing
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"Received message: {body.decode()}")  # Debugging log

        message_json = json.loads(body)
        message_soap = json_to_soap(message_json)

        with psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO messages (message_json, message_soap, creation_datetime) VALUES (%s, %s, %s)",
                    (json.dumps(message_json), message_soap, datetime.now()),
                )
                conn.commit()

        print("Message successfully saved to PostgreSQL")

    except Exception as e:
        print(f"Error processing message: {e}")

# HTTP server handler to expose the /data endpoint
class MessageHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/data":
            try:
                with psycopg2.connect(
                    host=POSTGRES_HOST,
                    database=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                ) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT * FROM messages;")
                        rows = cursor.fetchall()

                        messages = [
                            {
                                "id": row[0],
                                "message_json": row[1],
                                "message_soap": row[2],
                                "creation_datetime": row[3].isoformat(),
                                "status": row[4] if len(row) > 4 else None,
                                "error_message": row[5] if len(row) > 5 else None,
                            }
                            for row in rows
                        ]

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(messages).encode("utf-8"))

            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

# Start RabbitMQ consumer
def start_rabbitmq_consumer():
    try:
        print("Connecting to RabbitMQ...")
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.Connection

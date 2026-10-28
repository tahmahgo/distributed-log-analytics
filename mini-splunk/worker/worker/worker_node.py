import logging
import json
import uuid
import pika
import re
from datetime import datetime
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use service name (Docker DNS will resolve the IP)
CASSANDRA_HOST = 'splunk-cassandra'

# Connect to Cassandra
try:
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS splunk_data
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace('splunk_data')
    session.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id UUID PRIMARY KEY,
            timestamp TEXT,
            hostname TEXT,
            process TEXT,
            severity TEXT,
            message TEXT,
            ingested_at TIMESTAMP
        )
    """)
    session.execute("CREATE INDEX IF NOT EXISTS idx_hostname ON logs (hostname)")
    session.execute("CREATE INDEX IF NOT EXISTS idx_process ON logs (process)")
    session.execute("CREATE INDEX IF NOT EXISTS idx_severity ON logs (severity)")
    logger.info(f"✅ Connected to Cassandra at {CASSANDRA_HOST}")
except Exception as e:
    logger.error(f"❌ Cassandra error: {e}")
    raise

def parse_syslog(line):
    pattern = r'^(?P<timestamp>\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+(?P<hostname>[\w\-\.]+)\s+(?P<process>[\w\-\.]+)(?:\[(?P<pid>\d+)\])?:\s+(?P<message>.*)$'
    match = re.match(pattern, line.strip())
    if match:
        parts = match.groupdict()
        timestamp = parts['timestamp']
        hostname = parts['hostname']
        process = parts['process']
        message = parts['message']
        severity = 'info'
        if 'error' in message.lower():
            severity = 'error'
        elif 'warning' in message.lower():
            severity = 'warning'
        elif 'debug' in message.lower():
            severity = 'debug'
        return {
            'id': uuid.uuid4(),
            'timestamp': timestamp,
            'hostname': hostname,
            'process': process,
            'severity': severity,
            'message': message,
            'ingested_at': datetime.now()
        }
    return None

def callback(ch, method, properties, body):
    try:
        raw_message = body.decode('utf-8')
        logger.info(f"📥 Received: {raw_message[:100]}...")
        data = json.loads(raw_message)
        if isinstance(data, dict) and 'message' in data:
            raw_line = data['message']
            parsed = parse_syslog(raw_line)
            if parsed:
                session.execute(
                    "INSERT INTO logs (id, timestamp, hostname, process, severity, message, ingested_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (parsed['id'], parsed['timestamp'], parsed['hostname'], parsed['process'], parsed['severity'], parsed['message'], parsed['ingested_at'])
                )
                logger.info(f"✅ Stored: {parsed['hostname']} - {parsed['message'][:30]}...")
            else:
                logger.warning(f"⚠️ Failed to parse: {raw_line}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

if __name__ == '__main__':
    logger.info("🚀 Worker started")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='splunk-rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='ingest_queue', durable=True)
    channel.basic_consume(queue='ingest_queue', on_message_callback=callback)
    logger.info("👂 Listening for logs...")
    channel.start_consuming()

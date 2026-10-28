import logging
import json
import pika
from aiohttp import web

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_channel():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='splunk-rabbitmq'))
        channel = connection.channel()
        channel.queue_declare(queue='ingest_queue', durable=True)
        logger.info("✅ Connected to RabbitMQ")
        return channel
    except Exception as e:
        logger.error(f"❌ RabbitMQ error: {e}")
        return None

# Initialize channel
channel = get_channel()

async def health_handler(request):
    return web.json_response({'status': 'healthy', 'service': 'api-gateway'})

async def ingest_handler(request):
    global channel
    try:
        # Check if channel is closed and reconnect
        if not channel or channel.is_closed:
            logger.warning("⚠️ Channel is closed, reconnecting...")
            channel = get_channel()
            if not channel:
                raise Exception("Could not connect to RabbitMQ")

        data = await request.json()
        content = data.get('content', '')
        lines = [line for line in content.strip().split('\n') if line.strip()]

        for line in lines:
            message = json.dumps({'message': line})
            channel.basic_publish(
                exchange='',
                routing_key='ingest_queue',
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )

        logger.info(f"✅ Published {len(lines)} logs to RabbitMQ")
        return web.json_response({'status': 'success', 'ingested': len(lines)})
    except Exception as e:
        logger.error(f"❌ Ingest error: {e}")
        return web.json_response({'status': 'error', 'message': str(e)}, status=500)

async def query_handler(request):
    from cassandra.cluster import Cluster
    try:
        cluster = Cluster(['splunk-cassandra'])
        session = cluster.connect('splunk_data')

        query_type = request.query.get('type', '')
        value = request.query.get('value', '')
        rows = session.execute("SELECT * FROM logs")

        results = []
        for row in rows:
            match = False
            if query_type == 'SEARCH_HOST' and row.hostname == value:
                match = True
            elif query_type == 'SEARCH_DAEMON' and row.process == value:
                match = True
            elif query_type == 'SEARCH_SEVERITY' and row.severity == value:
                match = True
            elif query_type == 'SEARCH_KEYWORD' and value.lower() in row.message.lower():
                match = True
            elif query_type == 'SEARCH_DATE' and value in row.timestamp:
                match = True
            elif query_type == 'COUNT_KEYWORD' and value.lower() in row.message.lower():
                match = True

            if match:
                results.append({
                    'timestamp': row.timestamp,
                    'hostname': row.hostname,
                    'process': row.process,
                    'severity': row.severity,
                    'message': row.message
                })

        if query_type == 'COUNT_KEYWORD':
            return web.json_response({'count': len(results)})
        return web.json_response({'total': len(results), 'entries': results})
    except Exception as e:
        logger.error(f"❌ Query error: {e}")
        return web.json_response({'status': 'error', 'message': str(e)}, status=500)

async def purge_handler(request):
    from cassandra.cluster import Cluster
    try:
        cluster = Cluster(['splunk-cassandra'])
        session = cluster.connect('splunk_data')
        session.execute("TRUNCATE splunk_data.logs")
        logger.info("✅ All logs purged")
        return web.json_response({'status': 'success', 'message': 'All logs purged'})
    except Exception as e:
        logger.error(f"❌ Purge error: {e}")
        return web.json_response({'status': 'error', 'message': str(e)}, status=500)

app = web.Application()
app.router.add_get('/health', health_handler)
app.router.add_post('/ingest', ingest_handler)
app.router.add_get('/query', query_handler)
app.router.add_delete('/purge', purge_handler)

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=8080)

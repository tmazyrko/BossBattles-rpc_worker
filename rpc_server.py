#!/usr/bin/env python3
import json
import pika
import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values
from datetime import date, datetime

# Connect to MySQL database using config provided by .env file
cfg = dotenv_values(".env")
try:
    sqlconn = mysql.connector.connect(user=cfg['MYSQL_USER'], password=cfg['MYSQL_PASS'],
                                      host=cfg['MYSQL_IP'],
                                      database=cfg['DB_NAME'])
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)


def query_database(query, one=False):
    """Queries database and returns the result in a format that can be dumped to JSON"""
    cursor = sqlconn.cursor()
    cursor.execute(query)

    r = [dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in cursor.fetchall()]
    return (r[0] if r else None) if one else r


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


# Establish AMQP connection to RabbitMQ broker
connection = pika.BlockingConnection(pika.URLParameters('amqp://test:test@10.10.5.32/%2F'))
channel = connection.channel()
channel.queue_declare(queue='sqlQueue')


def on_request(ch, method, props, body):
    """Function executes upon consuming an RPC call via RabbitMQ"""
    query = body.decode("utf-8")

    print(" [.] Received query: %s" % query)
    my_query = query_database(query)
    json_output = json.dumps(my_query, default=json_serial)  # Dumps db query result to a JSON string

    # Send RPC reply message back to the server
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=str(json_output))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='sqlQueue', on_message_callback=on_request)  # Defines RabbitMQ queue to listen on

print(" [x] Awaiting RPC requests")
channel.start_consuming()

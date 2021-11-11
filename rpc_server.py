#!/usr/bin/env python3
import pika
import json
import ast
import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values
from log import Log
import signal
import sys


def signal_handler(signal, frame):  # Graceful CTRL+C handler
    logger.log("Exiting rpc_server.py", "WARN")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

cfg = dotenv_values(".env")
logger = Log(cfg['AMQP_URL'], "INFO")

# Sets up MySQL Connection
try:
    sqlconn = mysql.connector.connect(user=cfg['MYSQL_USER'], password=cfg['MYSQL_PASS'],
                                      host=cfg['MYSQL_IP'],
                                      database=cfg['DB_NAME'])
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        logger.error("MySQL Connector: Wrong username or password.")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        logger.error("MySQL Connector: Database does not exist.")
    else:
        logger.error("MySQL Connector: " + str(err))

# Sets up RabbitMQ connection
connection = pika.BlockingConnection(pika.URLParameters(cfg['AMQP_URL']))
channel = connection.channel()
channel.queue_declare(queue='sqlQueue')


def on_request(ch, method, props, body):  # Executes upon consuming an AMQP message
    query = body.decode("utf-8")

    logger.info("Received query %s" % query)
    response = query_database(query)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def query_database(query):  # Queries the database and returns a result
    cursor = sqlconn.cursor(dictionary=True)
    result_list = []
    try:
        cursor.execute(query)
        for row in cursor:
            result_list.append(str(row))
        sqlconn.commit()
    except mysql.connector.Error as e:
        try:
            logger.error("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
            return str([])
        except IndexError:
            logger.error("MySQL Error: %s" % str(e))
            return str([])
    except TypeError as e:
        logger.error("TypeError: %s" % str(e))
        return str([])
    except ValueError as e:
        logger.error("ValueError: %s" % str(e))
        return str([])

    try:
        result = ''.join(result_list)
        result = json.dumps(ast.literal_eval(result))
        return result
    except SyntaxError as e:
        logger.warn("SyntaxError: %s. This is most likely not a problem if it happens after an INSERT query." % str(e))
        return str([])


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='sqlQueue', on_message_callback=on_request)

logger.log("Awaiting RPC requests.")
channel.start_consuming()

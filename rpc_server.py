#!/usr/bin/env python3
import pika
import json
import ast
import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values

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


def query_database(query):
    cursor = sqlconn.cursor(dictionary=True)
    result_list = []
    try:
        cursor.execute(query)
        for row in cursor:
            result_list.append(str(row))
        #result_list = str(cursor.fetchall())
        #print(str(result_list))
        sqlconn.commit()
    except mysql.connector.Error as e:
        try:
            result = " [!] MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            print(result)
            return result
        except IndexError:
            result = " [!] MySQL Error: %s" % str(e)
            print(result)
            return result
    except TypeError as e:
        result = " [!] TypeError: %s" % str(e)
        print(result)
        return result
    except ValueError as e:
        result = " [!] ValueError: %s" % str(e)
        print(result)
        return result

    try:
        result = ''.join(result_list)
        #print(result)
        result = json.dumps(ast.literal_eval(result))
        #print(result)
        return result
    except SyntaxError as e:
        error_msg = " [!] SyntaxError: %s. This is most likely not a problem, especially if it happens after an INSERT query." % str(e)
        print(error_msg)
        return str([])


connection = pika.BlockingConnection(pika.URLParameters('amqp://test:test@10.10.5.32/%2F'))

channel = connection.channel()

channel.queue_declare(queue='sqlQueue')


def on_request(ch, method, props, body):
    query = body.decode("utf-8")

    print(" [.] Received query: %s" % query)
    response = query_database(query)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='sqlQueue', on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()

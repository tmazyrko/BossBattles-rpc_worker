import pika
import datetime
import pytz
import socket


class Log:

    def __init__(self, amqp_url, log_level="INFO", iana_timezone='America/New_York'):  # Default timezone: US East
        self.connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='logs', exchange_type="fanout", durable=True)

        self.log_levels = {"DEBUG": 1, "INFO": 2, "WARN": 3, "ERROR": 4, "FATAL": 5}
        self.level = self.log_levels[log_level]
        self.tz = iana_timezone

    def log(self, text, level="INFO"):  # This will always create a log, even if logging level is set higher than "INFO"
        timestamp = self.get_timestamp()
        hostname = self.get_hostname()
        msg = f'{timestamp} | {level} | {hostname} | {text}'
        print(msg)
        self.channel.basic_publish(exchange="logs", routing_key='', body=msg)
        return True

    def debug(self, text):
        if self.level <= 1:
            self.log(text, "DEBUG")
            return True
        else:
            return False

    def info(self, text):
        if self.level <= 2:
            self.log(text, "INFO")
            return True
        else:
            return False

    def warn(self, text):
        if self.level <= 3:
            self.log(text, "WARN")
            return True
        else:
            return False

    def error(self, text):
        if self.level <= 4:
            self.log(text, "ERROR")
            return True
        else:
            return False

    def fatal(self, text):
        if self.level <= 5:
            self.log(text, "FATAL")
            return True
        else:
            return False

    def get_timestamp(self):  # Use IANA timezone string
        timestamp = datetime.datetime.now(pytz.timezone(self.tz))
        formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S (UTC%z)")
        return formatted_timestamp

    def get_hostname(self):
        local_hostname = socket.gethostbyaddr(socket.gethostname())[0]
        return local_hostname

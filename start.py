#!/usr/bin/python
import pymysql.cursors
import sys
import time
import http.client
import json
import datetime
import paho.mqtt.client as mqtt
from threading import Thread


class HttpRequest(Thread):
    def __init__(self, url=None, topic=None, payload=None):
        Thread.__init__(self)
        self.host = str(url).split('/', 1)[0]
        self.endpoint = str(url).split('/', 1)[1]
        self.topic = topic
        self.payload = payload

    def run(self):
        connection = http.client.HTTPSConnection(self.host)
        connection.request('POST', '/' + self.endpoint, json.dumps({
            'timestamp': str(datetime.datetime.now()),
            'topic': self.topic,
            'payload': self.payload
        }), {'Content-type': 'application/json'})
        response = connection.getresponse()
        print(self.topic + " - " + str(response.getcode()))


class Process(Thread):
    def __init__(self, sid=None, host=None, username=None, password=None, topic=None, topic_qos=None,
                 destination_type=None, destination=None):
        Thread.__init__(self)
        self.sid = sid
        self.host = host
        self.username = username
        self.password = password
        self.topic = topic
        self.topic_qos = topic_qos
        self.destination_type = destination_type
        self.destination = destination
        self.client = None

    def on_connect(self, client, obj, flags, rc):
        self.client.subscribe(self.topic, qos=self.topic_qos)

    def on_message(self, client, obj, msg):
        try:
            if self.destination_type == 'http':
                request = HttpRequest(self.destination, msg.topic, str(msg.payload)[2:][:-1])
                request.start()
            elif self.destination_type == 'beanstalk':
                print("todo")
        except:
            pass

    def run(self):
        self.client = mqtt.Client(str(self.sid) + "_subscriber")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.username_pw_set(self.username, self.password)
        self.client.connect(self.host, 1883, 60)
        self.client.loop_forever()


def main(argv):
    db_connection = pymysql.connect(host=argv[0],
                                    user=argv[1],
                                    password=argv[2],
                                    db=argv[3],
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)
    processes = []
    try:
        with db_connection.cursor() as cursor:
            cursor.execute("SELECT `id`,`topic`,`topic_qos`,`target_type`,`target` FROM mqtt_subscriptions;")
            result = cursor.fetchall()
            for subscription in result:
                process = Process(subscription['id'], argv[4], argv[5], argv[6], subscription['topic'],
                                  subscription['topic_qos'], subscription['target_type'],
                                  subscription['target'])
                process.start()
                processes.append(process)
    finally:
        db_connection.close()
        while True:
            # print("check for imports")
            time.sleep(4)


if __name__ == "__main__":
    main(sys.argv[1:])

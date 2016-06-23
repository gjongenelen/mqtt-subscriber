#!/usr/bin/python
import pymysql.cursors
import sys
import time
import http.client
import json
import datetime
import urllib.parse
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
        print("["+str(time.strftime("%d/%m %H:%M:%S"))+"] " + str(response.getcode()) + " - " + self.topic + " -> " +self.host+"/"+self.endpoint)


class Process(Thread):
    def __init__(self, sid=None, host=None, username=None, password=None, topic=None, topic_qos=None,
                 destination_type=None, destination=None, auth_type=None, auth_server=None, auth_endpoint=None):
        Thread.__init__(self)
        self.sid = sid
        self.host = host
        self.username = username
        self.password = password
        self.topic = topic
        self.topic_qos = topic_qos
        self.destination_type = destination_type
        self.destination = destination
        self.auth_type = auth_type
        self.auth_server = auth_server
        self.auth_endpoint = auth_endpoint
        self.client = None

    def get_jwt(self, auth_server, username, password):
        connection = http.client.HTTPSConnection(self.auth_server)
        connection.request('POST', "/" + self.auth_endpoint, urllib.parse.urlencode({'email': self.username, 'password': self.password }), {"Content-type": "application/x-www-form-urlencoded" })
        response = connection.getresponse()
        data = json.loads(response.read().decode())
        connection.close()
        return data['token']

    def on_connect(self, client, obj, flags, rc):
        self.client.subscribe(self.topic, qos=self.topic_qos)

    def on_disconnect(self, client, userdata, rc):
        print(rc)
        time.sleep(5)
        if rc != 0:
            if self.auth_type == "jwt":
                self.set_auth()
            self.connect()

    def set_auth(self):
        if self.auth_type == "jwt":
            self.client.username_pw_set(self.get_jwt(self.auth_server, self.username, self.password), "token")
        else:
            self.client.username_pw_set(self.username, self.password)

    def on_message(self, client, obj, msg):
        try:
            if self.destination_type == 'http':
                request = HttpRequest(self.destination, msg.topic, str(msg.payload)[2:][:-1])
                request.start()
            elif self.destination_type == 'beanstalk':
                print("todo")
        except:
            pass

    def connect(self):
        self.client.connect(self.host, 1883, 60)
        self.client.loop_start()

    def run(self):
        self.client = mqtt.Client(str(self.sid) + "_subscriber")
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.set_auth()
        self.connect()


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
                                  subscription['target'],  argv[7], argv[8], argv[9])
                process.start()
                processes.append(process)
    finally:
        db_connection.close()
        while True:
            # print("check for imports")
            time.sleep(4)


if __name__ == "__main__":
    main(sys.argv[1:])

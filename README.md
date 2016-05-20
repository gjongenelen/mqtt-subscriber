#MQTT-Subscriber

## Installation

    CREATE TABLE `mqtt_subscriptions` (
      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
      `topic` varchar(200) NOT NULL DEFAULT '',
      `topic_qos` int(1) NOT NULL DEFAULT '0',
      `target_type` enum('beanstalk','http') DEFAULT NULL,
      `target` varchar(200) DEFAULT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=latin1;


## Running

    docker run -e MYSQL_HOST='MYSQL_HOST' \
               -e MYSQL_USERNAME='MYSQL_USERNAME' \
               -e MYSQL_PASSWORD='MYSQL_PASSWORD' \
               -e MYSQL_DATABASE='MYSQL_DATABASE' \
               -e MQTT_HOST='MQTT_HOST' \
               -e MQTT_USERNAME='MQTT_USERNAME' \
               -e MQTT_PASSWORD='MQTT_PASSWORD' \
               pts/mqtt


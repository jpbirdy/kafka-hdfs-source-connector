#!/usr/bin/env bash
mvn clean && mvn install -DskipTests -Dfile.encoding=UTF-8
cp target/hdfs-kafka-connector-3.2.1.jar /Users/jpbirdy/Downloads/confluent-3.2.1/share/java/kafka-connect-hdfs/
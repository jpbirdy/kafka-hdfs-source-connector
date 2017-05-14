# kafka-hdfs-source-connector
kafka-hdfs-source-connector is a Kafka Connector for loading data from
 HDFS. HDFS sink connector is [hear](*https://github.com/confluentinc/kafka-connect-hdfs)

* allow multi fils
* allow file&folder listen
* allow kerberos

# Development
To build a development version you'll need a recent version of Kafka. You can build kafka-hdfs-source-connector with Maven using the standard lifecycle phases.
Just run ./build.sh to build project to a standalone jar file.

# configures

Source connector properties file may as belows, and should put it to ANY
dictionary, such as '${CONFLUENT_HOME}/etc/schema-registry/connect-avro-standalone.propertiesetc/kafka-connect-hdfs/'

    name=test-hdfs
    connector.class=hdfs.HDFSSourceConnector
    hadoop.conf.dir=
    hadoop.home=
    hdfs.url=hdfs://localhost:9000
    hdfs.authentication.kerberos=false
    connect.hdfs.principal=
    connect.hdfs.keytab=
    hdfs.namenode.principal=
    kerberos.ticket.renew.period.ms=3600000
    file=
    file.path=/tmp
    topic.prefix=test-hdfs

## Running steps:

1. CD to CONFLUENT_HOME

    <code>$ cd ${CONFLUENT_HOME}</code>
2. Start zookeeper

    <code>$ bin/zookeeper-server-start etc/kafka/zookeeper.properties</code>
3. Start Kafka Broker

    <code>$ bin/kafka-server-start etc/kafka/server.properties</code>
4. Start Schema Registry(Should start in Kafka server)

    <code>$ bin/schema-registry-start etc/schema-registry/schema-registry
    .properties</code>

5. Start HDFS

   <code>$ start-dfs.sh</code>
6. Start HDFS Source

    <code>$ bin/connect-standalone
    etc/schema-registry/connect-avro-standalone.properties
    etc/kafka-connect-hdfs/test-hdfs.properties</code>
7. Connect Console consumer

    <code>./bin/kafka-avro-console-consumer --zookeeper localhost:2181 --topic
    test-hdfs --from-beginning
</code>
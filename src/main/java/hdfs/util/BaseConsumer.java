package hdfs.util;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;


public class BaseConsumer<K, V> implements AutoCloseable {

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final static long ConnectTimeOut = 5000L;

  private boolean beSync = false;

  protected Consumer<K, V> kafkaConsumer = null;

  public BaseConsumer(String host, String group, boolean sync) {
    Properties cfg = getConfig(host, group);

    kafkaConsumer = new KafkaConsumer<>(cfg);

    this.beSync = sync;
  }

  protected Properties getConfig(String server, String grp) {
    Properties props = new Properties();
    //zookeeper 配置
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000);

    //group 代表一个消费组
    props.put(ConsumerConfig.GROUP_ID_CONFIG, grp);

    //zk连接超时
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 300);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);

    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);

    //序列化类
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");

    return props;
  }

  public long consume(List<String> topics, ConsumeMessage consumer) {

    long messageCount = 0;

    kafkaConsumer.subscribe(topics);

    long t1 = System.currentTimeMillis();

    while (!closed.get()) {

      ConsumerRecords<K, V> records = kafkaConsumer.poll(10);

      if (records.isEmpty()) {

        long timespan = System.currentTimeMillis() - t1;

        if (beSync && timespan > ConnectTimeOut)
          break;

        continue;
      }

      for (String topic : topics) {
        for (ConsumerRecord<K, V> topicRecord : records.records(topic)) {
          if (!consumer.interestWith(topic))
            continue;

          if (consumer
              .dealwithMessage(topic, topicRecord.key(), topicRecord.value()))
            messageCount++;
        }
      }

      kafkaConsumer.commitSync();

      t1 = System.currentTimeMillis();
    }

    kafkaConsumer.unsubscribe();

    return messageCount;
  }

  public void shutdown() {
    closed.set(true);
    kafkaConsumer.close();
  }

  @Override
  public void close() throws Exception {
    shutdown();
  }
}

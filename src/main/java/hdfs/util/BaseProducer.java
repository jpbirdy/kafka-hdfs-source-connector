package hdfs.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BaseProducer<K, V> implements AutoCloseable {
  private KafkaProducer<K, V> kafkaProducer = null;

  private boolean needWait4Status = false;

  private String server = null;

  public BaseProducer(boolean waitForStatus, String server) {
    this.needWait4Status = waitForStatus;

    this.server = server;

    kafkaProducer = new KafkaProducer<>(getConfig());
  }

  protected Properties getConfig() {
    // 设置配置属性
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    // 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
    // http://kafka.apache.org/08/configuration.html
    if (needWait4Status) {
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 1000);
    }
    else {
      props.put(ProducerConfig.ACKS_CONFIG, "0");
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
    }

    return props;
  }

  public Map.Entry<Integer, Long> send(String topic, K key, V msg) {
    //如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
    ProducerRecord<K, V> data = new ProducerRecord<>(topic, key, msg);

    Future<RecordMetadata> result = kafkaProducer.send(data);

    if (needWait4Status) {
      try {
        RecordMetadata recordMetadata = result.get();

        return new AbstractMap.SimpleEntry<>(recordMetadata.partition(),
            recordMetadata.offset());

      }
      catch (InterruptedException | ExecutionException e) {
        return new AbstractMap.SimpleEntry<>(-1, -1l);
      }
    }
    else {
      return new AbstractMap.SimpleEntry<>(0, 0l);
    }

  }

  @Override
  public void close() throws Exception {
    if (needWait4Status) {
      kafkaProducer.close();
    }
    else {
      kafkaProducer.close(0, TimeUnit.MILLISECONDS);
    }
  }
}

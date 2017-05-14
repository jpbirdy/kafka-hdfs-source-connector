package hdfs.util;


public interface ConsumeMessage<K, V> {
  boolean interestWith(String topic);

  boolean dealwithMessage(String topic, K key, V msg);
}

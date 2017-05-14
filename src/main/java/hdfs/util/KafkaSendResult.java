package hdfs.util;

import java.util.HashMap;

public class KafkaSendResult {
  public KafkaSendResult(long count) {
    SendCount = count;
  }

  private HashMap<Integer, Long> partitionPos = new HashMap<>();

  public HashMap<Integer, Long> getPartitionInfo() {
    return partitionPos;
  }

  public void setPartitionInfo(HashMap<Integer, Long> partitionPos) {
    this.partitionPos = partitionPos;
  }

  public long SendCount;
}

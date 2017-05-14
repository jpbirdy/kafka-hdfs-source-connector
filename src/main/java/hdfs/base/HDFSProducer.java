package hdfs.base;

import hdfs.util.BaseProducer;
import hdfs.util.KafkaSendResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class HDFSProducer {
  private final String kafkaServer;
  private final String sourcePath;
  private ArrayList<Path> dfsFiles = new ArrayList<>();

  public HDFSProducer(String dfsPath, String host) {
    this.kafkaServer = host;
    this.sourcePath = dfsPath;

    getFiles(dfsPath);
  }

  private void getFiles(String dfsPath) {
    try {
      Configuration cfg = new Configuration(true);

      FileSystem fs = FileSystem.get(cfg);

      Path path = new Path(dfsPath);

      if (fs.isDirectory(path)) {
        RemoteIterator<LocatedFileStatus> childFiles = fs.listFiles(path, true);
        while (childFiles.hasNext()) {
          LocatedFileStatus fileStatus = childFiles.next();

          if (fileStatus.isFile()) {
            dfsFiles.add(fileStatus.getPath());
          }
        }
      }
      else {
        dfsFiles.add(path);
      }

      fs.close();
    }
    catch (IOException e) {
      throw new ConnectException(e.getMessage());
    }
  }

  public boolean send(String topic, Map<String, String> file2Collection) {

    ExecutorService excutor = Executors.newCachedThreadPool();

    HashMap<String, Future<KafkaSendResult>> results = new HashMap<>(
        dfsFiles.size());

    for (Path file : dfsFiles) {
      HDFSFileProducer fileProducer = new HDFSFileProducer(file, kafkaServer,
          topic);

      if (null != file2Collection) {
        for (Map.Entry<String, String> pattern : file2Collection.entrySet()) {
          if (file.getName().startsWith(pattern.getKey())) {
            fileProducer.setKey(pattern.getValue());
            break;
          }
        }
      }

      Future<KafkaSendResult> result = excutor.submit(fileProducer);
      results.put(file.getName(), result);
    }

    while (true) {
      int count = results.size();

      for (Map.Entry<String, Future<KafkaSendResult>> res : results
          .entrySet()) {
        if (res.getValue().isDone()) {
          count--;
        }
      }

      if (0 == count)
        break;
    }

    return true;
  }

  public static void main(String[] args) {
    if (args.length < 3)
      return;

    HDFSProducer producer = new HDFSProducer(args[0], args[1]);

    if (producer.send(args[2], null)) {
      System.out.println("Send hdfs files success.");
    }
    else {
      System.out.println("Send hdfs files failed.");
    }
  }
}

class HDFSFileProducer implements Callable<KafkaSendResult> {

  private Path target_file = null;

  private String server = null;
  private String targetTopic = null;

  private String messageKey = "";

  public HDFSFileProducer(Path filePath, String host, String topic) {
    target_file = filePath;
    server = host;
    targetTopic = topic;
  }

  public void setKey(String key) {
    messageKey = key;
  }

  @Override
  public KafkaSendResult call() throws Exception {
    KafkaSendResult result = new KafkaSendResult(0);

    try {
      FileSystem fs = FileSystem.get(new Configuration());

      if (!fs.exists(target_file)) {
        return result;
      }

      if (messageKey.isEmpty()) {
        messageKey = target_file.getName();
      }

      FSDataInputStream fsIn = fs.open(target_file);

      InputStreamReader input = new InputStreamReader(fsIn);

      BufferedReader reader = new BufferedReader(input);

      HashMap<Integer, Long> partitionPos = new HashMap<>();
      long totalCount = 0;

      String lineTxt = null;

      try (BaseProducer<String, String> producer = new BaseProducer<>(true,
          server)) {
        while (null != (lineTxt = reader.readLine())) {
          Map.Entry<Integer, Long> res = producer
              .send(targetTopic, messageKey, lineTxt);

          if (res.getValue() < 0)
            continue;

          totalCount++;

          if (!partitionPos.containsKey(res.getKey())) {
            partitionPos.put(res.getKey(), res.getValue());
          }
        }
      }
      catch (Exception e) {
        e.printStackTrace();
      }

      reader.close();
      fsIn.close();

      fs.close();

      result.SendCount = totalCount;
      result.setPartitionInfo(partitionPos);

    }
    catch (IOException e) {
      throw new ConnectException(e.getMessage());
    }

    return result;
  }
}

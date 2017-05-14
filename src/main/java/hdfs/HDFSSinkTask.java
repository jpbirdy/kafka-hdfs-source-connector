package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Map;


public class HDFSSinkTask extends SinkTask {
  public final static String TOPIC = "topic";
  public final static String TARGET_FILE = "targetfile";

  private String topic = "";
  private Path targetFile = null;

  @Override
  public String version() {
    return null;
  }

  @Override
  public void start(Map<String, String> props) {
    if (!props.containsKey(TOPIC)) {
      throw new ConnectException(TOPIC);
    }

    topic = props.get(TOPIC);

    if (!props.containsKey(TARGET_FILE)) {
      throw new ConnectException(TARGET_FILE);
    }

    targetFile = new Path(props.get(TARGET_FILE));

    try (FileSystem fileSystem = FileSystem.get(new Configuration())) {
      if (!fileSystem.createNewFile(targetFile)) {
        throw new ConnectException("Cannot create file on HDFS");
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void put(Collection<SinkRecord> collection) {
    try (FileSystem fileSystem = FileSystem.get(new Configuration())) {
      FSDataOutputStream outputStream = fileSystem.append(targetFile);

      BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(outputStream));

      for (SinkRecord record : collection) {
        writer.write(String.format("%s\t%s", record.key().toString(),
            record.value().toString()));
        writer.newLine();
      }

      writer.close();

      outputStream.close();

    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  @Override
  public void stop() {

  }
}

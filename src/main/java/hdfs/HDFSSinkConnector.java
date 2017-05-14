package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class HDFSSinkConnector extends SinkConnector {
  public final static String FOLDER_PATH = "folder";
  public final static String TOPICS = "topics";

  private String targetFolder = "";
  private ArrayList<String> topics = new ArrayList<>();

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    if (!props.containsKey(FOLDER_PATH)) {
      throw new ConnectException(FOLDER_PATH);
    }

    targetFolder = props.get(FOLDER_PATH);

    if (!props.containsKey(TOPICS)) {
      throw new ConnectException(TOPICS);
    }

    topics.addAll(Arrays.asList(props.get(TOPICS).split(",")));

    try {
      FileSystem fs = FileSystem.get(new Configuration());

      Path folderPath = new Path(targetFolder);

      if (fs.exists(folderPath)) {
        throw new ConnectException("Exist target folder");
      }

      fs.mkdirs(folderPath);

      fs.close();
    }
    catch (IOException e) {
      throw new ConnectException(e.getMessage());
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return HDFSSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    int taskCount = topics.size();

    ArrayList<Map<String, String>> configs = new ArrayList<>();

    for (String topic : topics) {
      HashMap<String, String> cfg = new HashMap<>();
      cfg.put(HDFSSinkTask.TOPIC, topic);
      cfg.put(HDFSSinkTask.TARGET_FILE, targetFolder + "//" + topic);

      configs.add(cfg);
    }

    return configs;
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return null;
  }
}

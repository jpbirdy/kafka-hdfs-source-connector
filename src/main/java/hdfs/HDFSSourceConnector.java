package hdfs;

import hdfs.source.HdfsSourceConnectorConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jialou.jp
 * @project HdfsKafkaConnector
 * @class HDFSSourceConnector
 * @date 17/5/11 21:15
 * @desc
 */
public class HDFSSourceConnector extends SourceConnector {
  public final static String TYPE = "type";
  //  文件夹
  public final static String TYPE_DIR = "DIR";
  //  文件
  public final static String TYPE_FILE = "FILE";

  private static final Logger log = LoggerFactory
      .getLogger(HDFSSourceConnector.class);


  private String topic = "";
  private String hdfsUrl = "";
  private String keyTabPath = "";
  private String userNameKey = "";

  private List<String> files = new ArrayList<>();
  private List<String> paths = new ArrayList<>();

  private Map<String, String> configProperties;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }


  @Override
  public void start(Map<String, String> props) {
    System.out.println(props);
    configProperties = props;
    if (!props.containsKey(HdfsSourceConnectorConfig.FILE_PATH)) {
      throw new ConnectException(HdfsSourceConnectorConfig.FILE_PATH);
    }

    if (!props.containsKey(HdfsSourceConnectorConfig.FILE)) {
      throw new ConnectException(HdfsSourceConnectorConfig.FILE);
    }

    if (!props.containsKey(HdfsSourceConnectorConfig.HDFS_URL_CONFIG)) {
      throw new ConnectException(HdfsSourceConnectorConfig.HDFS_URL_CONFIG);
    }

//    if (props.containsKey(USER_NAME_KEY)) {
//      userNameKey = props.get(USER_NAME_KEY).trim();
//    }
//
//    if (props.containsKey(KEYTAB_FILE_KEY)) {
//      keyTabPath = props.get(KEYTAB_FILE_KEY).trim();
//    }


    hdfsUrl = props.get(HdfsSourceConnectorConfig.HDFS_URL_CONFIG);
    String pathString = props.get(HdfsSourceConnectorConfig.FILE_PATH);
    String fileString = props.get(HdfsSourceConnectorConfig.FILE);


    if (!props.containsKey(HdfsSourceConnectorConfig.TOPIC_PREFIX_CONFIG)) {
      throw new ConnectException(HdfsSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    }

    topic = props.get(HdfsSourceConnectorConfig.TOPIC_PREFIX_CONFIG);

    try {
      //      FileSystem fs = FileSystem.get(URI.create(hdfsUrl), new Configuration());

      String[] fileArr = fileString.split(",");
      String[] pathArr = pathString.split(",");

      for (String f : fileArr) {
        if ("".equals(f.trim())) {
          continue;
        }
        //        Path path = new Path(hdfsUrl + f);
        //        if (fs.exists(path) && fs.isFile(path)) {
        files.add(f);
        //        }
      }

      for (String f : pathArr) {
        if ("".equals(f.trim())) {
          continue;
        }
        //        Path path = new Path(hdfsUrl + f);
        //        if (fs.exists(path) && fs.isDirectory(path)) {
        paths.add(f);
        //        }
      }

      //      Path path = new Path(target_file_path);

      //      if (fs.isDirectory(path)) {
      //        RemoteIterator<LocatedFileStatus> childFiles = fs.listFiles(path, true);
      //        while (childFiles.hasNext()) {
      //          LocatedFileStatus fileStatus = childFiles.next();
      //
      //          if (fileStatus.isFile()) {
      //            files.add(fileStatus.getPath().toString());
      //          }
      //        }
      //      }
      //      else {
      //        files.add(target_file_path);
      //      }

      //      fs.close();
    }
    catch (Exception e) {
      throw new ConnectException(e.getMessage());
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return HDFSSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();

    for (String file : files) {
      Map<String, String> taskCfg = new HashMap<>();
      taskCfg.putAll(configProperties);
      taskCfg.put(HDFSSourceTask.TARGET, file);
      taskCfg.put(HDFSSourceConnector.TYPE, TYPE_FILE);
      configs.add(taskCfg);
    }

    for (String path : paths) {
      Map<String, String> taskCfg = new HashMap<>();
      taskCfg.putAll(configProperties);
      taskCfg.put(HDFSSourceTask.TARGET, path);
      taskCfg.put(HDFSSourceConnector.TYPE, TYPE_DIR);
      configs.add(taskCfg);
    }

    return configs;
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return HdfsSourceConnectorConfig.CONFIG_DEF;
  }


  public static void main(String[] args) throws IOException {
    String testFile = "/tmp/7";
    String hdfsUri = "hdfs://192.168.1.19:9000";
    String dsf = hdfsUri + testFile;

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);
    //    FileSystem fs = FileSystem.get(conf);

    //
    //
    //
    //    RemoteIterator<LocatedFileStatus> childFiles = fs.listFiles(new Path(dsf), true);
    //    while (childFiles.hasNext()) {
    //      LocatedFileStatus fileStatus = childFiles.next();
    //
    //      if (fileStatus.isFile()) {
    //        System.out.println(fileStatus.getPath().toString());
    //        System.out.println(fileStatus.getPath().getName());
    //        System.out.println(fileStatus.getPath().toUri().getPath());
    //        System.out.println(fileStatus.getPath().toUri().getPath().replaceAll
    //            ("/", "_"));
    //
    //      }
    //    }
    //
    //    System.exit(0);
    //
    //
    //


    FSDataInputStream hdfsInStream = fs.open(new Path(dsf));

    //    InputStreamReader input = new InputStreamReader(hdfsInStream);

    //    BufferedReader reader = new BufferedReader(input);

    int lastOffset = 0;

    hdfsInStream.seek(lastOffset);

    int buffer = 16;
    byte[] ioBuffer = new byte[buffer];
    byte[] lineBuffer = new byte[buffer * buffer];
    int lineLen = 0;
    int readLen = hdfsInStream.read(ioBuffer);
    long lastPos = lastOffset;

    while (readLen != -1) {
      for (int i = 0; i < readLen; i++) {
        if (ioBuffer[i] != '\n') {
          lineBuffer[lineLen++] = ioBuffer[i];
        }
        else {
          System.out.print("get a line offset is " + (lastPos + i));
          System.out.println(new String(lineBuffer, 0, lineLen));
          lineLen = 0;
        }
      }
      System.out
          .println(readLen + " line is " + new String(lineBuffer, 0, lineLen));
      lastPos = hdfsInStream.getPos();
      System.out.println(lastPos);
      //      System.out.print(hdfsInStream.getPos());
      readLen = hdfsInStream.read(ioBuffer);

    }

    //    String line = null;
    //    while((line = reader.readLine()) != null) {
    //      System.out.println(line);
    //    }

    hdfsInStream.close();
    fs.close();

    System.out.println("hello");
  }
}

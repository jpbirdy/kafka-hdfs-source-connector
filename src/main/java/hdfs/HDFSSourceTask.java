package hdfs;

import hdfs.source.HdfsSourceConnectorConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.*;


/**
 * @author jialou.jp
 * @project HdfsKafkaConnector
 * @class HDFSSourceTask
 * @date 17/5/11 21:05
 * @desc
 */
public class HDFSSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(HDFSSourceTask.class);
  public final static String TARGET = "TARGET";

  private static final int READ_SLEEP = 5000;
  private static final int BUFFER_SIZE = 4096;
  //  读取hdfs,控制一个文件单次最大消息量
  private static final int FILE_MAX_MESSAGE_SIZE = 4096;

  private String target = "";

  private String topicPrefix = "";
  private String hdfsUrl = "";

  private String type = "";

//  private String keyTabPath = "";
//  private String userNameKey = "";

  private FileSystem fileSystem = null;

  private Configuration conf = new Configuration();

  private Map<String, Object> offset = new HashMap<>();
  private Map<String, Long> fileOffset = new HashMap<>();
  private HdfsSourceConnectorConfig connectorConfig;
  //  private Map<Map<String, String>, Map<String, Object>> offset = null;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    if (!props.containsKey(TARGET)) {
      throw new ConnectException(TARGET);
    }
    System.out.println("Source TASK start: props is ");
    System.out.println(props);

    HdfsSourceConnectorConfig connectorConfig = new HdfsSourceConnectorConfig(props);

    String hadoopHome = connectorConfig.getString(HdfsSourceConnectorConfig.HADOOP_HOME_CONFIG);
    System.setProperty("hadoop.home.dir", hadoopHome);

    this.connectorConfig = connectorConfig;

    String hadoopConfDir = connectorConfig.getString(HdfsSourceConnectorConfig.HADOOP_CONF_DIR_CONFIG);
    log.info("Hadoop configuration directory {}", hadoopConfDir);
    conf = new Configuration();
    if (!hadoopConfDir.equals("")) {
      conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
      conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));
    }
    try {
      login(connectorConfig);
    }
    catch (IOException e) {
      throw new ConnectException(e);
    }

    target = props.get(TARGET);
    hdfsUrl = props.get(HdfsSourceConnectorConfig.HDFS_URL_CONFIG);
    type = props.get(HDFSSourceConnector.TYPE);
//    keyTabPath = props.get(HdfsSourceConnectorConfig.KEYTAB_FILE_KEY);
//    userNameKey = props.get(HdfsSourceConnectorConfig.USER_NAME_KEY);

    if (!props.containsKey(HdfsSourceConnectorConfig.TOPIC_PREFIX_CONFIG)) {
      throw new ConnectException(HdfsSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    }
    topicPrefix = props.get(HdfsSourceConnectorConfig.TOPIC_PREFIX_CONFIG);

  }

  private String generateTopic(String file) {
    String suffix = file.replaceAll("/", "_");
//    return topicPrefix + suffix;
    return topicPrefix;
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    try {
      fileSystem = FileSystem.get(URI.create(hdfsUrl), conf);

      //      Path filePath = new Path(target);
      //      if (!fileSystem.exists(filePath))
      //        return null;
      //      System.out.println("全局local offset " + fileOffset);
      ArrayList<SourceRecord> records = new ArrayList<>();
      if (type.equals(HDFSSourceConnector.TYPE_FILE)) {
        records.addAll(loadFileRecord(target));
      }
      else {
        records.addAll(loadDirRecord(target));
      }

      if (records.size() == 0) {
        log.info(target + ", 啥也没读到,让我歇一时");
        Thread.sleep(READ_SLEEP);
      }
      return records;

    }
    catch (IOException e) {
      // Underlying stream was killed, probably as a result of calling stop.
      // Allow to return null, and driving thread will handle any shutdown if necessary.
    }

    return null;
  }


  private List<SourceRecord> loadFileRecord(String target)
      throws IOException, InterruptedException {

    List<SourceRecord> records = new ArrayList<>();
    Path filePath = new Path(hdfsUrl + target);
    if (!fileSystem.exists(filePath) || !fileSystem.isFile(filePath)) {
      return records;
    }
    FSDataInputStream fsIn = fileSystem.open(filePath);
    //      InputStreamReader input = new InputStreamReader(fsIn);
    //      BufferedReader reader = new BufferedReader(input);

    Long streamOffset;
    if (!fileOffset.containsKey(target)) {
      loadOffset(target);
      //      System.out.println("%%%%%%%%%%offset%%%%%%%");
      //      System.out.println(offset);
      if (offset != null && offset.containsKey("position")) {
        fileOffset.put(target, (Long) offset.get("position"));
        //        System.out.println("get seek " + (Long) offset.get("position"));
      }
      else {
        fileOffset.put(target, 0L);
      }
    }

    streamOffset = fileOffset.get(target);
    //    log.info("当前文件" + target + " offset" + streamOffset);
    fsIn.seek(streamOffset);


    //      fsIn.seek(0);

    //      System.out.println(offset);
    //      String lineTxt = null;
    //      Long pos = 0L;

    byte[] ioBuffer = new byte[BUFFER_SIZE];
    byte[] lineBuffer = new byte[BUFFER_SIZE * BUFFER_SIZE];
    int lineLen = 0;
    int readLen = fsIn.read(ioBuffer);
    long lastPos = streamOffset;

    //    if (readLen == -1) {
    //      log.info("已到文件尾部");
    //    }

    while (readLen != -1 && records.size() < FILE_MAX_MESSAGE_SIZE) {
      for (int i = 0; i < readLen; i++) {
        if (ioBuffer[i] != '\n') {
          lineBuffer[lineLen++] = ioBuffer[i];
        }
        else {
          long pos = (lastPos + i + 1);
          //          System.out.print("get a line offset is "+ pos);
          String lineTxt = new String(lineBuffer, 0, lineLen);
          lineLen = 0;
          Map<String, String> sourcePartition = Collections
              .singletonMap("filename", target);
          Map<String, Long> sourceOffset = Collections
              .singletonMap("position", pos);
          fileOffset.put(target, pos);

          records.add(new SourceRecord(sourcePartition, sourceOffset,
              generateTopic(target), Schema.STRING_SCHEMA, lineTxt));
        }
      }
      lastPos = fsIn.getPos();
      readLen = fsIn.read(ioBuffer);
    }

    if (readLen > 0) {
      log.info("文件量过大,超过最大消息量,发送一批消息后再来读");
    }
    //      reader.close();
    fsIn.close();

    //      System.out.println(records);
    return records;
  }

  private List<SourceRecord> loadDirRecord(String target)
      throws IOException, InterruptedException {
    List<SourceRecord> records = new ArrayList<>();
    Path filePath = new Path(hdfsUrl + target);
    if (!fileSystem.exists(filePath) || !fileSystem.isDirectory(filePath)) {
      return records;
    }

    RemoteIterator<LocatedFileStatus> childFiles = fileSystem
        .listFiles(filePath, true);
    while (childFiles.hasNext()) {
      LocatedFileStatus fileStatus = childFiles.next();

      if (fileStatus.isFile()) {
        records.addAll(loadFileRecord(fileStatus.getPath().toUri().getPath()));
      }
    }

    return records;
  }

  @Override
  public void stop() {
    try {
      fileSystem.close();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void loadOffset(String target) {
    offset = new HashMap<>();
    log.info("loading offset " + target);
    //    List<Map<String, String>> partitions = new ArrayList<>();
    Map<String, String> sourcePartition = Collections
        .singletonMap("filename", target);
    //    partitions.add(sourcePartition);
    offset = context.offsetStorageReader().offset(sourcePartition);
    //    offset.putAll(context.offsetStorageReader().offset(sourcePartition));
  }


  private volatile boolean isRunning;


  private void login(HdfsSourceConnectorConfig connectorConfig) throws IOException {
//    if (UserGroupInformation.isSecurityEnabled()) {
//      SecurityUtil.login(conf, HDFSSourceConnector.KEYTAB_FILE_KEY,
//          HDFSSourceConnector.USER_NAME_KEY);
//    }

    boolean secureHadoop = connectorConfig.getBoolean(HdfsSourceConnectorConfig.HDFS_AUTHENTICATION_KERBEROS_CONFIG);

    if (secureHadoop) {
      SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);

      String principalConfig = connectorConfig.getString(HdfsSourceConnectorConfig.CONNECT_HDFS_PRINCIPAL_CONFIG);
      String keytab = connectorConfig.getString(HdfsSourceConnectorConfig.CONNECT_HDFS_KEYTAB_CONFIG);

      conf.set("hadoop.security.authentication", "kerberos");
      conf.set("hadoop.security.authorization", "true");
      String hostname = InetAddress.getLocalHost().getCanonicalHostName();
      // replace the _HOST specified in the principal config to the actual host
      String principal = SecurityUtil.getServerPrincipal(principalConfig, hostname);
      String namenodePrincipalConfig = connectorConfig.getString(
          HdfsSourceConnectorConfig
          .HDFS_NAMENODE_PRINCIPAL_CONFIG);

      String namenodePrincipal = SecurityUtil.getServerPrincipal(namenodePrincipalConfig, hostname);
      // namenode principal is needed for multi-node hadoop cluster
      if (conf.get("dfs.namenode.kerberos.principal") == null) {
        conf.set("dfs.namenode.kerberos.principal", namenodePrincipal);
      }
      log.info("Hadoop namenode principal: " + conf.get("dfs.namenode.kerberos.principal"));

      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
      final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      log.info("Login as: " + ugi.getUserName());

      final long renewPeriod = connectorConfig.getLong(HdfsSourceConnectorConfig.KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG);

      isRunning = true;
      Thread ticketRenewThread = new Thread(new Runnable() {
        @Override
        public void run() {
          synchronized (HDFSSourceTask.this) {
            while (isRunning) {
              try {
                HDFSSourceTask.this.wait(renewPeriod);
                if (isRunning) {
                  ugi.reloginFromKeytab();
                }
              }
              catch (IOException e) {
                // We ignore this exception during relogin as each successful relogin gives
                // additional 24 hours of authentication in the default config. In normal
                // situations, the probability of failing relogin 24 times is low and if
                // that happens, the task will fail eventually.
                log.error("Error renewing the ticket", e);
              }
              catch (InterruptedException e) {
                // ignored
              }
            }
          }
        }
      });
      log.info("Starting the Kerberos ticket renew thread with period {}ms.", renewPeriod);
      ticketRenewThread.start();
    }


  }
}

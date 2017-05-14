/**
 * Created by jpbirdy on 17/5/11.
 */

package hdfs.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author jialou.jp
 * @project HdfsKafkaConnector
 * @class HdfsSourceConnectorConfig
 * @date 17/5/11 23:43
 * @desc
 */
public class HdfsSourceConnectorConfig extends AbstractConfig {

  public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
  private static final String TOPIC_PREFIX_DOC =
      "Prefix to prepend to table names to generate the name of the Kafka topic to publish data " +
          "to, or in the case of a custom query, the full name of the topic to publish to.";
  private static final String TOPIC_PREFIX_DISPLAY = "TOPIC_PREFIX Prefix";

  public static final String CONNECTOR_GROUP = "Connector";

  public final static String FILE_PATH = "file.path";

  private static final String FILE_PATH_DOC = "hdfs path to listen";
  private static final String FILE_PATH_DISPLAY = "hdfs path";

  public final static String FILE = "file";
  private static final String FILE_DOC = "hdfs file to listen";
  private static final String FILE_DISPLAY = "hdfs file";

//  public final static String KEYTAB_FILE_KEY_CONFIG = "hdfs.keytab.file";
//  private static final String KEYTAB_FILE_KEY_DOC = "kerberos";
//  private static final String KEYTAB_FILE_KEY_DISPLAY = "hdfs file";



  public static final String HDFS_GROUP = "HDFS";
  // HDFS Group
  public static final String HDFS_URL_CONFIG = "hdfs.url";
  private static final String HDFS_URL_DOC =
      "The HDFS connection URL. This configuration has the format of hdfs:://hostname:port and "
          + "specifies the HDFS to export data to.";
  private static final String HDFS_URL_DISPLAY = "HDFS URL";

  public static final String HADOOP_CONF_DIR_CONFIG = "hadoop.conf.dir";
  private static final String HADOOP_CONF_DIR_DOC =
      "The Hadoop configuration directory.";
  public static final String HADOOP_CONF_DIR_DEFAULT = "";
  private static final String HADOOP_CONF_DIR_DISPLAY = "Hadoop Configuration Directory";

  public static final String HADOOP_HOME_CONFIG = "hadoop.home";
  private static final String HADOOP_HOME_DOC =
      "The Hadoop home directory.";
  public static final String HADOOP_HOME_DEFAULT = "";
  private static final String HADOOP_HOME_DISPLAY = "Hadoop home directory";



  public static final String SECURITY_GROUP = "Security";
  // Security group
  public static final String HDFS_AUTHENTICATION_KERBEROS_CONFIG = "hdfs.authentication.kerberos";
  private static final String HDFS_AUTHENTICATION_KERBEROS_DOC =
      "Configuration indicating whether HDFS is using Kerberos for authentication.";
  private static final boolean HDFS_AUTHENTICATION_KERBEROS_DEFAULT = false;
  private static final String HDFS_AUTHENTICATION_KERBEROS_DISPLAY = "HDFS Authentication Kerberos";

  public static final String CONNECT_HDFS_PRINCIPAL_CONFIG = "connect.hdfs.principal";
  private static final String CONNECT_HDFS_PRINCIPAL_DOC =
      "The principal to use when HDFS is using Kerberos to for authentication.";
  public static final String CONNECT_HDFS_PRINCIPAL_DEFAULT = "";
  private static final String CONNECT_HDFS_PRINCIPAL_DISPLAY = "Connect Kerberos Principal";

  public static final String CONNECT_HDFS_KEYTAB_CONFIG = "connect.hdfs.keytab";
  private static final String CONNECT_HDFS_KEYTAB_DOC =
      "The path to the keytab file for the HDFS connector principal. "
          + "This keytab file should only be readable by the connector user.";
  public static final String CONNECT_HDFS_KEYTAB_DEFAULT = "";
  private static final String CONNECT_HDFS_KEYTAB_DISPLAY = "Connect Kerberos Keytab";

  public static final String HDFS_NAMENODE_PRINCIPAL_CONFIG = "hdfs.namenode.principal";
  private static final String HDFS_NAMENODE_PRINCIPAL_DOC = "The principal for HDFS Namenode.";
  public static final String HDFS_NAMENODE_PRINCIPAL_DEFAULT = "";
  private static final String HDFS_NAMENODE_PRINCIPAL_DISPLAY = "HDFS NameNode Kerberos Principal";

  public static final String KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG = "kerberos.ticket.renew.period.ms";
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DOC =
      "The period in milliseconds to renew the Kerberos ticket.";
  public static final long KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT = 60000 * 60;
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY = "Kerberos Ticket Renew Period (ms)";


  private static final ConfigDef.Recommender hdfsAuthenticationKerberosDependentsRecommender = new BooleanParentRecommender(HDFS_AUTHENTICATION_KERBEROS_CONFIG);


  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public static ConfigDef baseConfigDef() {
    ConfigDef config = new ConfigDef();
    config.define(TOPIC_PREFIX_CONFIG, ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH, TOPIC_PREFIX_DOC, CONNECTOR_GROUP, 4,
        ConfigDef.Width.MEDIUM, TOPIC_PREFIX_DISPLAY)
        .define(FILE_PATH, ConfigDef.Type.STRING,
            ConfigDef.Importance.MEDIUM, FILE_PATH_DOC, CONNECTOR_GROUP, 4,
            ConfigDef.Width.MEDIUM, FILE_PATH_DISPLAY)
        .define(FILE, ConfigDef.Type.STRING,
            ConfigDef.Importance.MEDIUM, FILE_DOC, CONNECTOR_GROUP, 4,
            ConfigDef.Width.MEDIUM, FILE_DISPLAY)

        ;

    // Define HDFS configuration group
    config.define(HDFS_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, HDFS_URL_DOC, HDFS_GROUP, 1, ConfigDef.Width.MEDIUM, HDFS_URL_DISPLAY)
        .define(HADOOP_CONF_DIR_CONFIG, ConfigDef.Type.STRING, HADOOP_CONF_DIR_DEFAULT, ConfigDef.Importance.HIGH, HADOOP_CONF_DIR_DOC, HDFS_GROUP, 2, ConfigDef.Width.MEDIUM, HADOOP_CONF_DIR_DISPLAY)
        .define(HADOOP_HOME_CONFIG, ConfigDef.Type.STRING, HADOOP_HOME_DEFAULT, ConfigDef.Importance.HIGH, HADOOP_HOME_DOC, HDFS_GROUP, 3, ConfigDef.Width.SHORT, HADOOP_HOME_DISPLAY)
    ;


    // Define Security configuration group
    config.define(HDFS_AUTHENTICATION_KERBEROS_CONFIG, ConfigDef.Type.BOOLEAN, HDFS_AUTHENTICATION_KERBEROS_DEFAULT, ConfigDef.Importance.HIGH, HDFS_AUTHENTICATION_KERBEROS_DOC,
        SECURITY_GROUP, 1, ConfigDef.Width.SHORT, HDFS_AUTHENTICATION_KERBEROS_DISPLAY,
        Arrays.asList(CONNECT_HDFS_PRINCIPAL_CONFIG, CONNECT_HDFS_KEYTAB_CONFIG, HDFS_NAMENODE_PRINCIPAL_CONFIG, KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG))
        .define(CONNECT_HDFS_PRINCIPAL_CONFIG, ConfigDef.Type.STRING, CONNECT_HDFS_PRINCIPAL_DEFAULT, ConfigDef.Importance.HIGH, CONNECT_HDFS_PRINCIPAL_DOC,
            SECURITY_GROUP, 2, ConfigDef.Width.MEDIUM, CONNECT_HDFS_PRINCIPAL_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender)
        .define(CONNECT_HDFS_KEYTAB_CONFIG, ConfigDef.Type.STRING, CONNECT_HDFS_KEYTAB_DEFAULT, ConfigDef.Importance.HIGH, CONNECT_HDFS_KEYTAB_DOC,
            SECURITY_GROUP, 3, ConfigDef.Width.MEDIUM, CONNECT_HDFS_KEYTAB_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender)
        .define(HDFS_NAMENODE_PRINCIPAL_CONFIG, ConfigDef.Type.STRING, HDFS_NAMENODE_PRINCIPAL_DEFAULT, ConfigDef.Importance.HIGH, HDFS_NAMENODE_PRINCIPAL_DOC,
            SECURITY_GROUP, 4, ConfigDef.Width.MEDIUM, HDFS_NAMENODE_PRINCIPAL_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender)
        .define(KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG, ConfigDef.Type.LONG, KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT, ConfigDef.Importance.LOW, KERBEROS_TICKET_RENEW_PERIOD_MS_DOC,
            SECURITY_GROUP, 5, ConfigDef.Width.SHORT, KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender);


    return config;
  }

  private static class BooleanParentRecommender implements ConfigDef.Recommender {

    protected String parentConfigName;

    public BooleanParentRecommender(String parentConfigName) {
      this.parentConfigName = parentConfigName;
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return new LinkedList<>();
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return (Boolean) connectorConfigs.get(parentConfigName);
    }
  }

  private static boolean classNameEquals(String className, Class<?> clazz) {
    return className.equals(clazz.getSimpleName()) || className.equals(clazz.getCanonicalName());
  }

  public HdfsSourceConnectorConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
  }
}

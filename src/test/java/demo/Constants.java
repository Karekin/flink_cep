package demo;

public final class Constants {
    // Required configurations constants for connecting to Database
    public static final String JDBC_URL_ARG = "jdbcUrl";
    public static final String JDBC_DRIVE = "com.mysql.cj.jdbc.Driver";
    public static final String TABLE_NAME_ARG = "tableName";
    public static final String JDBC_INTERVAL_MILLIS_ARG = "jdbcIntervalMs";
//    public static final String JDBC_URL ="jdbc:mysql://192.168.88.166:3306/xp_flink_dev?user=root&password=123456&useUnicode=true&characterEncoding=utf8&autoReconnect=true";
    public static final String JDBC_URL ="jdbc:mysql://10.192.30.60:4306/xp_flink_dev?user=XP_Admin&password=XP_AdminXP_Admin&useUnicode=true&characterEncoding=utf8&autoReconnect=true";
    public static final String TABLE_NAME ="dynamic_cep";

    // Required configurations constants for connecting to Kafka
    public static final String KAFKA_BROKERS_ARG = "kafkaBrokers";
    public static final String INPUT_TOPIC_ARG = "inputTopic";
    public static final String INPUT_TOPIC_GROUP_ARG = "inputTopicGroup";


}

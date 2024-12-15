package org.mason77.constant;

public class StreamConstant {

    // flink env的配置文件
    public static final String FLINK_ENV_CONFIG = "flink-env.properties";

    // kafka 配置 文件版
    public static final String KAFKA_ENV_CONFIG = "kafka-env.properties";
    public static final String DIM_KAFKA_CONSUMER = "dim-kafka-consumer.properties";

    // kafka 常量版 配置
    public static final String KAFKA_BROKERS = "node02:9092,node03:9092,node04:9092";
    public static final String KAFKA_DB = "topic_db";
    public static final String KAFKA_LOG = "topic_log";
    public static final String KAFKA_GROUP_ID = "dim_app_group";

    // dim的消费者配置

    // mysql配置
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_HOST = "node02";
    public static final Integer MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "000000";
    public static final String MYSQL_URL = "jdbc:mysql://node02:3306?useSSL=false";

    public static final String MYSQL_DB = "gmall_config";
    public static final String MYSQL_TABLE = "table_process_dim";

    // 维度表配置
    public static final String DIM_MYSQL = "dim-mysql.properties";
    public static final String HBASE_NAMESPACE = "gmall";
}

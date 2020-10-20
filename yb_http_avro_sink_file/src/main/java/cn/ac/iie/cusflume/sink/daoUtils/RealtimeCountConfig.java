package cn.ac.iie.cusflume.sink.daoUtils;

import java.io.Serializable;

public class RealtimeCountConfig implements Serializable {

    private static final long serialVersionUID = -8649024767966235184L;

    /**
     * realtime_service_config.properties
     */
    public static final String BOOTSTRAP_SERVERS = RealtimeCountConfigurations.getStringProperty(0, "bootstrap.servers");
    public static final String BOOTSTRAP_OUTPUT_SERVERS = RealtimeCountConfigurations.getStringProperty(0, "bootstrap.output.servers");
    public static final Integer BATCH_INSERT_NUM = RealtimeCountConfigurations.getIntProperty(0, "batch.insert.num");
    public static final String GROUP_ID_PREFIX = RealtimeCountConfigurations.getStringProperty(0, "group.id.prefix");//groupid前缀
    public static final String GROUP_ID_SUFFIX = RealtimeCountConfigurations.getStringProperty(0, "group.id.suffix");//groupid后缀
    public static final String AUTO_OFFSET_RESET = RealtimeCountConfigurations.getStringProperty(0, "auto.offset.reset");

    public static final String FAIL_NTC_COLLECT_MAIL_LOG = RealtimeCountConfigurations.getStringProperty(0, "fail.ntc.collect.mail.log");
    public static final String FAIL_NTC_COLLECT_HTTP_DOC_LOG = RealtimeCountConfigurations.getStringProperty(0, "fail.ntc.collect.http.doc.log");
    public static final String FAIL_NTC_COLLECT_FTP_DOC_LOG = RealtimeCountConfigurations.getStringProperty(0, "fail.ntc.collect.ftp.doc.log");

    /**
     * 状态X-Tag
     */
    public static final String MONITOR_NOFILE_MSG_X_TAG = RealtimeCountConfigurations.getStringProperty(0, "monitor.nofile.msg.xtag");

    /**
     * 放到YbHttpAvroSinkFile文件中的monitorMsgXTag配置
     */
    public static final String MONITOR_MSG_SINK_FILE_XTAG = RealtimeCountConfigurations.getStringProperty(0, "monitor.msg.sink.file.xtag");

    /**
     * 监控器类型:一部(yb):状态上报;广东(gd):日志打印,然后靠脚本收集到influxdb上传(外部脚本完成)
     */
    public static final String MONITOR_TYPE = RealtimeCountConfigurations.getStringProperty(0, "monitor.type");
    public static final Integer MONITOR_CLEAN_TYPE = RealtimeCountConfigurations.getIntProperty(0, "monitor.clean.type");

    /**
     * 状态上报所需参数-仅一部-系统组件编码-目前24832-20191222
     */
    public static final String MONITOR_SYSTEM_COMPONENT_CODE = RealtimeCountConfigurations.getStringProperty(0, "monitor.system.component.code");
    public static final String MONITOR_SYSTEM_COMPONENT_CODE_FLUME = RealtimeCountConfigurations.getStringProperty(0, "monitor.system.component.code.flume");

    //监控用influxDb配置
    public static final String INFLUX_TABLE = RealtimeCountConfigurations.getStringProperty(0, "influx.table");
    public static final String INFLUX_ACTION_MSG = RealtimeCountConfigurations.getStringProperty(0, "influx.action.msg");
    public static final String INFLUX_ACTION_FILE = RealtimeCountConfigurations.getStringProperty(0, "influx.action.file");
    public static final String INFLUX_ACTION_FILE_BYTES = RealtimeCountConfigurations.getStringProperty(0, "influx.action.file.bytes");
    public static final String INFLUX_ADDRESS = RealtimeCountConfigurations.getStringProperty(0, "influx.address");
    public static final String INFLUX_USERNAME = RealtimeCountConfigurations.getStringProperty(0, "influx.username");
    public static final String INFLUX_PASSWORD = RealtimeCountConfigurations.getStringProperty(0, "influx.password");
    public static final String INFLUX_DATABASE = RealtimeCountConfigurations.getStringProperty(0, "influx.database");

    /**
     * 注册获取的数据对象SchemaID
     */
    public static final Integer SCHEMA_ID_NTC_COLLECT_FILE_LOG = RealtimeCountConfigurations.getIntProperty(0, "schema.id.ntc-collect-file-log");
    public static final Integer SCHEMA_ID_NTC_CONN_RECORD_LOG = RealtimeCountConfigurations.getIntProperty(0, "schema.id.ntc-conn-record-log");
    public static final Integer SCHEMA_ID_NTC_COLLECT_HTTP_DOC_LOG = RealtimeCountConfigurations.getIntProperty(0, "schema.id.ntc-collect-http-doc-log");
    public static final Integer SCHEMA_ID_NTC_COLLECT_HTTP_AV_LOG = RealtimeCountConfigurations.getIntProperty(0, "schema.id.ntc-collect-http-av-log");
    public static final Integer SCHEMA_ID_NTC_COLLECT_MAIL_LOG = RealtimeCountConfigurations.getIntProperty(0, "schema.id.ntc-collect-mail-log");
    public static final Integer SCHEMA_ID_NTC_COLLECT_FTP_DOC_LOG = RealtimeCountConfigurations.getIntProperty(0, "schema.id.ntc-collect-ftp-doc-log");
    public static final Integer SCHEMA_ID_NTC_COLLECT_TELNET_LOG = RealtimeCountConfigurations.getIntProperty(0, "schema.id.ntc-collect-telnet-log");
    //一部状态上报数据对象
    public static final Integer SCHEMA_ID_MONITOR_MSG = RealtimeCountConfigurations.getIntProperty(0, "schema.id.monitor-msg");
    //广东状态上报数据对象
    public static final Integer SCHEMA_ID_INFLUX_SAPP_BPS_STAT_LOG = RealtimeCountConfigurations.getIntProperty(0, "schema.id.influx-sapp-bps-stat-log");


    /**
     * flume_config.properties
     */
    //HTTP配置信息
    public static final Integer HTTP_ASYNC_PARALLELISM = RealtimeCountConfigurations.getIntProperty(1, "http.async.parallelism");
    public static final Integer HTTP_ASYNC_SOCKETTIMEOUT = RealtimeCountConfigurations.getIntProperty(1, "http.async.socketTimeout");
    public static final Integer HTTP_ASYNC_CONNECTTIMEOUT = RealtimeCountConfigurations.getIntProperty(1, "http.async.connectTimeout");
    public static final Integer HTTP_ASYNC_POOLSIZE = RealtimeCountConfigurations.getIntProperty(1, "http.async.poolSize");
    public static final Integer HTTP_ASYNC_MAXPERROUTE = RealtimeCountConfigurations.getIntProperty(1, "http.async.maxPerRoute");
    public static final Integer HTTP_ASYNC_CONNECTIONREQUESTTIMEOUT = RealtimeCountConfigurations.getIntProperty(1, "http.async.connectionRequestTimeout");

    //序列化Schema信息
    /**
     * 携手前端定义
     */
    //纯消息
    public static final String SCHEMA_NTC_CONN_RECORD_LOG = RealtimeCountConfigurations.getStringProperty(1, "schema.ntc-conn-record-log");
    public static final String SCHEMA_NTC_COLLECT_SSL_LOG = RealtimeCountConfigurations.getStringProperty(1, "schema.ntc-collect-ssl-log");
    public static final String SCHEMA_NTC_COLLECT_DNS_LOG = RealtimeCountConfigurations.getStringProperty(1, "schema.ntc-collect-dns-log");
    //文件消息
    public static final String SCHEMA_NTC_COLLECT_MAIL_LOG = RealtimeCountConfigurations.getStringProperty(1, "schema.ntc-collect-mail-log");
    public static final String SCHEMA_NTC_COLLECT_HTTP_DOC_LOG = RealtimeCountConfigurations.getStringProperty(1, "schema.ntc-collect-http-doc-log");
    public static final String SCHEMA_NTC_COLLECT_HTTP_AV_LOG = RealtimeCountConfigurations.getStringProperty(1, "schema.ntc-collect-http-av-log");
    public static final String SCHEMA_NTC_COLLECT_FTP_DOC_LOG = RealtimeCountConfigurations.getStringProperty(1, "schema.ntc-collect-ftp-doc-log");
    public static final String SCHEMA_NTC_COLLECT_TELNET_LOG = RealtimeCountConfigurations.getStringProperty(1, "schema.ntc-collect-telnet-log");
    //特殊
    public static final String SCHEMA_NTC_COLLECT_FILE_LOG = RealtimeCountConfigurations.getStringProperty(1, "schema.ntc-collect-file-log");
    public static final String SCHEMA_LOG_TAG = RealtimeCountConfigurations.getStringProperty(1, "schema.log-tag");
    /**
     * 完全自己定义(和前端无关)-修改不频繁
     */
    public static final String SCHEMA_INFLUX_SAPP_BPS_STAT_LOG = RealtimeCountConfigurations.getStringProperty(1, "schema.influx-sapp-bps-stat-log");
    /**
     * 总线定义或数据字典定义(不需要频繁修改)
     */
    public static final String SCHEMA_MONITOR_MSG = RealtimeCountConfigurations.getStringProperty(1, "schema.monitor-msg");
    public static final String SCHEMA_STATUS_TAG = RealtimeCountConfigurations.getStringProperty(1, "schema.status-tag");

}
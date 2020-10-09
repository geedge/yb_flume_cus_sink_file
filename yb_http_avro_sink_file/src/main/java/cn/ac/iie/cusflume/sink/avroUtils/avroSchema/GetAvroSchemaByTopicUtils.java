package cn.ac.iie.cusflume.sink.avroUtils.avroSchema;

import cn.ac.iie.cusflume.sink.daoUtils.RealtimeCountConfig;
import org.apache.log4j.Logger;


public class GetAvroSchemaByTopicUtils {
    private static Logger logger = Logger.getLogger(GetAvroSchemaByTopicUtils.class);

    public static String getAvroSchemaByTopicName(String topic) {
        switch (topic) {
            case "NTC-CONN-RECORD-LOG":
                return RealtimeCountConfig.SCHEMA_NTC_CONN_RECORD_LOG;
            case "NTC-COLLECT-SSL-LOG"://server_cert直接作为json放在String类型下
                return RealtimeCountConfig.SCHEMA_NTC_COLLECT_SSL_LOG;
            case "NTC-COLLECT-DNS-LOG":
                return RealtimeCountConfig.SCHEMA_NTC_COLLECT_DNS_LOG;
            case "NTC-COLLECT-MAIL-LOG":
                return RealtimeCountConfig.SCHEMA_NTC_COLLECT_MAIL_LOG;
            case "NTC-COLLECT-HTTP-DOC-LOG":
                return RealtimeCountConfig.SCHEMA_NTC_COLLECT_HTTP_DOC_LOG;
            case "NTC-COLLECT-HTTP-AV-LOG":
                /**
                 * 20200810新增，数据Schema等同于NTC-COLLECT-HTTP-DOC-LOG
                 */
                return RealtimeCountConfig.SCHEMA_NTC_COLLECT_HTTP_AV_LOG;
            case "NTC-COLLECT-FTP-DOC-LOG":
                return RealtimeCountConfig.SCHEMA_NTC_COLLECT_FTP_DOC_LOG;
            case "NTC-COLLECT-TELNET-LOG":
                return RealtimeCountConfig.SCHEMA_NTC_COLLECT_TELNET_LOG;
            case "log-tag"://20191219新增-日志标签
                return RealtimeCountConfig.SCHEMA_LOG_TAG;
            case "NTC-COLLECT-FILE-LOG":
                return RealtimeCountConfig.SCHEMA_NTC_COLLECT_FILE_LOG;
            case "monitor-msg"://一部状态上传对应的数据对象-格式为总线规定
                return RealtimeCountConfig.SCHEMA_MONITOR_MSG;
            case "status-tag"://状态数据标签-搭配状态数据对象使用-出自数据字典
                return RealtimeCountConfig.SCHEMA_STATUS_TAG;
            case "INFLUX-SAPP-BPS-STAT-LOG"://20200110广东SAPP状态上传-即广东状态上传对应的数据对象-格式自己定义
                return RealtimeCountConfig.SCHEMA_INFLUX_SAPP_BPS_STAT_LOG;
            default:
                logger.error("There is no corresponding topic! topic name is :" + topic);
                break;
        }
        return null;
    }

}

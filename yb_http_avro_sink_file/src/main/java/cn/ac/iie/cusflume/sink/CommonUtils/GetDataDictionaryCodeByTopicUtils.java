package cn.ac.iie.cusflume.sink.CommonUtils;

import cn.ac.iie.cusflume.sink.daoUtils.RealtimeCountConfig;
import org.apache.log4j.Logger;


public class GetDataDictionaryCodeByTopicUtils {
    private static Logger logger = Logger.getLogger(GetDataDictionaryCodeByTopicUtils.class);

    /**
     * 获取数据对象自身的类型编码
     *
     * @param topic
     * @return
     */
    public static int getDataObjectTypeCodeByTopicName(String topic) {
        switch (topic) {
            /**
             * 非文件消息-log
             */
            case "NTC-CONN-RECORD-LOG":
            case "NTC-COLLECT-HTTP-LOG":
            case "NTC-COLLECT-SSL-LOG":
            case "NTC-COLLECT-DNS-LOG":
            case "NTC-COLLECT-FILE-LOG"://此时没有数据对象,数据对象类型选择1-log日志
                return 1;
            /**
             * 文件消息-file
             */
            case "NTC-COLLECT-FTP-DOC-LOG":
            case "NTC-COLLECT-HTTP-DOC-LOG":
            case "NTC-COLLECT-HTTP-AV-LOG"://20200810新增，等同于NTC-COLLECT-HTTP-DOC-LOG
            case "NTC-COLLECT-MAIL-LOG":
            case "NTC-COLLECT-TELNET-LOG"://20200325新增
                return 2;
            /**
             * 状态消息(监控消息)
             * status
             */
            case "monitor-msg":
            case "INFLUX-SAPP-BPS-STAT-LOG"://20200113新增-广东流量状态上传
                return 4;
            default:
                logger.error("GetDataDictionaryCodeByTopicUtils--->getDataObjectTypeCodeByTopicName There is a unknown topic! topic name is :" + topic);
                break;
        }
        return 0;//标识错误
    }

    /**
     * 获取数据标签SchemaID
     * NTC-COLLECT-FILE-LOG填文件标签76
     * 其他日志都是读取自身的d_tag应该填写日志标签75
     * 状态上报(一部需要)暂时作为状态标签处理,暂定78
     * @param topic
     * @return
     */
    public static int getDataTagSchemaIDByTopicName(String topic) {
        switch (topic) {
            /**
             * 文件标签-file_tag
             */
            case "NTC-COLLECT-FILE-LOG"://此时没有数据对象,数据对象类型选择1-log日志,但数据标签的SchemaID选择文件标签76
                return 76;
            /**
             * 日志标签-log_tag
             */
            case "NTC-CONN-RECORD-LOG":
            case "NTC-COLLECT-HTTP-LOG":
            case "NTC-COLLECT-SSL-LOG":
            case "NTC-COLLECT-DNS-LOG":
            case "NTC-COLLECT-FTP-DOC-LOG":
            case "NTC-COLLECT-HTTP-DOC-LOG":
            case "NTC-COLLECT-HTTP-AV-LOG"://20200810新增，等同于NTC-COLLECT-HTTP-DOC-LOG
            case "NTC-COLLECT-MAIL-LOG":
            case "NTC-COLLECT-TELNET-LOG"://20200325新增
                return 75;
            /**
             * 状态标签-status_tag
             */
            case "monitor-msg":
            case "INFLUX-SAPP-BPS-STAT-LOG"://20200113新增-广东流量状态上传
                return 78;//不太清楚状态标签是使用日志标签还是使用状态标签,78为状态标签SchemaID
            default:
                logger.error("GetDataDictionaryCodeByTopicUtils--->getDataTagSchemaIDByTopicName There is a unknown topic! topic name is :" + topic);
                break;
        }
        return 0;//标识错误
    }

    /**
     * 获取数据对象SchemaID-需要到总线注册获取
     *
     * @param topic
     * @return
     */
    public static int getDataObjectSchemaIDByTopicName(String topic) {
        switch (topic) {
            /**
             * 非文件消息-log
             */
            case "NTC-COLLECT-FILE-LOG"://文件标签单独发送,作为消息特殊处理,此处的SchemaID为0(规定),即没有数据对象这一块
                return RealtimeCountConfig.SCHEMA_ID_NTC_COLLECT_FILE_LOG;
            case "NTC-CONN-RECORD-LOG":
                return RealtimeCountConfig.SCHEMA_ID_NTC_CONN_RECORD_LOG;
            case "NTC-COLLECT-HTTP-LOG":
                return 3;
            case "NTC-COLLECT-SSL-LOG":
                return 4;
            case "NTC-COLLECT-DNS-LOG":
                return 5;
            /**
             * 文件消息-file
             */
            case "NTC-COLLECT-FTP-DOC-LOG":
                return RealtimeCountConfig.SCHEMA_ID_NTC_COLLECT_FTP_DOC_LOG;
            case "NTC-COLLECT-TELNET-LOG"://20200325新增
                return RealtimeCountConfig.SCHEMA_ID_NTC_COLLECT_TELNET_LOG;
            case "NTC-COLLECT-HTTP-DOC-LOG":
                return RealtimeCountConfig.SCHEMA_ID_NTC_COLLECT_HTTP_DOC_LOG;
            case "NTC-COLLECT-HTTP-AV-LOG"://20200810新增，等同于NTC-COLLECT-HTTP-DOC-LOG
                return RealtimeCountConfig.SCHEMA_ID_NTC_COLLECT_HTTP_AV_LOG;
            case "NTC-COLLECT-MAIL-LOG":
                return RealtimeCountConfig.SCHEMA_ID_NTC_COLLECT_MAIL_LOG;
            /**
             * 一部状态消息(监控消息)-一部数据对象SchemaID-总线本身已经提前注册
             * status
             */
            case "monitor-msg":
                return RealtimeCountConfig.SCHEMA_ID_MONITOR_MSG;//此avro格式为总线规定的状态对象schema,数据字典数据子类编码-16387,schemaID-22
            /**
             * 广东状态消息(监控消息)-广东数据对象SchemaID
             * status
             */
            case "INFLUX-SAPP-BPS-STAT-LOG":
                return RealtimeCountConfig.SCHEMA_ID_INFLUX_SAPP_BPS_STAT_LOG;
            default:
                logger.error("GetDataDictionaryCodeByTopicUtils--->getDataObjectSchemaIDByTopicName There is a unknown topic! topic name is :" + topic);
                break;
        }
        return 0;//0表示标识错误
    }

}

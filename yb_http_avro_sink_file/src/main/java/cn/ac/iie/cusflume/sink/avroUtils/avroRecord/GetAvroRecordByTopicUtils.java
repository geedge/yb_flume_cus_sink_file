package cn.ac.iie.cusflume.sink.avroUtils.avroRecord;

import cn.ac.iie.cusflume.sink.CommonUtils.DecodeUtils;
import cn.ac.iie.cusflume.sink.bean.dataBean.NTC_COLLECT_DNS_LOG;
import cn.ac.iie.cusflume.sink.bean.dataBean.NTC_COLLECT_HTTP_LOG;
import cn.ac.iie.cusflume.sink.bean.dataBean.NTC_COLLECT_SSL_LOG;
import cn.ac.iie.cusflume.sink.bean.fileBean.FILE_TAG_BEAN;
import cn.ac.iie.cusflume.sink.bean.fileBean.NTC_COLLECT_MAIL_LOG;
import cn.ac.iie.cusflume.sink.bean.tagBean.LOG_D_TAG_BEAN;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class GetAvroRecordByTopicUtils {
    private static Logger logger = Logger.getLogger(GetAvroRecordByTopicUtils.class);

    private static JsonAvroConverter converter = new JsonAvroConverter();

    public static GenericRecord getAvroRecordByTopicName(String topic, GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        switch (topic) {
            case "NTC-CONN-RECORD-LOG":
                return getConnRecordAvroRecord(recordAvro, dataJson, schemaStr);
//            case "NTC-COLLECT-HTTP-LOG":
//                return getCollHttpAvroRecord(recordAvro, dataJson);
            case "NTC-COLLECT-SSL-LOG":
                return getCollSslAvroRecord(recordAvro, dataJson, schemaStr);
            case "NTC-COLLECT-DNS-LOG":
                return getCollDnsAvroRecord(recordAvro, dataJson, schemaStr);
            case "NTC-COLLECT-HTTP-DOC-LOG":
                return getCollectHttpDocPostAvroRecord(recordAvro, dataJson, schemaStr);
            case "NTC-COLLECT-HTTP-AV-LOG":
                return getCollectHttpAvPostAvroRecord(recordAvro, dataJson, schemaStr);
            case "NTC-COLLECT-FTP-DOC-LOG":
                return getCollFtpDocAvroRecord(recordAvro, dataJson, schemaStr);
            case "NTC-COLLECT-MAIL-LOG":
                return getCollMailAvroRecord(recordAvro, dataJson, schemaStr);
            case "NTC-COLLECT-FILE-LOG":
                return getCollFileAvroRecord(recordAvro, dataJson, schemaStr);
            case "log-tag":
                return getLogTagAvroRecord(recordAvro, dataJson, schemaStr);
            case "monitor-msg"://一部状态数据对象avro序列化
                return getMonitorMsgAvroRecord(recordAvro, dataJson, schemaStr);
            case "status-tag"://数据字典-状态数据标签序列化
                return getStatusTagAvroRecord(recordAvro, dataJson, schemaStr);
            case "INFLUX-SAPP-BPS-STAT-LOG"://20200110广东SAPP状态上传-广东状态数据对象avro序列化
                return getInfluxStatusAvroRecord(recordAvro, dataJson, schemaStr);
            case "NTC-COLLECT-TELNET-LOG":
                return getCollTelnetAvroRecord(recordAvro, dataJson, schemaStr);
            default:
                logger.error("There is no corresponding topic! topic name is :" + topic);
                break;
        }
        return null;
    }

    private static GenericRecord getMonitorMsgAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        recordAvro = converter.convertToGenericDataRecord(dataJson.getBytes(), schemaStr);

        return recordAvro;
    }

    private static GenericRecord getCollFtpDocAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        recordAvro = converter.convertToGenericDataRecord(dataJson.getBytes(), schemaStr);
        return recordAvro;
    }

    private static GenericRecord getCollTelnetAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        recordAvro = converter.convertToGenericDataRecord(dataJson.getBytes(), schemaStr);
        return recordAvro;
    }

    private static GenericRecord getCollectHttpDocPostAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        recordAvro = converter.convertToGenericDataRecord(dataJson.getBytes(), schemaStr);

        return recordAvro;
    }

    private static GenericRecord getCollectHttpAvPostAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        recordAvro = converter.convertToGenericDataRecord(dataJson.getBytes(), schemaStr);
        return recordAvro;
    }

    private static GenericRecord getCollDnsAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        NTC_COLLECT_DNS_LOG ntcCollectDnsLog = JSONObject.parseObject(dataJson, NTC_COLLECT_DNS_LOG.class);//用于json字段压成字符串
        recordAvro = converter.convertToGenericDataRecord(JSONObject.toJSONString(ntcCollectDnsLog).getBytes(), schemaStr);
        return recordAvro;
    }

    private static GenericRecord getCollSslAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        NTC_COLLECT_SSL_LOG ntcCollectSslLog = JSONObject.parseObject(dataJson, NTC_COLLECT_SSL_LOG.class);//用于json字段压成字符串
        recordAvro = converter.convertToGenericDataRecord(JSONObject.toJSONString(ntcCollectSslLog).getBytes(), schemaStr);
        return recordAvro;
    }

    private static GenericRecord getCollHttpAvroRecord(GenericRecord recordAvro, String dataJson) {
        NTC_COLLECT_HTTP_LOG ntcCollectHttpLog = JSONObject.parseObject(dataJson, NTC_COLLECT_HTTP_LOG.class);

        String k = ntcCollectHttpLog.getK();
        //String v = ntcCollectHttpLog.getV();
        //解析k,获取四元组,http_seq,proxy_flag,isn
        String[] splitK = k.split("#");
        //String quad = splitK[0];//四元组集合,例如192.168.11.156.53608>112.80.255.122.80,>左边为源ip和源端口,>右边为目的ip和目的端口
        String http_seq = splitK[1];
        String proxy_flag = splitK[2];
        ntcCollectHttpLog.setHttp_seq(Integer.parseInt(http_seq));
        ntcCollectHttpLog.setProxy_flag(Integer.parseInt(proxy_flag));

        return recordAvro;
    }

    private static GenericRecord getCollMailAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        NTC_COLLECT_MAIL_LOG ntcCollectMailLog = JSONObject.parseObject(dataJson, NTC_COLLECT_MAIL_LOG.class);

        if (StringUtil.isNotBlank(ntcCollectMailLog.getSubject())) {
            String subjectCharset = JSONObject.parseObject(dataJson).getString("subject_charset");
            ntcCollectMailLog.setSubject(DecodeUtils.base64Str(ntcCollectMailLog.getSubject(), subjectCharset));
        }

        recordAvro = converter.convertToGenericDataRecord(JSONObject.toJSONString(ntcCollectMailLog).getBytes(), schemaStr);

        return recordAvro;
    }

    private static GenericRecord getConnRecordAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        recordAvro = converter.convertToGenericDataRecord(dataJson.getBytes(), schemaStr);

        return recordAvro;
    }


    private static GenericRecord getCollFileAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        FILE_TAG_BEAN fileTagBean = JSONObject.parseObject(dataJson, FILE_TAG_BEAN.class);

        if (StringUtil.isNotBlank(fileTagBean.getFile_name())) {
            String fileNameCharset = JSONObject.parseObject(dataJson).getString("file_name_charset");
            fileTagBean.setFile_name(DecodeUtils.base64Str(fileTagBean.getFile_name(), fileNameCharset));
        }

        recordAvro = converter.convertToGenericDataRecord(JSONObject.toJSONString(fileTagBean).getBytes(), schemaStr);

        return recordAvro;
    }

    /**
     * 消息内日志标签序列化
     *
     * @param recordAvro
     * @param dataJson
     * @param schemaStr
     * @return
     */
    private static GenericRecord getLogTagAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        //20200228新增-用于解决前端long类型精度丢失,前端以字符串写入,这一步将字符串转为数字
        LOG_D_TAG_BEAN log_d_tag_bean = JSONObject.parseObject(dataJson, LOG_D_TAG_BEAN.class);
        recordAvro = converter.convertToGenericDataRecord(JSONObject.toJSONString(log_d_tag_bean).getBytes(), schemaStr);//20200228新修改,解决前端精度丢失问题

        return recordAvro;
    }

    /**
     * 消息内状态标签序列化
     *
     * @param recordAvro
     * @param dataJson
     * @param schemaStr
     * @return
     */
    private static GenericRecord getInfluxStatusAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        recordAvro = converter.convertToGenericDataRecord(dataJson.getBytes(), schemaStr);

        return recordAvro;
    }

    /**
     * 消息内状态标签序列化
     *
     * @param recordAvro
     * @param dataJson
     * @param schemaStr
     * @return
     */
    private static GenericRecord getStatusTagAvroRecord(GenericRecord recordAvro, String dataJson, Schema schemaStr) {
        recordAvro = converter.convertToGenericDataRecord(dataJson.getBytes(), schemaStr);

        return recordAvro;
    }

}

package cn.ac.iie.cusflume.sink.CommonUtils;

import cn.ac.iie.cusflume.sink.bean.fileBean.*;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.Arrays;


public class GetFilePathByTopicUtils {
    private static Logger logger = Logger.getLogger(GetFilePathByTopicUtils.class);

    public static String getFilePathByTopicName(String dataJson, String topic) {
        switch (topic) {
            /**
             * 非文件消息
             */
            case "NTC-CONN-RECORD-LOG":
                return "-";
            case "NTC-COLLECT-HTTP-LOG":
                return "-";
            case "NTC-COLLECT-SSL-LOG":
                return "-";
            case "NTC-COLLECT-DNS-LOG":
                return "-";
            /**
             * 文件消息
             */
            case "NTC-COLLECT-FTP-DOC-LOG":
                return getCollFtpDocFilePath(dataJson);//20200211新增
            case "NTC-COLLECT-HTTP-AV-LOG"://和NTC-COLLECT-HTTP-DOC-LOG共用相同Schema-20200904
//                return getCollHttpAvFilePath(dataJson);
            case "NTC-COLLECT-HTTP-DOC-LOG":
                return getCollHttpDocFilePath(dataJson);
            case "NTC-COLLECT-MAIL-LOG":
                return getCollMailFilePath(dataJson);
            case "NTC-COLLECT-FILE-LOG":
//                return getCollFileFilePath(dataJson);
                return "-";//由于这里Mail与Http都已经设置了传FilePath请求头,所以这里就没有设置再设置请求头,后期有需要可以加上
            case "NTC-COLLECT-TELNET-LOG":
                return getCollTelnetFilePath(dataJson);//20200325新增
            /**
             * 状态消息(监控消息)
             */
            case "monitor-msg":
                return "-";
            case "INFLUX-SAPP-BPS-STAT-LOG":
                return "-";
            default:
                logger.error("GetFilePathByTopicUtils---There is a unknown topic! topic name is :" + topic);
                break;
        }
        return "-";
    }

    /**
     * 获取NTC-COLLECT-MAIL-LOG的FilePath
     *
     * @param dataJson
     * @return
     */
    private static String getCollMailFilePath(String dataJson) {
        NTC_COLLECT_MAIL_LOG ntcCollectMailLog = JSONObject.parseObject(dataJson, NTC_COLLECT_MAIL_LOG.class);
        String filePathList = "[\"" + StringUtils.join(ntcCollectMailLog.getD_tag().getFile_path_list(), "\",\"") + "\"]";//新-20191222;//元素有双引号与转义\版本
        if (!("[\"\"]".equals(filePathList)) && StringUtil.isNotBlank(filePathList)) {
            return filePathList;
        } else {
            return "-";
        }

    }

    /**
     * 获取NTC-COLLECT-HTTP-DOC-LOG的FilePath
     * NTC-COLLECT-HTTP-AV-LOG复用NTC-COLLECT-HTTP-DOC-LOG的数据结构
     * @param dataJson
     * @return
     */
    private static String getCollHttpDocFilePath(String dataJson) {
        NTC_COLLECT_HTTP_DOC_LOG ntcCollectHttpDocLog = JSONObject.parseObject(dataJson, NTC_COLLECT_HTTP_DOC_LOG.class);
        String filePathList = "[\"" + StringUtils.join(ntcCollectHttpDocLog.getD_tag().getFile_path_list(), "\",\"") + "\"]";//新-20191222;//元素有双引号与转义\版本
        if (!("[\"\"]".equals(filePathList)) && StringUtil.isNotBlank(filePathList)) {
            return filePathList;
        } else {
            return "-";
        }
    }

    /**
     * 获取NTC-COLLECT-FTP-DOC-LOG的FilePath
     *
     * @param dataJson
     * @return
     */
    private static String getCollFtpDocFilePath(String dataJson) {
        NTC_COLLECT_FTP_DOC_LOG ntcCollectFtpDocLog = JSONObject.parseObject(dataJson, NTC_COLLECT_FTP_DOC_LOG.class);
        String filePathList = "[\"" + StringUtils.join(ntcCollectFtpDocLog.getD_tag().getFile_path_list(), "\",\"") + "\"]";
        if (!("[\"\"]".equals(filePathList)) && StringUtil.isNotBlank(filePathList)) {
            return filePathList;
        } else {
            return "-";
        }
    }

    /**
     * 获取NTC-COLLECT-TELNET-LOG的FilePath
     *
     * @param dataJson
     * @return
     */
    private static String getCollTelnetFilePath(String dataJson) {
        NTC_COLLECT_TELNET_LOG ntcCollectTelnetLog = JSONObject.parseObject(dataJson, NTC_COLLECT_TELNET_LOG.class);
        String filePathList = "[\"" + StringUtils.join(ntcCollectTelnetLog.getD_tag().getFile_path_list(), "\",\"") + "\"]";
        if (!("[\"\"]".equals(filePathList)) && StringUtil.isNotBlank(filePathList)) {
            return filePathList;
        } else {
            return "-";
        }
    }

    /**
     * 获取NTC-COLLECT-FILE-LOG的FilePath
     *
     * @param dataJson
     * @return
     */
    private static String getCollFileFilePath(String dataJson) {
        FILE_TAG_BEAN ntcCollectHttpDocLog = JSONObject.parseObject(dataJson, FILE_TAG_BEAN.class);
        String filePath = "[\"" + ntcCollectHttpDocLog.getFile_path() + "\"]";
        if (!("[\"\"]".equals(filePath)) && StringUtil.isNotBlank(filePath)) {
            return filePath;
        } else {
            return "-";
        }
    }

}

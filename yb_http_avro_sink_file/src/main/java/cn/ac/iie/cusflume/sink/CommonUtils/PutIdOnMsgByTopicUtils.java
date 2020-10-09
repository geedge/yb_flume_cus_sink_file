package cn.ac.iie.cusflume.sink.CommonUtils;

import cn.ac.iie.cusflume.sink.bean.fileBean.NTC_COLLECT_FTP_DOC_LOG;
import cn.ac.iie.cusflume.sink.bean.fileBean.NTC_COLLECT_HTTP_DOC_LOG;
import cn.ac.iie.cusflume.sink.bean.fileBean.NTC_COLLECT_MAIL_LOG;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


public class PutIdOnMsgByTopicUtils {
    private static Logger logger = Logger.getLogger(PutIdOnMsgByTopicUtils.class);

    /**
     * 单条数据包含单个文件时使用
     *
     * @param topic
     * @param dataJson
     * @param fileId
     * @return
     */
    public static String putIdOnMsgByTopicName(String topic, String dataJson, String fileId) {
        switch (topic) {
//            case "NTC-COLLECT-MAIL-LOG":
//                return compCollectMail();
            case "NTC-COLLECT-HTTP-DOC-LOG":
                return compCollectHttpDoc(topic, dataJson, fileId);
            case "NTC-COLLECT-FTP-DOC-LOG":
                return compCollectFtpDoc(topic, dataJson, fileId);
            default:
                logger.error("There is no corresponding topic! topic name is :" + topic);
                break;
        }
        return null;
    }

    /**
     * 单条数据包含多种文件时使用
     *
     * @param topic
     * @param dataJson
     * @param fileId
     * @param fileKind
     * @return
     */
    public static String putIdOnMsgByTopicName(String topic, String dataJson, String fileId, String fileKind) {
        switch (topic) {
            case "NTC-COLLECT-MAIL-LOG":
                return compCollectMail(topic, dataJson, fileId, fileKind);
            default:
                logger.error("There is no corresponding topic! topic name is :" + topic);
                break;
        }
        return null;
    }

    private static String compCollectMail(String topic, String dataJson, String fileId, String fileKind) {
        NTC_COLLECT_MAIL_LOG ntc_collect_mail_log = JSONObject.parseObject(dataJson, NTC_COLLECT_MAIL_LOG.class);
        if ("eml_file".equals(fileKind)) {
            ntc_collect_mail_log.setEml_file_id(fileId);
        } else if ("attachments".equals(fileKind)) {
            ntc_collect_mail_log.setAttachments_id(fileId);
        } else {
            logger.error("PutIdOnMsgByTopicUtils compCollectMail fileKind is unknown===>>>" + fileKind);
        }
        return JSONObject.toJSONString(ntc_collect_mail_log);
    }


    private static String compCollectFtpDoc(String topic, String dataJson, String fileId) {
        NTC_COLLECT_FTP_DOC_LOG ntc_collect_ftp_doc_log = JSONObject.parseObject(dataJson, NTC_COLLECT_FTP_DOC_LOG.class);
//        ntc_collect_ftp_doc_log.setRes_body_file_id(fileId);
        ntc_collect_ftp_doc_log.setContent_path(fileId);
        return JSONObject.toJSONString(ntc_collect_ftp_doc_log);
    }


    private static String compCollectHttpDoc(String topic, String dataJson, String fileId) {
        NTC_COLLECT_HTTP_DOC_LOG ntcCollectHttpDocLog = JSONObject.parseObject(dataJson, NTC_COLLECT_HTTP_DOC_LOG.class);
        if (StringUtils.isNotBlank(ntcCollectHttpDocLog.getReq_body_file())) {
            ntcCollectHttpDocLog.setReq_body_file_id(fileId);
        } else if (StringUtils.isNotBlank(ntcCollectHttpDocLog.getRes_body_file())) {
            ntcCollectHttpDocLog.setRes_body_file_id(fileId);
        } else {
            logger.error("PutIdOnMsgByTopicUtils there is no Req_body_file or Res_body_file in this message===>>>" + dataJson);
        }
        return JSONObject.toJSONString(ntcCollectHttpDocLog);
    }

    /**---------------------------------删除的topic------------------------------------------------------------**/

}

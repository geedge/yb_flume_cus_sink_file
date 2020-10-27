package cn.ac.iie.cusflume.sink.HttpAsyncUtils.mail;

import cn.ac.iie.cusflume.sink.CommonUtils.PutIdOnMsgByTopicUtils;
import cn.ac.iie.cusflume.sink.HttpAsyncUtils.HttpClientUtil;
import cn.ac.iie.cusflume.sink.avroUtils.AvroMonitorTimerTask;
import cn.ac.iie.cusflume.sink.avroUtils.DataCenterLoad;
import cn.ac.iie.cusflume.sink.bean.configBean.ConfigInfo;
import cn.ac.iie.cusflume.sink.bean.postFileBean.PostFileResBody;
import cn.ac.iie.cusflume.sink.bean.producer.Res.ProResBody;
import cn.ac.iie.cusflume.sink.daoUtils.KafkaDB;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

/**
 * 用于处理NTC_COLLECT_MAIL_LOG
 * 异步httpclient回调对象---POST文件
 * 在get文件成功后开始执行post文件操作
 * post文件成功时,获取到文件id加入到message中,开始执行post消息操作,
 * 为此需要message(kafka获取)以及post消息url(配置文件获取),
 * 还需要User-Agent以及X-Tag用于验证
 * <p>
 * post失败时,重试三次
 */
public class AsyncPostMailFilesCallback implements FutureCallback<HttpResponse> {
    private static Logger LOG = Logger.getLogger(AsyncPostMailFilesCallback.class);

    private DataCenterLoad dclAsyncPost;
//    private KafkaDB kafkaDBAsyncPost;

    private String postFileUrl;
    private String postMsgUrl;
    //    private InputStream resultIs;//get到的文件流,用于post文件失败时重试---这里MAIL未使用此对象
    private ConfigInfo configInfo;//1.存储post文件,信息url(配置文件获取);2.存储入库前验证文件,信息url(配置文件获取);3.存储文件,信息SessionCookie(请求总线获取);4.存储msg作为avro入库所需的topicName,userAgent,xTag,batchSize

    private String sendMsg;//用于post成功后补全id发往zx或者post失败发往kafka

    private PostFileResBody postFileResBody;//存储post文件成功后返回的响应信息,内部包含文件id,key名为id,这个不走构造方法,用于获取返回响应体时使用,也不需要保存
    private int count;

    //为MAIL新增
    private int dealIsCount;//存放已处理的流的个数
    private int isCount;//存放流的总数

//    private InputStream emlFileResultIs;//存放get到的emlFile文件流,用于post文件失败时重试,注意这个不走构造方法--流模式
    private byte[] emlFileResultIs;//存放get到的emlFile文件流,用于post文件失败时重试,注意这个不走构造方法---字节数组模式

//    private LinkedList<InputStream> attachmentsResultIsList;//存放get到的attachments集合文件流,用于post文件失败时重试,注意这个不走构造方法---流模式
    private LinkedList<byte[]> attachmentsResultIsList;//存放get到的attachments集合文件流,用于post文件失败时重试,注意这个不走构造方法---字节数组模式

    private LinkedList<String> attachmentsIdList;//用于临时存放获取的attachmentsId

    public AsyncPostMailFilesCallback(ConfigInfo configInfo, String sendMsg, int count, int isCount, int dealIsCount, LinkedList<String> attachmentsIdList) {
        this.configInfo = configInfo;
        this.postFileUrl = configInfo.getPostFileUrl();
        this.postMsgUrl = configInfo.getSingleProduceUrl();
        this.sendMsg = sendMsg;//存放对应于url的数据
        this.count = count;//初始为0
        this.isCount = isCount;//初始总数为url总数
        this.dealIsCount = dealIsCount;//初始为0
        this.attachmentsIdList = attachmentsIdList;//初始为空

        //初始化入库程序
        dclAsyncPost = new DataCenterLoad();
//        kafkaDBAsyncPost = KafkaDB.getInstance();
    }

    public String getPostFileUrl() {
        return postFileUrl;
    }

    public void setPostFileUrl(String postFileUrl) {
        this.postFileUrl = postFileUrl;
    }

    public String getPostMsgUrl() {
        return postMsgUrl;
    }

    public void setPostMsgUrl(String postMsgUrl) {
        this.postMsgUrl = postMsgUrl;
    }

//    public InputStream getResultIs() {
//        return resultIs;
//    }
//
//    public void setResultIs(InputStream resultIs) {
//        this.resultIs = resultIs;
//    }

    public ConfigInfo getConfigInfo() {
        return configInfo;
    }

    public void setConfigInfo(ConfigInfo configInfo) {
        this.configInfo = configInfo;
    }

    public String getSendMsg() {
        return sendMsg;
    }

    public void setSendMsg(String sendMsg) {
        this.sendMsg = sendMsg;
    }

    public PostFileResBody getPostFileResBody() {
        return postFileResBody;
    }

    public void setPostFileResBody(PostFileResBody postFileResBody) {
        this.postFileResBody = postFileResBody;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getDealIsCount() {
        return dealIsCount;
    }

    public void setDealIsCount(int dealIsCount) {
        this.dealIsCount = dealIsCount;
    }

    public int getIsCount() {
        return isCount;
    }

    public void setIsCount(int isCount) {
        this.isCount = isCount;
    }

//    public InputStream getEmlFileResultIs() {
    public byte[] getEmlFileResultIs() {
        return emlFileResultIs;
    }

//    public void setEmlFileResultIs(InputStream emlFileResultIs) {
    public void setEmlFileResultIs(byte[] emlFileResultIs) {
        this.emlFileResultIs = emlFileResultIs;
    }

//    public LinkedList<InputStream> getAttachmentsResultIsList() {
    public LinkedList<byte[]> getAttachmentsResultIsList() {
        return attachmentsResultIsList;
    }

//    public void setAttachmentsResultIsList(LinkedList<InputStream> attachmentsResultIsList) {
    public void setAttachmentsResultIsList(LinkedList<byte[]> attachmentsResultIsList) {
        this.attachmentsResultIsList = attachmentsResultIsList;
    }

    public LinkedList<String> getAttachmentsIdList() {
        return attachmentsIdList;
    }

    public void setAttachmentsIdList(LinkedList<String> attachmentsIdList) {
        this.attachmentsIdList = attachmentsIdList;
    }

    @Override
    public String toString() {
        return "AsyncPostMailFilesCallback{" +
                "postFileUrl='" + postFileUrl + '\'' +
                ", postMsgUrl='" + postMsgUrl + '\'' +
                ", configInfo=" + configInfo +
                ", sendMsg='" + sendMsg + '\'' +
                ", postFileResBody=" + postFileResBody +
                ", count=" + count +
                ", dealIsCount=" + dealIsCount +
                ", isCount=" + isCount +
                ", attachmentsIdList=" + attachmentsIdList +
                '}';
    }

    /**
     * 请求完成后调用该函数
     */
    @Override
    public void completed(HttpResponse response) {
        try {
//        LOG.info("filename:" + source.getFilename() + " status: " + response.getStatusLine().getStatusCode() + " response:" + getHttpContent(response));
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                dealIsCount++;
                String postResBody = getHttpContent(response);
                if (dealIsCount >= 2) {//说明获取的id是attachments的id
                    //post文件成功后可以获取文件id
                    if (StringUtils.isNotBlank(postResBody)) {
                        LOG.info("AsyncPostMailFilesCallback post attachmentFile completed,post file statuscode is:{" + statusCode + "},this attachmentFile resBody is " + postResBody);
                        this.postFileResBody = JSONObject.parseObject(postResBody, PostFileResBody.class);
//                    String attachmentsFileId = postFileResBody.getData().get("id");//获取文件id------旧
                        String attachmentsFileId = postFileResBody.getMsg();//获取文件id------新-20191115-返回体格式-{"code": "200", "msg": "2019111507094164188567821505", "data": "不需要额外数据"}
                        if (StringUtils.isNotBlank(attachmentsFileId)) {
                            if (attachmentsIdList != null) {//已经存放attachmentsId,至少已经初始化attachmentsIdList
                                attachmentsIdList.add(attachmentsFileId);
                            } else {//未初始化
                                this.attachmentsIdList = new LinkedList<String>();
                                attachmentsIdList.add(attachmentsFileId);
                            }
                        } else {
                            LOG.warn("AsyncPostMailFilesCallback post attachments_file success and postResBody(response body) is not null but attachmentsFileId is null,this postResBody is===>>>" + postResBody);
                        }
                    } else {
                        LOG.warn("AsyncPostMailFilesCallback post attachments_file success but postResBody(response body) is null.");
                    }
                } else if (dealIsCount == 1) {//说明获取的id是eml_file
                    //post文件成功后补全eml_file_id
                    if (StringUtils.isNotBlank(postResBody)) {
                        LOG.info("AsyncPostMailFilesCallback post eml_file completed,post file statuscode is:{" + statusCode + "},this eml_file resBody is " + postResBody);
                        this.postFileResBody = JSONObject.parseObject(postResBody, PostFileResBody.class);
//                    String emlFileId = postFileResBody.getData().get("id");//获取文件id---------旧方法
                        String emlFileId = postFileResBody.getMsg();//获取文件id---------新方法---返回体格式---{"code": "200", "msg": "2019111507094164188567821505", "data": "不需要额外数据"}
//            putIdOnMsg(fileId,configInfo.getTopicName());
                        if (StringUtils.isNotBlank(emlFileId)) {
                            this.sendMsg = PutIdOnMsgByTopicUtils.putIdOnMsgByTopicName(configInfo.getTopicName(), sendMsg, emlFileId, "eml_file");//补全数据
                        } else {
                            LOG.info("AsyncPostMailFilesCallback post eml_file completed and eml_file resBody is not blank but emlFileId is null ,this eml_file resBody is " + postResBody);
                        }
//                //往总线发送
//                //String urlProducer, String topicName, String jsonData, int batchSize, String userAgent, String msgSessionCookie
//                dclAsyncPost.avroDataLoad(configInfo.getPostMsgUrl(), configInfo.getTopicName(), sendMsg, configInfo.getBatchSize(), configInfo.getUserAgent(), configInfo.getMsgSessionCookie());
                    } else {
                        LOG.warn("AsyncPostMailFilesCallback post eml_file success but postResBody(response body) is null.");
                    }
                }

                //开始补全msg并推送msg至ZX,或者推送余下文件至ZX返回id
                if (dealIsCount == isCount) {//说明文件已经全部推送并且id已经全部获取

                    //全部获取到id后标识文件推送完成,开始计算成功推送的文件和文件大小
                    AvroMonitorTimerTask.fileSuccessSum++;//eml_file的计数
                    AvroMonitorTimerTask.fileSuccessSum += attachmentsIdList.size();//attachments_file的计数
                    //计算成功推送的文件的字节大小
//                    ByteArrayOutputStream byteArrayOutputStream = AvroMonitorTimerTask.getByteArrayOutputStream(emlFileResultIs);
//                    this.emlFileResultIs = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
                    AvroMonitorTimerTask.fileBytesSuccessSum += emlFileResultIs.length;//eml_file字节数的计数

//                    LinkedList<ByteArrayOutputStream> byteArrayOutputStreamsList = AvroMonitorTimerTask.getByteArrayOutputStream(attachmentsResultIsList);
//
//                    //重新填充被消耗的attachmentsResultIsList
//                    attachmentsResultIsList.clear();
//                    for (int i = 0; i < byteArrayOutputStreamsList.size(); i++) {
//                        attachmentsResultIsList.add(new ByteArrayInputStream(byteArrayOutputStreamsList.get(i).toByteArray()));
//                    }

                    for (int i = 0; i < attachmentsResultIsList.size(); i++) {
                        AvroMonitorTimerTask.fileBytesSuccessSum += attachmentsResultIsList.get(i).length;//attachments_file字节数的计数
                    }

                    //将临时保存的attachmentsIdList转化为字符串补全入message
                    String attachmentsIdStr = attachmentsIdList.toString();
                    this.sendMsg = PutIdOnMsgByTopicUtils.putIdOnMsgByTopicName(configInfo.getTopicName(), sendMsg, attachmentsIdStr, "attachments");//补全数据

                    //计算推送消息
                    AvroMonitorTimerTask.msgReadyPostSum++;//多个文件对应一条消息

                    //开始推送消息进入总线
                    ProResBody proResBody = dclAsyncPost.avroDataLoad(configInfo.getSingleProduceUrl(), configInfo.getTopicName(), sendMsg, configInfo.getBatchSize(), configInfo.getUserAgent(), configInfo.getMsgSessionCookie());
                    LOG.info("Send message with many fileId to zx over,this responseBody is===>" + proResBody.toString());
                } else if (dealIsCount < isCount) {//继续发送余下文件流获取id
                    // 注意此处dealIsCount必然是>=1的,所以不需要考虑eml_file,因为eml_file必然已经完成,直接发送attachmentsResultIsList中的流获取id
//                    InputStream attachmentResultIs = attachmentsResultIsList.get(dealIsCount - 1);
                    byte[] attachmentResultIs = attachmentsResultIsList.get(dealIsCount - 1);
                    //更新AsyncPostMailFilesCallback类的内容
                    AsyncPostMailFilesCallback asyncPostMailFilesCallback = updateFieldsAsyncPostMailFilesCallback();
                    //继续发送余下的流获取id
                    HttpClientUtil.httpAsyncPostFileToZx(configInfo.getPostFileUrl(), attachmentResultIs, asyncPostMailFilesCallback);
                }
            } else if (statusCode == 403) {//空文件,不再重试发送
                LOG.info("AsyncPostMailFilesCallback post Files statusCode is 403 so get the fileIs but this minio file is empty.This message is===>>>" + sendMsg + "<<<===");
            } else {
                LOG.info("AsyncPostMailFilesCallback post Files statusCode is " + statusCode + " so get the fileIs but this minio file has some problem.This message is===>>>" + sendMsg + "<<<===");
            }

            HttpClientUtils.closeQuietly(response);
//        TimeTask.successsum ++;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private AsyncPostMailFilesCallback updateFieldsAsyncPostMailFilesCallback() {
        AsyncPostMailFilesCallback asyncHttpClientPostFileCallback = new AsyncPostMailFilesCallback(configInfo, sendMsg, count, isCount, dealIsCount, attachmentsIdList);
        asyncHttpClientPostFileCallback.setEmlFileResultIs(emlFileResultIs);
        asyncHttpClientPostFileCallback.setAttachmentsResultIsList(attachmentsResultIsList);
        return asyncHttpClientPostFileCallback;
    }

    /**
     * 请求取消后调用该函数
     */
    @Override
    public void cancelled() {
        LOG.info("AsyncPostMailFilesCallback is cancelled... ...");
//        LOG.error("filename: " + source.getFilename() + " cancelled");
    }

    /**
     * 请求失败后调用该函数
     */
    @Override
    public void failed(Exception e) {
//        LOG.warn("AsyncPostMailFilesCallback postFile is failed,message is===>>>" + sendMsg + "<<<===");
        try {
            count++;
            if (count > 1) {
                LOG.warn("AsyncPostMailFilesCallback post zxFile is failed,retry count=" + count);
                LOG.info("Now AsyncPostMailFilesCallback dealIsCount is===>{" + dealIsCount + "}");
            }
            if (count > 4) {
                LOG.error("AsyncPostMailFilesCallback post zxFile is failed and already retry 3 times.error===>>>" + e + "<<<===.This failed message is===>>>" + sendMsg + "<<<===.Now attachmentsIdList is===>{" + attachmentsIdList.toString() + "}<===");
            } else {
                if (dealIsCount == 0) {//说明失败的是emlFileResultIs,是从AsyncGetMailFilesCallback传送过来的
                    if (configInfo != null && StringUtils.isNotBlank(sendMsg) && emlFileResultIs != null) {
                        //更新AsyncPostMailFilesCallback内部存储的信息
                        AsyncPostMailFilesCallback asyncPostMailFilesCallback = updateFieldsAsyncPostMailFilesCallback();//注意此处count不再为0
                        //重试时也是以上一次失败时存储的流的形式发往总线
                        HttpClientUtil.httpAsyncPostFileToZx(configInfo.getPostFileUrl(), emlFileResultIs, asyncPostMailFilesCallback);
                    } else {
                        LOG.error("AsyncPostMailFilesCallback when post zxFile the emlFileResultIs is null,this msg is===>>>" + sendMsg);
                    }
                } else if (dealIsCount >= 1) {//说明失败的是attachmentsResultIsList中的流
                    //假设dealIsCount=1，说明eml更新成功并获得id,但attachments第0索引的失败了，此时重试应该使用第0索引的流，即dealIsCount-1
                    //假设dealIsCount=3,说明eml，attachments第0，1索引的流都成功了，但索引2失败了，此时应该重试索引2，即dealIsCount-1
//                    InputStream attachmentResultIs = attachmentsResultIsList.get(dealIsCount - 1);
                    byte[] attachmentResultIs = attachmentsResultIsList.get(dealIsCount - 1);
                    if (configInfo != null && StringUtils.isNotBlank(sendMsg) && attachmentResultIs != null) {
                        //更新AsyncPostMailFilesCallback类的内容
                        AsyncPostMailFilesCallback asyncPostMailFilesCallback = updateFieldsAsyncPostMailFilesCallback();
                        //继续发送余下的流获取id,这个流的索引是根据dealIsCount获取的
                        HttpClientUtil.httpAsyncPostFileToZx(configInfo.getPostFileUrl(), attachmentResultIs, asyncPostMailFilesCallback);
                    } else {
                        LOG.error("AsyncPostMailFilesCallback when post zxFile the attachmentResultIs is null,this msg is===>>>" + sendMsg);
                    }
                }

            }
        } catch (Exception e2) {
            LOG.error("AsyncPostMailFilesCallback retry is error===>>>" + e2);
        }
    }

    //    protected void getHttpContent(HttpResponse response) {
    protected String getHttpContent(HttpResponse response) {

        HttpEntity entity = response.getEntity();
        String body = null;
        if (entity == null) {
            return null;
        }
        try {
            body = EntityUtils.toString(entity, "utf-8");
        } catch (ParseException e) {
            LOG.warn("the response's content inputstream is corrupt", e);
        } catch (IOException e) {
            LOG.warn("the response's content inputstream is corrupt", e);
        }
        return body;
    }
}







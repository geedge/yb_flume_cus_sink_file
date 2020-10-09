package cn.ac.iie.cusflume.sink.HttpAsyncUtils;

//import com.google.api.client.util.Lists;
//import com.mesa.miniotohttp.tools.OssUtil;
//import com.mesa.miniotohttp.tools.TimeTask;

import cn.ac.iie.cusflume.sink.CommonUtils.PutIdOnMsgByTopicUtils;
import cn.ac.iie.cusflume.sink.HttpAsyncUtils.ybBean.PutFileInfo;
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
import java.util.Map;

/**
 * 异步httpclient回调对象---POST文件
 * 在get文件成功后开始执行post文件操作
 * post文件成功时,获取到文件id加入到message中,开始执行post消息操作,
 * 为此需要message(kafka获取)以及post消息url(配置文件获取),
 * 还需要User-Agent以及X-Tag用于验证
 * <p>
 * post失败时,重试三次
 */
public class AsyncHttpClientPostFileCallback implements FutureCallback<HttpResponse> {
    private static Logger LOG = Logger.getLogger(AsyncHttpClientPostFileCallback.class);

    private DataCenterLoad dclAsyncPost;
    private KafkaDB kafkaDBAsyncPost;

    private String postFileUrl;
    private String postMsgUrl;
    private byte[] resultIs;//get到的文件流,用于post文件失败时重试-字节数组版本
    private ConfigInfo configInfo;//1.存储post文件,信息url(配置文件获取);2.存储入库前验证文件,信息url(配置文件获取);3.存储文件,信息SessionCookie(请求总线获取);4.存储msg作为avro入库所需的topicName,userAgent,xTag,batchSize

    private String sendMsg;//用于post成功后补全id发往zx或者post失败发往kafka

    private PostFileResBody postFileResBody;//存储post文件成功后返回的响应信息,内部包含文件id,key名为id
    private int count;

    private PutFileInfo putFileInfo;//用于存放发送文件时的需要携带的相关信息

    public AsyncHttpClientPostFileCallback(ConfigInfo configInfo, String sendMsg, int count) {
        this.configInfo = configInfo;
        this.postFileUrl = configInfo.getPostFileUrl();
        this.postMsgUrl = configInfo.getPostMsgUrl();
        this.sendMsg = sendMsg;//存放对应于url的数据
        this.count = count;

        //初始化入库程序
        dclAsyncPost = new DataCenterLoad();
        kafkaDBAsyncPost = KafkaDB.getInstance();
    }

    //暂未使用
    public AsyncHttpClientPostFileCallback(String postFileUrl, String postMsgUrl, int count) {
        this.postFileUrl = postFileUrl;
        this.postMsgUrl = postMsgUrl;
        this.count = count;

        //初始化入库程序
        dclAsyncPost = new DataCenterLoad();
        kafkaDBAsyncPost = KafkaDB.getInstance();
    }

    //暂未使用
    public AsyncHttpClientPostFileCallback(String postFileUrl, String postMsgUrl, byte[] resultIs, int count) {
        this.postFileUrl = postFileUrl;
        this.postMsgUrl = postMsgUrl;
        this.resultIs = resultIs;
        this.count = count;

        //初始化入库程序
        dclAsyncPost = new DataCenterLoad();
        kafkaDBAsyncPost = KafkaDB.getInstance();
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

    public byte[] getResultIs() {
        return resultIs;
    }

    public void setResultIs(byte[] resultIs) {
        this.resultIs = resultIs;
    }

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

    @Override
    public String toString() {
        return "AsyncHttpClientPostFileCallback{" +
                "postFileUrl='" + postFileUrl + '\'' +
                ", postMsgUrl='" + postMsgUrl + '\'' +
                ", configInfo=" + configInfo +
                ", sendMsg='" + sendMsg + '\'' +
                ", postFileResBody=" + postFileResBody +
                ", count=" + count +
                '}';
    }

    /**
     * 请求完成后调用该函数
     */
    @Override
    public void completed(HttpResponse response) {
        try {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                LOG.info("AsyncHttpClientPostFileCallback completed,post file statuscode is:{" + statusCode + "},now start to send message to zx.");
                //post文件成功后可以获取文件id
                String postResBody = getHttpContent(response);
                System.out.println("Post File to zx resBody====>" + postResBody);
                if (StringUtils.isNotBlank(postResBody)) {
                    this.postFileResBody = JSONObject.parseObject(postResBody, PostFileResBody.class);
                    String fileId = postFileResBody.getMsg();//获取文件id----新---20191115---返回体格式：{"code": "200", "msg": "2019111507094164188567821505", "data": "不需要额外数据"}
                    AvroMonitorTimerTask.fileSuccessSum++;//成功获取到ID的文件计数,即成功推送的文件数

                    AvroMonitorTimerTask.fileBytesSuccessSum += resultIs.length;//成功获取到ID的文件计字节数,即成功推送的文件字节数

                    this.sendMsg = PutIdOnMsgByTopicUtils.putIdOnMsgByTopicName(configInfo.getTopicName(), sendMsg, fileId);//补全数据

                    AvroMonitorTimerTask.msgReadyPostSum++;//一个文件对应一条消息
                    //往总线发送
                    ProResBody proResBody = dclAsyncPost.avroDataLoad(configInfo.getPostMsgUrl(), configInfo.getTopicName(), sendMsg, configInfo.getBatchSize(), configInfo.getUserAgent(), configInfo.getMsgSessionCookie());
                    LOG.info("Send message with fileId to zx over,this responseBody is===>" + proResBody.toString());
                } else {
                    LOG.warn("AsyncHttpClientPostFileCallback post file success but postResBody(response body) is null.");
                }
            } else if (statusCode == 403) {//空文件,不再重试发送
                LOG.error("AsyncHttpClientPostFileCallback post zxFile statusCode is 403 so get the fileIs but this minio file is empty.This message is===>>>" + sendMsg + "<<<===");
            } else {
                RetryAsyncHttpClientPostFileFailed(statusCode);
            }
        } catch (Exception e) {
            LOG.error("AsyncHttpClientPostFileCallback post zxFile or send finalMsg is error .This message is===>>>" + sendMsg + "<<<===");
            e.printStackTrace();
        }

        HttpClientUtils.closeQuietly(response);
    }

    /**
     * 请求取消后调用该函数
     */
    @Override
    public void cancelled() {
        LOG.info("AsyncHttpClientPostFileCallback is cancelled... ...");
    }

    /**
     * 请求失败后调用该函数
     */
    @Override
    public void failed(Exception e) {
        //重试
        RetryAsyncHttpClientPostFileFailed(e);
    }

    /**
     * failed被调用时的重试
     *
     * @param e
     */
    private void RetryAsyncHttpClientPostFileFailed(Exception e) {
        try {
            count++;
            if (count > 1) {
                LOG.warn("AsyncHttpClientPostFileCallback post zxFile is failed,retry count=" + count);
            }
            if (count > 4) {
                LOG.error("AsyncHttpClientPostFileCallback post zxFile is failed and already retry 3 times.error===>>>" + e + "<<<===.This message is===>>>" + sendMsg + "<<<===");
            } else {
                if (configInfo != null && StringUtils.isNotBlank(sendMsg) && resultIs != null) {
                    //重试发送文件
                    AsyncHttpClientPostFileCallback asyncHttpClientPostFileCallback = new AsyncHttpClientPostFileCallback(configInfo, sendMsg, count);//注意此处count不再为0
                    //重试时也是以之前存储的流的形式发往总线
                    HttpClientUtil.httpAsyncPostFileToZx(configInfo.getPostFileUrl(), resultIs, asyncHttpClientPostFileCallback);
                }
            }
        } catch (Exception e2) {
            LOG.error("AsyncHttpClientGetFileCallback retry is error===>>>" + e2);
        }
    }

    /**
     * 流不为空但文件异常情况下的重试
     *
     * @param statusCode
     */
    private void RetryAsyncHttpClientPostFileFailed(int statusCode) {
        try {
            count++;
            if (count > 1) {
                LOG.warn("AsyncHttpClientPostFileCallback post zxFile statusCode is abnormal,retry count=" + count);
            }
            if (count > 4) {
                LOG.error("AsyncHttpClientPostFileCallback post zxFile statusCode is abnormal and already retry 3 times.statusCode is{" + statusCode + "}.This message is===>>>" + sendMsg + "<<<===");
            } else {
                if (configInfo != null && StringUtils.isNotBlank(sendMsg) && resultIs != null) {
                    //重试发送文件
                    AsyncHttpClientPostFileCallback asyncHttpClientPostFileCallback = new AsyncHttpClientPostFileCallback(configInfo, sendMsg, count);//注意此处count不再为0
                    //重试时也是以之前存储的流的形式发往总线
                    HttpClientUtil.httpAsyncPostFileToZx(configInfo.getPostFileUrl(), resultIs, asyncHttpClientPostFileCallback);
                }
            }
        } catch (Exception e2) {
            LOG.error("AsyncHttpClientGetFileCallback retry is error===>>>" + e2);
        }
    }

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







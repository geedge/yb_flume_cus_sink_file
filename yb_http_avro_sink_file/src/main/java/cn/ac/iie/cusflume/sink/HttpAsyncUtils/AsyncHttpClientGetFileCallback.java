package cn.ac.iie.cusflume.sink.HttpAsyncUtils;

import cn.ac.iie.cusflume.sink.avroUtils.AvroMonitorTimerTask;
import cn.ac.iie.cusflume.sink.avroUtils.DataCenterLoad;
import cn.ac.iie.cusflume.sink.bean.configBean.ConfigInfo;
import cn.ac.iie.cusflume.sink.daoUtils.KafkaDB;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
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
import java.util.List;
import java.util.Map;

/**
 * 异步httpclient回调对象---GET文件
 * 执行成功,开始将get到的文件post至zx
 * 执行失败,重试3次
 */
public class AsyncHttpClientGetFileCallback implements FutureCallback<HttpResponse> {
    private static Logger LOG = Logger.getLogger(AsyncHttpClientGetFileCallback.class);

    private KafkaDB kafkaDBAsyncGet;

    private String postFileUrl;
    private String postMsgUrl;
    private String getFileUrl;
    private AsyncHttpClientPostFileCallback asyncHttpClientPostFileCallback;
    private ConfigInfo configInfo;
    private String sendMsg;//用于get文件失败后发往kafka
    private int count;

    //暂未使用
    public AsyncHttpClientGetFileCallback(String postFileUrl, String postMsgUrl, String getFileUrl, AsyncHttpClientPostFileCallback asyncHttpClientPostFileCallback, int count) {
        this.postFileUrl = postFileUrl;
        this.postMsgUrl = postMsgUrl;
        this.getFileUrl = getFileUrl;
        this.asyncHttpClientPostFileCallback = asyncHttpClientPostFileCallback;
        this.count = count;//建议初始为0
    }

    public AsyncHttpClientGetFileCallback(ConfigInfo configInfo, String getFileUrl, String sendMsg, AsyncHttpClientPostFileCallback asyncHttpClientPostFileCallback, int count) {
        this.configInfo = configInfo;
        this.postFileUrl = configInfo.getPostFileUrl();//通过configInfo赋值
        this.postMsgUrl = configInfo.getSingleProduceUrl();//通过configInfo赋值
        this.getFileUrl = getFileUrl;
        this.sendMsg = sendMsg;
        this.asyncHttpClientPostFileCallback = asyncHttpClientPostFileCallback;
        this.count = count;//建议初始为0

        //初始化入kafka程序
        kafkaDBAsyncGet = KafkaDB.getInstance();
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

    public String getGetFileUrl() {
        return getFileUrl;
    }

    public void setGetFileUrl(String getFileUrl) {
        this.getFileUrl = getFileUrl;
    }

    public AsyncHttpClientPostFileCallback getAsyncHttpClientPostFileCallback() {
        return asyncHttpClientPostFileCallback;
    }

    public void setAsyncHttpClientPostFileCallback(AsyncHttpClientPostFileCallback asyncHttpClientPostFileCallback) {
        this.asyncHttpClientPostFileCallback = asyncHttpClientPostFileCallback;
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

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "AsyncHttpClientGetFileCallback{" +
                "postFileUrl='" + postFileUrl + '\'' +
                ", postMsgUrl='" + postMsgUrl + '\'' +
                ", getFileUrl='" + getFileUrl + '\'' +
                ", asyncHttpClientPostFileCallback=" + asyncHttpClientPostFileCallback +
                ", configInfo=" + configInfo +
                ", sendMsg='" + sendMsg + '\'' +
                ", count=" + count +
                '}';
    }

    /**
     * 请求完成后调用该函数
     */
    @Override
    public void completed(HttpResponse response) {
        try {
            byte[] result = IOUtils.toByteArray(response.getEntity().getContent());
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                asyncHttpClientPostFileCallback.setResultIs(result);//添加获取的文件流,用于post文件失败时重试使用
                LOG.info("AsyncHttpClientGetFileCallback completed,response status:{" + statusCode + "},get file success,post file to zx next.");

                AvroMonitorTimerTask.fileReadyPostSum++;//文件获取流成功才会开始推送文件,计个数
                AvroMonitorTimerTask.fileBytesReadyPostSum += result.length;//文件获取流成功才会开始推送文件,计字节数

                //文件获取成功后以流的形式开始发往总线
                HttpClientUtil.httpAsyncPostFileToZx(configInfo.getPostFileUrl(), result, asyncHttpClientPostFileCallback);//失败时进入到的这个asyncHttpClientPostFileCallback内部是有文件流数据的
            } else if (statusCode == 404) {
                //文件未存在,重试
                retryFor404();
            } else {
                LOG.info("AsyncHttpClientGetFileCallback getFile failed,response status:{" + statusCode + "},this msg is===>" + sendMsg + "<===");
            }

            HttpClientUtils.closeQuietly(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 由于404引起的重试,大概率是因为文件未完全生成
     */
    private void retryFor404() {
        try {
            count++;
            if (count > 1) {
                LOG.warn("AsyncHttpClientGetFileCallback getFile is failed,statusCode is 404,retry count=" + count);
            }
            if (count > 4) {
                LOG.error("AsyncHttpClientGetFileCallback getFile is failed and already retry 3 times,statusCode is 404,This message is===>>>" + sendMsg + "<<<===");
            } else {
                Map map = JSONObject.parseObject(sendMsg, Map.class);
                int found_time = (int) map.get("found_time");
                long now_time = System.currentTimeMillis() / 1000;
                long timeDiffer = now_time - found_time;
                if (timeDiffer >= 20) {
                    AsyncHttpClientGetFileCallback asyncHttpClientGetFileCallback = new AsyncHttpClientGetFileCallback(configInfo, getFileUrl, sendMsg, asyncHttpClientPostFileCallback, count);//注意此处count不再为0
                    HttpClientUtil.getFileAndPostFile(getFileUrl, asyncHttpClientGetFileCallback);
                } else {
                    Thread.sleep((20 - timeDiffer) * 1000);
                    AsyncHttpClientGetFileCallback asyncHttpClientGetFileCallback = new AsyncHttpClientGetFileCallback(configInfo, getFileUrl, sendMsg, asyncHttpClientPostFileCallback, count);//注意此处count不再为0
                    HttpClientUtil.getFileAndPostFile(getFileUrl, asyncHttpClientGetFileCallback);
                }
            }
        } catch (Exception e2) {
            LOG.error("AsyncHttpClientGetFileCallback retryFor404 getFile retry is error===>>>" + e2);
        }
    }

    /**
     * 请求取消后调用该函数
     */
    @Override
    public void cancelled() {
        LOG.warn("request is cancelled...");
    }

    /**
     * 请求失败后调用该函数
     */
    @Override
    public void failed(Exception e) {
        retryForFailed(e);
    }

    /**
     * 由于请求失败引起的重试
     *
     * @param e
     */
    private void retryForFailed(Exception e) {
        try {
            count++;
            if (count > 1) {
                LOG.warn("AsyncHttpClientGetFileCallback getFile is failed,retry count=" + count);
            }
            if (count > 4) {
                LOG.error("AsyncHttpClientGetFileCallback getFile is failed and already retry 3 times.error===>>>" + e + "<<<===.This message is===>>>" + sendMsg + "<<<===");
            } else {
                Map map = JSONObject.parseObject(sendMsg, Map.class);
                int found_time = (int) map.get("found_time");
                long now_time = System.currentTimeMillis() / 1000;
                long timeDiffer = now_time - found_time;
                if (timeDiffer >= 20) {
                    AsyncHttpClientGetFileCallback asyncHttpClientGetFileCallback = new AsyncHttpClientGetFileCallback(configInfo, getFileUrl, sendMsg, asyncHttpClientPostFileCallback, count);//注意此处count不再为0
                    HttpClientUtil.getFileAndPostFile(getFileUrl, asyncHttpClientGetFileCallback);
                } else {
                    Thread.sleep((20 - timeDiffer) * 1000);
                    AsyncHttpClientGetFileCallback asyncHttpClientGetFileCallback = new AsyncHttpClientGetFileCallback(configInfo, getFileUrl, sendMsg, asyncHttpClientPostFileCallback, count);//注意此处count不再为0
                    HttpClientUtil.getFileAndPostFile(getFileUrl, asyncHttpClientGetFileCallback);
                }
            }
        } catch (Exception e2) {
            LOG.error("AsyncHttpClientGetFileCallback retryForFailed getFile retry is error===>>>" + e2);
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

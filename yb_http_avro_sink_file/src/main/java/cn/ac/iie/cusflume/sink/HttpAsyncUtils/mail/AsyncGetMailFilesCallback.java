package cn.ac.iie.cusflume.sink.HttpAsyncUtils.mail;

import cn.ac.iie.cusflume.sink.HttpAsyncUtils.HttpClientUtil;
import cn.ac.iie.cusflume.sink.avroUtils.AvroMonitorTimerTask;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 用于处理 NTC_COLLECT_MAIL_LOG
 * 异步httpclient回调对象---GET文件
 * 执行成功,开始将get到的文件post至zx
 * 执行失败,重试3次
 */
public class AsyncGetMailFilesCallback implements FutureCallback<HttpResponse> {
    private static Logger LOG = Logger.getLogger(AsyncGetMailFilesCallback.class);

    private KafkaDB kafkaDBAsyncGet;

    private String postFileUrl;
    private String postMsgUrl;
    private String getFileUrl;//此处初始用于存放eml_file_url,多文件的url存放在attachmentsUrl,先处理eml_file_url成功后再处理attachmentsUrl
    private AsyncPostMailFilesCallback asyncPostMailFilesCallback;
    private ConfigInfo configInfo;
    private String sendMsg;//用于get文件失败后发往kafka
    private int count;//重试次数,初始为0

    //为MAIL新增
    private int dealUrlCount;//已处理计数,初始为0
    private int urlCount;//需要处理的总数
    private LinkedList<String> attachmentsUrl;//需要处理的attachmentsUrl

    //初始调用时count=0,dealUrlCount=0,urlCount=1+attachmentsUrl.size()
    //后续调用count=0,dealUrlCount=0按照情况增加,urlCount不变
    public AsyncGetMailFilesCallback(ConfigInfo configInfo, String getFileUrl, String sendMsg, AsyncPostMailFilesCallback asyncPostMailFilesCallback, int count, int urlCount, int dealUrlCount, LinkedList<String> attachmentsUrl) {
        this.configInfo = configInfo;
        this.postFileUrl = configInfo.getPostFileUrl();//通过configInfo赋值
        this.postMsgUrl = configInfo.getSingleProduceUrl();//通过configInfo赋值
        this.getFileUrl = getFileUrl;//初次存储的是eml_file_url,后续每次存储的都是当次需要请求的文件路径
        this.sendMsg = sendMsg;
        this.asyncPostMailFilesCallback = asyncPostMailFilesCallback;
        this.count = count;//建议初始为0
        this.urlCount = urlCount;
        this.dealUrlCount = dealUrlCount;//建议初始为0
        this.attachmentsUrl = attachmentsUrl;

//        //初始化入kafka程序
//        kafkaDBAsyncGet = KafkaDB.getInstance();
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

    public AsyncPostMailFilesCallback getAsyncPostMailFilesCallback() {
        return asyncPostMailFilesCallback;
    }

    public void setAsyncPostMailFilesCallback(AsyncPostMailFilesCallback asyncPostMailFilesCallback) {
        this.asyncPostMailFilesCallback = asyncPostMailFilesCallback;
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

    public int getDealUrlCount() {
        return dealUrlCount;
    }

    public void setDealUrlCount(int dealUrlCount) {
        this.dealUrlCount = dealUrlCount;
    }

    public int getUrlCount() {
        return urlCount;
    }

    public void setUrlCount(int urlCount) {
        this.urlCount = urlCount;
    }

    public LinkedList<String> getAttachmentsUrl() {
        return attachmentsUrl;
    }

    public void setAttachmentsUrl(LinkedList<String> attachmentsUrl) {
        this.attachmentsUrl = attachmentsUrl;
    }

    @Override
    public String toString() {
        return "AsyncGetMailFilesCallback{" +
                "kafkaDBAsyncGet=" + kafkaDBAsyncGet +
                ", postFileUrl='" + postFileUrl + '\'' +
                ", postMsgUrl='" + postMsgUrl + '\'' +
                ", getFileUrl='" + getFileUrl + '\'' +
                ", asyncPostMailFilesCallback=" + asyncPostMailFilesCallback +
                ", configInfo=" + configInfo +
                ", sendMsg='" + sendMsg + '\'' +
                ", count=" + count +
                ", dealUrlCount=" + dealUrlCount +
                ", urlCount=" + urlCount +
                ", attachmentsUrl=" + attachmentsUrl +
                '}';
    }

    /**
     * 请求完成后调用该函数
     */
    @Override
    public void completed(HttpResponse response) {
        try {
//            LOG.warn("response:{}", getHttpContent(response));
            //拉取文件成功后,将文件转为流
//            InputStream result = IOUtils.toBufferedInputStream(response.getEntity().getContent());
            //新版使用字节数组
            byte[] result = IOUtils.toByteArray(response.getEntity().getContent());
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                dealUrlCount++;
//            asyncPostMailFilesCallback.setResultIs(result);//添加获取的文件流,用于post文件失败时重试使用--------老版
                if (dealUrlCount >= 2) {//说明拉取的是attachments的file
//                    LinkedList<InputStream> attachmentsResultIsList = asyncPostMailFilesCallback.getAttachmentsResultIsList();
                    LinkedList<byte[]> attachmentsResultIsList = asyncPostMailFilesCallback.getAttachmentsResultIsList();
                    if (attachmentsResultIsList != null && attachmentsResultIsList.size() > 0) {//说明已经存有其他流
                        attachmentsResultIsList.add(result);
                    } else {//说明还没有存储流,new一个新的存放流
//                        attachmentsResultIsList = new LinkedList<InputStream>();
                        attachmentsResultIsList = new LinkedList<byte[]>();
                        attachmentsResultIsList.add(result);
                    }
                    asyncPostMailFilesCallback.setAttachmentsResultIsList(attachmentsResultIsList);
                    LOG.info("AsyncGetMailFilesCallback getAttachmentsFile completed,response status:{" + statusCode + "}.");
                } else if (dealUrlCount == 1) {//说明拉取的是eml_file的流
                    asyncPostMailFilesCallback.setEmlFileResultIs(result);
                    LOG.info("AsyncGetMailFilesCallback getEmlFile completed,response status:{" + statusCode + "}.");
                }

                //开始发送文件或者获取余下的文件
                if (dealUrlCount == urlCount) {
                    //文件全部获取成功,以流的形式开始发往总线,首先发送eml_file的流,后续的attachments的流进入到asyncPostMailFilesCallback内部进行发送并获取返回的id
//                    InputStream emlFileResultIs = asyncPostMailFilesCallback.getEmlFileResultIs();
                    byte[] emlFileResultIs = asyncPostMailFilesCallback.getEmlFileResultIs();
                    if (emlFileResultIs != null) {
                        //文件全部获取成功时,流才表示readyPostFile
                        AvroMonitorTimerTask.fileReadyPostSum++;//加上Eml_file
                        AvroMonitorTimerTask.fileReadyPostSum += asyncPostMailFilesCallback.getAttachmentsResultIsList().size();//加上attachments_file

                        //文件全部获取成功时,获取成功的流才计算ReadyPostFileBytes
//                        ByteArrayOutputStream byteArrayOutputStream = AvroMonitorTimerTask.getByteArrayOutputStream(emlFileResultIs);
//                        emlFileResultIs = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());//重新填充被消耗的emlFileResultIs

                        AvroMonitorTimerTask.fileBytesReadyPostSum += emlFileResultIs.length;//加上Eml_file字节数

//                        LinkedList<ByteArrayOutputStream> byteArrayOutputStreamsList = AvroMonitorTimerTask.getByteArrayOutputStream(asyncPostMailFilesCallback.getAttachmentsResultIsList());
//
//                        //重新填充被消耗的attachmentsResultIsList
//                        asyncPostMailFilesCallback.getAttachmentsResultIsList().clear();
//                        for (int i = 0; i < byteArrayOutputStreamsList.size(); i++) {
//                            asyncPostMailFilesCallback.getAttachmentsResultIsList().add(new ByteArrayInputStream(byteArrayOutputStreamsList.get(i).toByteArray()));
//                        }

                        for (int i = 0; i < asyncPostMailFilesCallback.getAttachmentsResultIsList().size(); i++) {
                            AvroMonitorTimerTask.fileBytesReadyPostSum += asyncPostMailFilesCallback.getAttachmentsResultIsList().get(i).length;//加上attachments_file字节数
                        }

                        HttpClientUtil.httpAsyncPostFileToZx(configInfo.getPostFileUrl(), emlFileResultIs, asyncPostMailFilesCallback);//失败时进入到的这个asyncPostMailFilesCallback内部是有文件流数据的
                    } else {
                        LOG.error("dealUrlCount == urlCount but emlFileResultIs is null,message is===>>>" + sendMsg);
                    }
                } else if (dealUrlCount < urlCount) {
                    //说明文件没有全部获取成功,继续获取
                    String attachmentUrl = attachmentsUrl.get(dealUrlCount - 1);//获取(dealUrlCount - 1)索引位置的url
                    AsyncGetMailFilesCallback asyncGetMailFilesCallback = new AsyncGetMailFilesCallback(configInfo, attachmentUrl, sendMsg, asyncPostMailFilesCallback, count, urlCount, dealUrlCount, attachmentsUrl);//asyncPostMailFilesCallback也包含configInfo
                    HttpClientUtil.httpAsyncGetFile(attachmentUrl, asyncGetMailFilesCallback);//回调时都是最新的数据
                }
            } else if (statusCode == 404) {
                //404-重试,注意此次重试大概率由于文件未生成
                retryFor404();
            } else {
//                LOG.info("AsyncGetMailFilesCallback getFiles failed,response status:{" + statusCode + "},this msg is===>" + sendMsg + "<===");
                LOG.info("AsyncGetMailFilesCallback getFiles failed,response status:{" + statusCode + "},dealUrlCount is {" + dealUrlCount + "},this msg is===>" + sendMsg + "<===");
            }

            HttpClientUtils.closeQuietly(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 由于404引起的重试
     */
    private void retryFor404() {
        try {
            count++;
            if (count > 1) {
                LOG.warn("AsyncGetMailFilesCallback getFile is failed,retry count=" + count);
                LOG.info("Now AsyncGetMailFilesCallback dealUrlCount is===>{" + dealUrlCount + "}");
            }
            if (count > 4) {
                LOG.error("AsyncGetMailFilesCallback getFile is failed and already retry 3 times.This failed message is===>>>" + sendMsg + "<<<===");
            } else {
                Map map = JSONObject.parseObject(sendMsg, Map.class);
                int found_time = (int) map.get("found_time");
                long now_time = System.currentTimeMillis() / 1000;
                long timeDiffer = now_time - found_time;
                if (timeDiffer >= 20) {//直接重试
                    //上一次请求失败的url存储在getFileUrl中
                    AsyncGetMailFilesCallback asyncHttpClientGetFileCallback = new AsyncGetMailFilesCallback(configInfo, getFileUrl, sendMsg, asyncPostMailFilesCallback, count, urlCount, dealUrlCount, attachmentsUrl);//注意此处count不再为0
                    HttpClientUtil.httpAsyncGetFile(getFileUrl, asyncHttpClientGetFileCallback);
                } else {//sleep后重试
                    Thread.sleep((20 - timeDiffer) * 1000);

                    //上一次请求失败的url存储在getFileUrl中
                    AsyncGetMailFilesCallback asyncHttpClientGetFileCallback = new AsyncGetMailFilesCallback(configInfo, getFileUrl, sendMsg, asyncPostMailFilesCallback, count, urlCount, dealUrlCount, attachmentsUrl);//注意此处count不再为0
                    HttpClientUtil.httpAsyncGetFile(getFileUrl, asyncHttpClientGetFileCallback);
                }
            }
        } catch (Exception e2) {
            LOG.error("AsyncGetMailFilesCallback getFile retry is error===>>>" + e2);
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
//        LOG.warn("AsyncGetMailFilesCallback getFile is failed,message is===>>>" + sendMsg + "<<<===");
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
                LOG.warn("AsyncGetMailFilesCallback getFile is failed,retry count=" + count);
                LOG.info("Now AsyncGetMailFilesCallback dealUrlCount is===>{" + dealUrlCount + "}");
            }
            if (count > 4) {
                LOG.error("AsyncGetMailFilesCallback getFile is failed and already retry 3 times.error===>>>" + e + "<<<===.This failed message is===>>>" + sendMsg + "<<<===");
//                if (StringUtils.isNotBlank(configInfo.getTopicName()) && StringUtils.isNotBlank(sendMsg)) {
//                    //将数据发往kafka,用于后续重试
////                    kafkaDBAsyncGet.postFailMsgToKafka(configInfo.getTopicName(), sendMsg);
//                    LOG.error("Send to Kafka(Temp)...test!!!");
//                }
            } else {
                Map map = JSONObject.parseObject(sendMsg, Map.class);
                int found_time = (int) map.get("found_time");
                long now_time = System.currentTimeMillis() / 1000;
                long timeDiffer = now_time - found_time;
                if (timeDiffer >= 20) {//直接重试
                    //上一次请求失败的url存储在getFileUrl中
                    AsyncGetMailFilesCallback asyncHttpClientGetFileCallback = new AsyncGetMailFilesCallback(configInfo, getFileUrl, sendMsg, asyncPostMailFilesCallback, count, urlCount, dealUrlCount, attachmentsUrl);//注意此处count不再为0
                    HttpClientUtil.httpAsyncGetFile(getFileUrl, asyncHttpClientGetFileCallback);
                } else {//sleep后重试
                    Thread.sleep((20 - timeDiffer) * 1000);

                    //上一次请求失败的url存储在getFileUrl中
                    AsyncGetMailFilesCallback asyncHttpClientGetFileCallback = new AsyncGetMailFilesCallback(configInfo, getFileUrl, sendMsg, asyncPostMailFilesCallback, count, urlCount, dealUrlCount, attachmentsUrl);//注意此处count不再为0
                    HttpClientUtil.httpAsyncGetFile(getFileUrl, asyncHttpClientGetFileCallback);
                }
            }
        } catch (Exception e2) {
            LOG.error("AsyncGetMailFilesCallback getFile retry is error===>>>" + e2);
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

//    @Override
//    public Object call() throws Exception {
//        return asyncHttpClientPostFileCallback.getPostFileResBody();
//    }
}

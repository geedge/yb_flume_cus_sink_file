package cn.ac.iie.cusflume.sink.HttpAsyncUtils.msgCallBack;

import cn.ac.iie.cusflume.sink.HttpAsyncUtils.HttpClientUtil;
import cn.ac.iie.cusflume.sink.YbHttpAvroSinkFile;
import cn.ac.iie.cusflume.sink.avroUtils.AvroMonitorTimerTask;
import cn.ac.iie.cusflume.sink.bean.redirectBean.ResRedirBody;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * 被回调的对象，给异步的httpclient使用
 */
public class AsyncBatchMsgCallBack implements FutureCallback<HttpResponse> {
    private static Logger logger = Logger.getLogger(AsyncBatchMsgCallBack.class);

    private String batchProduceUrl;
    private String topicName;
    private  String checksum;
    private int count;
    private long postTime;

    public AsyncBatchMsgCallBack(String batchProduceUrl, String topicName,
                                    String checksum, int count, long postTime) {
        this.batchProduceUrl = batchProduceUrl;
        this.topicName = topicName;
        this.count = count;
        this.postTime = postTime;
        this.checksum = checksum;
    }


    public String getBatchProduceUrl() {
        return batchProduceUrl;
    }

    public void setBatchProduceUrl(String batchProduceUrl) {
        this.batchProduceUrl = batchProduceUrl;
    }

    public String getChecksum() {
        return checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getPostTime() {
        return postTime;
    }

    public void setPostTime(long postTime) {
        this.postTime = postTime;
    }

    /**
     * 请求完成后调用该函数
     */
    @Override
    public void completed(HttpResponse response) {
        try {
            int statusCode = response.getStatusLine().getStatusCode();

            HttpEntity entity = response.getEntity();

            String result = null;
            if (entity != null) {
                result = EntityUtils.toString(entity, "UTF-8");
            }

            /**
             * 不直接进行对象转换，除非数据加载不成功
             */
            Map map = JSONObject.parseObject(result, Map.class);

            int resRedirBodyCode = 0;

            if (map != null) {
                resRedirBodyCode = (int) map.get("code");
            }

            /**
             * 20200818-接口细化响应码
             */
            if (statusCode == 200 && resRedirBodyCode == 200) {
                logger.info(
                        "数据加载成功,返回码: " + statusCode +
                        "生产数据checksum==>" + checksum + "<==," +
                        "服务端响应时间(ms)==>" + (System.currentTimeMillis() - postTime) + "<==," +
                        "数据加载成功,返回码: " + statusCode);
                AvroMonitorTimerTask.addSuccessNum(count);
            } else {
                AvroMonitorTimerTask.addFailedNum(count);
                switch (resRedirBodyCode) {
                    case 300:
                        logger.error("AsyncPostBatchMsgCallBack==>重定向响应体-redirect-ret==>" + result
                                + "总线批量发送URL==>" + batchProduceUrl
                                + "服务端响应时间(ms)==>" + (System.currentTimeMillis() - postTime) + "<==,"
                                + "<==,Status Code:" + statusCode + ",resRedirBodyCode:300,当前服务节点负载过高,将向其他通信地址发送请求.");

                        //若不包含对应字段，则不进行对象转换，减少报错
                        if (result.contains("redirect")) {
                            ResRedirBody resRedirBody = JSONObject.parseObject(result, ResRedirBody.class);
                            String redirectUrl = resRedirBody.getData().getRedirect();
                            if (StringUtils.isNotBlank(redirectUrl)) {
                                YbHttpAvroSinkFile.changeUrl(redirectUrl);
                            }
                        } else {
                            logger.error("AsyncPostBatchMsgCallBack==>服务端响应体中ResRedirBody.data.redirect字段不存在或格式不正确!!!");
                        }

                        break;
                    case 301:
                        logger.error("AsyncPostBatchMsgCallBack==>Status Code:" + statusCode
                                + "总线批量发送URL==>" + batchProduceUrl
                                + "服务端响应时间(ms)==>" + (System.currentTimeMillis() - postTime) + "<==,"
                                + ",resRedirBodyCode:301,当前所有服务端节点均负载过高,暂无可用资源,请等待.");
                        break;
                    case 410:
                        logger.error("AsyncPostBatchMsgCallBack==>Status Code:" + statusCode
                                + "总线批量发送URL==>" + batchProduceUrl
                                + "服务端响应时间(ms)==>" + (System.currentTimeMillis() - postTime) + "<==,"
                                + ",resRedirBodyCode:410,Cookie已过期或Cookie错误,将开始更新Cookie.");
                        YbHttpAvroSinkFile.updateCookie();
                        break;
                    case 500:
                        logger.error("AsyncPostBatchMsgCallBack==>Status Code:" + statusCode
                                + "总线批量发送URL==>" + batchProduceUrl
                                + "服务端响应时间(ms)==>" + (System.currentTimeMillis() - postTime) + "<==,"
                                + ",resRedirBodyCode:500,处理请求过程出现系统错误.");
                        YbHttpAvroSinkFile.updateCookie();
                        break;
                    default:
                        logger.error("AsyncPostBatchMsgCallBack==>数据加载失败,响应体:" + result
                                + "总线批量发送URL==>" + batchProduceUrl
                                + "服务端响应时间(ms)==>" + (System.currentTimeMillis() - postTime) + "<==,"
                                + "---Status Code:" + statusCode + "---resRedirBodyCode:" + resRedirBodyCode);
                        break;
                }
            }

            if (entity != null) {
                try {
                    EntityUtils.consume(entity);
                } catch (final IOException ex) {
                    logger.error("IOException : " + ex);
                }
            }

        } catch (Exception e) {
            logger.error("AsyncPostBatchMsgCallBack Get response from ZX is error===>>>"
                    + e.getMessage());
            e.printStackTrace();
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
    }

    /**
     * 请求取消后调用该函数
     */
    @Override
    public void cancelled() {
        logger.error("AsyncPostMagCallBack Request is cancelled");
    }

    /**
     * 请求失败后调用该函数
     */
    @Override
    public void failed(Exception e) {
        logger.error("AsyncPostMagCallBack Request is Failed,This Failed data checksum is ==>"+ checksum);
    }

}







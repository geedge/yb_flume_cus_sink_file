package cn.ac.iie.cusflume.sink.HttpAsyncUtils.msgCallBack;

import cn.ac.iie.cusflume.sink.HttpAsyncUtils.HttpClientUtil;
import cn.ac.iie.cusflume.sink.YbHttpAvroSinkFile;
import cn.ac.iie.cusflume.sink.avroUtils.AvroMonitorTimerTask;
import cn.ac.iie.cusflume.sink.avroUtils.MD5Utils;
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
public class AsyncSingleMsgCallBack implements FutureCallback<HttpResponse> {
    private static Logger logger = Logger.getLogger(AsyncSingleMsgCallBack.class);

    private String postMsgUrl;
    private String topicName;
    private String dataJson;
    private String userAgent;
    private String msgSessionCookie;
    private int count;
    private long postTime;

    public AsyncSingleMsgCallBack(String postMsgUrl, String topicName, String dataJson, String userAgent, String msgSessionCookie, int count, long postTime) {
        this.postMsgUrl = postMsgUrl;
        this.topicName = topicName;
        this.dataJson = dataJson;
        this.userAgent = userAgent;
        this.msgSessionCookie = msgSessionCookie;
        this.count = count;
        this.postTime = postTime;
    }

    public String getPostMsgUrl() {
        return postMsgUrl;
    }

    public void setPostMsgUrl(String postMsgUrl) {
        this.postMsgUrl = postMsgUrl;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getDataJson() {
        return dataJson;
    }

    public void setDataJson(String dataJson) {
        this.dataJson = dataJson;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getMsgSessionCookie() {
        return msgSessionCookie;
    }

    public void setMsgSessionCookie(String msgSessionCookie) {
        this.msgSessionCookie = msgSessionCookie;
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
            String ret = null;
            if (entity != null) {
                ret = EntityUtils.toString(entity, "UTF-8");
            }
            logger.info("返回的生产原始响应体String数据为:" + ret);

            /**
             * 不直接进行对象转换，除非数据加载不成功
             */
            Map map = JSONObject.parseObject(ret, Map.class);
            int resRedirBodyCode = 0;
            if (map != null) {
                resRedirBodyCode = (int) map.get("code");
            }

            /**
             * 20200818-接口细化响应码
             */
            if (statusCode == 200 && resRedirBodyCode == 200) {
                logger.info("数据加载成功,返回码: " + statusCode);
                logger.debug("生产数据==>" + dataJson + "<==," +
                        "生产数据checksum==>" + MD5Utils.md5Encode(dataJson) + "<==," +
                        "服务端响应时间(ms)==>" + (System.currentTimeMillis() - postTime) + "<==," +
                        "数据加载成功,返回码: " + statusCode);

                AvroMonitorTimerTask.addSuccessNum(count);
            } else {
                AvroMonitorTimerTask.addFailedNum(count);
                switch (resRedirBodyCode) {
                    case 300:
                        logger.error("AsyncSingleMsgCallBack==>重定向响应体-redirect-ret==>" + ret
                                + "服务端响应时间(ms)==>" + (System.currentTimeMillis() - postTime) + "<==,"
                                + "<==,Status Code:" + statusCode + ",resRedirBodyCode:300,当前服务节点负载过高,将向其他通信地址发送请求.");
                        //若不包含对应字段，则不进行对象转换，减少报错
                        if (ret.contains("redirect")) {
                            ResRedirBody resRedirBody = JSONObject.parseObject(ret, ResRedirBody.class);
                            String redirectUrl = resRedirBody.getData().getRedirect();
                            if (StringUtils.isNotBlank(redirectUrl)) {
                                YbHttpAvroSinkFile.changeUrl(redirectUrl);
                            }
                        } else {
                            logger.error("AsyncSingleMsgCallBack==>服务端响应体中ResRedirBody.data.redirect字段不存在或格式不正确!!!");
                        }

                        HttpClientUtil.getRetryQueue().put(dataJson);
                        break;
                    case 301:
                        logger.error("AsyncSingleMsgCallBack==>Status Code:" + statusCode
                                + "服务端响应时间(ms)==>" + (System.currentTimeMillis() - postTime) + "<==,"
                                + ",resRedirBodyCode:301,当前所有服务端节点均负载过高,暂无可用资源,请等待.");
                        HttpClientUtil.getRetryQueue().put(dataJson);
                        break;
                    case 410:
                        logger.error("AsyncSingleMsgCallBack==>Status Code:" + statusCode
                                + "服务端响应时间(ms)==>" + (System.currentTimeMillis() - postTime) + "<==,"
                                + ",resRedirBodyCode:410,Cookie已过期或Cookie错误,将开始更新Cookie.");
                        YbHttpAvroSinkFile.updateCookie();
                        break;
                    case 500:
                        logger.error("AsyncSingleMsgCallBack==>Status Code:" + statusCode
                                + "服务端响应时间(ms)==>" + (System.currentTimeMillis() - postTime) + "<==,"
                                + ",resRedirBodyCode:500,处理请求过程出现系统错误.");
                        YbHttpAvroSinkFile.updateCookie();
                        break;
                    default:

                        logger.error("AsyncSingleMsgCallBack==>数据加载失败,响应体:" + ret
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
            logger.error("AsyncSingleMsgCallBack Get response from ZX is error===>>>" + e.getMessage() + "<<<===Message is==>" + dataJson + "<==");//细化日志
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
        logger.error("AsyncSingleMsgCallBack Request is cancelled");
    }

    /**
     * 请求失败后调用该函数
     */
    @Override
    public void failed(Exception e) {
        AvroMonitorTimerTask.addFailedNum(count);
        logger.error("AsyncSingleMsgCallBack Request is Failed,This Failed data is ==>"
                    + dataJson );
    }

}







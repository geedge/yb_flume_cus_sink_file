package cn.ac.iie.cusflume.sink;

import cn.ac.iie.cusflume.sink.CommonUtils.SinkHttpClientUtil;
import cn.ac.iie.cusflume.sink.avroUtils.AvroMonitorTimerTask;
import cn.ac.iie.cusflume.sink.avroUtils.MD5Utils;
import cn.ac.iie.cusflume.sink.bean.redirectBean.ResRedirBody;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import java.io.IOException;

public class SinkService {
    private static final SinkService sinkService = new SinkService();

    private SinkService() {
    }
    //SinkHttpClientUtil.getHttpClient();
    public static CloseableHttpClient httpClient = null;
    private static Logger LOG = Logger.getLogger(SinkHttpClientUtil.class);

    public static SinkService getInstance() {
        return sinkService;
    }

    public void producerAvroMessageToBus(
            String urlProducer, String topic, byte[] results,
            String dataJson, String userAgent, String msgSessionCookie, int batchSize) {

        httpClient = SinkHttpClientUtil.getHttpClient();
        HttpPost httpPost = null;
        urlProducer = urlProducer.trim();
        String msg = "-1";
        // 创建POST请求对象
        CloseableHttpResponse response = null;
        try {
            httpPost = new HttpPost(urlProducer);
            httpPost.addHeader("User-Agent", userAgent);
            httpPost.setHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
            httpPost.addHeader("Cookie", msgSessionCookie);
            String md5Avro = MD5Utils.md5Encode(results);
            httpPost.addHeader("Checksum", md5Avro);
            LOG.info("批量发送body Checksum MD5 为:" + md5Avro);
            httpPost.addHeader("Content-Type", "binary/octet-stream");
            httpPost.addHeader("X-Tag", cn.ac.iie.cusflume.sink.HttpAsyncUtils.HttpClientUtil.getXTAG(dataJson, topic));
            ByteArrayEntity payload = new ByteArrayEntity(results);
            payload.setContentEncoding("utf-8");
            httpPost.setEntity(payload);
            long startTime = System.currentTimeMillis();
            response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            msg = EntityUtils.toString(entity, "UTF-8");
            Log.warn(Thread.currentThread().getName() + "，请求响应用时（ms）：" + (System.currentTimeMillis() - startTime));
            if (statusCode != HttpStatus.SC_OK) {
                AvroMonitorTimerTask.addSuccessNum(batchSize);
                LOG.info("数据总线响应内容：" + msg);
            } else {
                AvroMonitorTimerTask.addFailedNum(batchSize);
                LOG.error("数据总线反馈出错：" + statusCode + "，msg" + msg);

                switch (statusCode) {
                    case 300:
                        LOG.info("AsyncPostMsgCallBack==>重定向响应体-redirect-ret==>"
                                + ",resRedirBodyCode:300,当前服务节点负载过高,将向其他通信地址发送请求.");
                        //若不包含对应字段，则不进行对象转换，减少报错
                        if (msg.contains("redirect")) {
                            ResRedirBody resRedirBody = JSONObject.parseObject(msg, ResRedirBody.class);
                            String redirectUrl = resRedirBody.getData().getRedirect();
                            if (StringUtils.isNotBlank(redirectUrl)) {
                                YbHttpAvroSinkFile.changeUrl(redirectUrl);
                            }
                        } else {
                            LOG.error("AsyncPostMsgCallBack==>服务端响应体中ResRedirBody.data.redirect字段不存在或格式不正确!!!");
                        }


                        break;
                    case 301:
                        LOG.error("AsyncPostMsgCallBack==>Status Code:" + statusCode +
                                ",resRedirBodyCode:301,当前所有服务端节点均负载过高,暂无可用资源,请等待.");
                        break;
                    case 410:
                        LOG.error("AsyncPostMsgCallBack==>Status Code:" + statusCode
                                + "服务端响应时间(ms)==>" + "<==,"
                                + ",resRedirBodyCode:410,Cookie已过期或Cookie错误,将开始更新Cookie.");
                        YbHttpAvroSinkFile.updateCookie();
                        break;
                    case 500:
                        LOG.error("AsyncPostMsgCallBack==>Status Code:" + statusCode
                                + "服务端响应时间(ms)==>" + "<==,"
                                + ",resRedirBodyCode:500,处理请求过程出现系统错误.");
                        YbHttpAvroSinkFile.updateCookie();
                        break;
                    default:
                        LOG.error("AsyncPostMsgCallBack==>数据加载失败,响应体:" + "<==,"
                                + "---Status Code:" + statusCode);
                        break;
                }

            }


        } catch (ClientProtocolException e) {
            LOG.error("协议错误: {}", e);
        } catch (ParseException e) {
            LOG.error("解析错误: {}", e);
        } catch (IOException e) {
            LOG.error("IO错误: {}", e);
        } catch (Exception e) {
            LOG.error("其它错误: {}", e);
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consumeQuietly(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    LOG.error("释放链接错误: {}", e);
                }
            }
        }

    }

}


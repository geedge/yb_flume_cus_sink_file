package cn.ac.iie.cusflume.sink.CommonUtils;

import cn.ac.iie.cusflume.sink.YbHttpAvroSinkFile;
import cn.ac.iie.cusflume.sink.avroUtils.AvroMonitorTimerTask;
import cn.ac.iie.cusflume.sink.bean.redirectBean.ResRedirBody;
import cn.ac.iie.cusflume.sink.daoUtils.RealtimeCountConfig;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;

public class SinkHttpClientUtil {
    private static Logger LOG = Logger.getLogger(SinkHttpClientUtil.class);
    /** 全局连接池对象 */
    private static final PoolingHttpClientConnectionManager connManager =
            new PoolingHttpClientConnectionManager();
    public static final String DEFAULT_CHARSET = "utf-8";
    private static int socketTimeout = RealtimeCountConfig.HTTP_ASYNC_SOCKETTIMEOUT;//设置等待数据超时时间60秒钟 根据业务调整
    private static int connectTimeout = RealtimeCountConfig.HTTP_ASYNC_CONNECTTIMEOUT;//连接超时
    private static int poolSize = RealtimeCountConfig.HTTP_ASYNC_POOLSIZE;//连接池最大连接数
    private static int maxPerRoute = RealtimeCountConfig.HTTP_ASYNC_MAXPERROUTE;//每个主机的并发最多只有1500
    private static int connectionRequestTimeout = RealtimeCountConfig.HTTP_ASYNC_CONNECTIONREQUESTTIMEOUT; //从连接池中后去连接的timeout时间


    static {
        connManager.setMaxTotal(poolSize);
        connManager.setDefaultMaxPerRoute(maxPerRoute);
    }

    public static CloseableHttpClient getHttpClient() {
        RequestConfig requestConfig = RequestConfig.custom()
                // 获取连接超时时间
                .setConnectionRequestTimeout(connectionRequestTimeout)
                // 请求超时时间
                .setConnectTimeout(connectTimeout)
                // 响应超时时间
                .setSocketTimeout(socketTimeout)
                .build();


        /**
         * 测出超时重试机制为了防止超时不生效而设置
                *  如果直接放回false,不重试
                *  这里会根据情况进行判断是否重试
                */
        HttpRequestRetryHandler retry = (exception, executionCount, context) -> {
            if (executionCount >= 3) {// 如果已经重试了3次，就放弃
                return false;
            }
            if (exception instanceof NoHttpResponseException) {// 如果服务器丢掉了连接，那么就重试
                return true;
            }
            if (exception instanceof SSLHandshakeException) {// 不要重试SSL握手异常
                return false;
            }
            if (exception instanceof InterruptedIOException) {// 超时
                return true;
            }
            if (exception instanceof UnknownHostException) {// 目标服务器不可达
                return false;
            }
            if (exception instanceof ConnectTimeoutException) {// 连接被拒绝
                return false;
            }
            if (exception instanceof SSLException) {// ssl握手异常
                return false;
            }
            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            // 如果请求是幂等的，就再次尝试
            if (!(request instanceof HttpEntityEnclosingRequest)) {
                return true;
            }
            return false;
        };


        ConnectionKeepAliveStrategy myStrategy = (response, context) -> {
            HeaderElementIterator it = new BasicHeaderElementIterator
                    (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && param.equalsIgnoreCase("timeout")) {
                    return Long.parseLong(value) * 1000;
                }
            }
            return 60 * 1000;//如果没有约定，则默认定义时长为60s
        };

        // 创建httpClient
        return HttpClients.custom()
                // 把请求相关的超时信息设置到连接客户端
                .setDefaultRequestConfig(requestConfig)
                // 把请求重试设置到连接客户端
                .setRetryHandler(retry)
                .setKeepAliveStrategy(myStrategy)
                // 配置连接池管理对象
                .setConnectionManager(connManager)
                .build();

    }


    public static void httpPost(String url, byte[] requestBody, int batchSize, Header... headers) {
        String msg = "-1";
        // 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        // 创建POST请求对象
        CloseableHttpResponse response = null;
        try {
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
            httpPost.setHeader("Content-Type", "application/json");
            if (StringUtil.isNotEmpty(headers)) {
                for (Header h : headers) {
                    httpPost.addHeader(h);
                }
            }
            ByteArrayEntity payload = new ByteArrayEntity(requestBody);
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
            if (statusCode != HttpStatus.SC_OK ) {
                AvroMonitorTimerTask.addSuccessNum(batchSize);
                LOG.info("数据总线响应内容：" + msg);
            }  else {
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
                                + "服务端响应时间(ms)==>" +  "<==,"
                                + ",resRedirBodyCode:410,Cookie已过期或Cookie错误,将开始更新Cookie.");
                        YbHttpAvroSinkFile.updateCookie();
                        break;
                    case 500:
                        LOG.error("AsyncPostMsgCallBack==>Status Code:" + statusCode
                                + "服务端响应时间(ms)==>" +  "<==,"
                                + ",resRedirBodyCode:500,处理请求过程出现系统错误.");
                        YbHttpAvroSinkFile.updateCookie();
                        break;
                    default:
                        LOG.error("AsyncPostMsgCallBack==>数据加载失败,响应体:"  + "<==,"
                                + "---Status Code:" + statusCode );
                        break;
                }

            }



        } catch (ClientProtocolException e) {
            LOG.error("协议错误: {}", e);
        } catch (ParseException e) {
            LOG.error("解析错误: {}", e);
        } catch (IOException e) {
            LOG.error("IO错误: {}", e);
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

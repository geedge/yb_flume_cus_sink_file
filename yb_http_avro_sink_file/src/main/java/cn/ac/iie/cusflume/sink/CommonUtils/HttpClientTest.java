package cn.ac.iie.cusflume.sink.CommonUtils;

import cn.ac.iie.cusflume.sink.HttpAsyncUtils.HttpClientUtil;
import cn.ac.iie.cusflume.sink.avroUtils.MD5Utils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import java.io.IOException;

public class HttpClientTest {

    private static CloseableHttpClient client = null;
    private static Logger LOG = Logger.getLogger(HttpClientTest.class);
    static {
        //连接池对象
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        //将最大连接数增加到200
        connectionManager.setMaxTotal(40);
        //将每个路由的默认最大连接数增加到20
        connectionManager.setDefaultMaxPerRoute(40);
        //HttpClient对象
        client = HttpClients.custom().setConnectionManager(connectionManager).build();
    }


    public static  void producerAvroMessageToZX (String urlProducer,String topic, byte[] results, String dataJson, String userAgent, String msgSessionCookie, int count, long postTime)  {
        try {
            HttpPost httpPost = new HttpPost(urlProducer);
            //2020-9-9 新加头部
            httpPost.setHeader("Connection", "Keep-Alive");
            httpPost.setHeader("Content-type", "application/avro+json;charset=UTF-8");
            httpPost.setHeader(HttpHeaders.USER_AGENT, userAgent);
            httpPost.addHeader("X-Tag", HttpClientUtil.getXTAG(dataJson, topic));
            httpPost.setHeader("Cookie", msgSessionCookie);
            String md5Avro = MD5Utils.md5Encode(results);
            httpPost.addHeader("Checksum", md5Avro);
            httpPost.setEntity(new ByteArrayEntity(results));

            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {
                @Override
                public String handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
                    return EntityUtils.toString(response.getEntity());
                }
            };
            RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(10000)
                    .setConnectTimeout(10000)
                    .setSocketTimeout(10000)
                    .build();
            httpPost.setConfig(requestConfig);
            long startTime = System.currentTimeMillis();
            String body = client.execute(httpPost, responseHandler); // 线程可能会在这里被阻塞
            Log.warn(Thread.currentThread().getName() + "用时" + (System.currentTimeMillis() - startTime));

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

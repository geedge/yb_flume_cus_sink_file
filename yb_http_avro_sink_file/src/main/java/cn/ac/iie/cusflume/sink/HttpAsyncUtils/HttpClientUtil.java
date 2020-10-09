package cn.ac.iie.cusflume.sink.HttpAsyncUtils;

import cn.ac.iie.cusflume.sink.CommonUtils.GetDataDictionaryCodeByTopicUtils;
import cn.ac.iie.cusflume.sink.CommonUtils.GetFilePathByTopicUtils;
import cn.ac.iie.cusflume.sink.HttpAsyncUtils.mail.AsyncPostMailFilesCallback;
import cn.ac.iie.cusflume.sink.HttpAsyncUtils.msgCallBack.AsyncPostMsgCallBack;
import cn.ac.iie.cusflume.sink.avroUtils.AvroMonitorTimerTask;
import cn.ac.iie.cusflume.sink.avroUtils.MD5Utils;
import cn.ac.iie.cusflume.sink.avroUtils.avroRecord.GetAvroRecordByTopicUtils;
import cn.ac.iie.cusflume.sink.avroUtils.avroSchema.GetAvroSchemaByTopicUtils;
import cn.ac.iie.cusflume.sink.daoUtils.RealtimeCountConfig;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
import okhttp3.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * http client 业务逻辑处理类
 * 执行请求发送
 */
public class HttpClientUtil {

    private static Logger LOG = Logger.getLogger(HttpClientUtil.class);

    protected static ExecutorService pool = Executors.newFixedThreadPool(RealtimeCountConfig.HTTP_ASYNC_PARALLELISM);//线程池

    private static String utf8Charset = "utf-8";

    private static final MediaType JSON
            = MediaType.parse("application/json; charset=utf-8");

    /**
     * 向指定的url发送一次post请求,参数是List<NameValuePair>
     *
     * @param baseUrl 请求地址
     * @param list    请求参数,格式是List<NameValuePair>
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestParam接收参数
     */
    public static String httpSyncPost(String baseUrl, List<BasicNameValuePair> list) {

        CloseableHttpClient httpClient = HttpClientFactory.getInstance().getHttpSyncClientPool().getHttpClient();
        HttpPost httpPost = new HttpPost(baseUrl);

        //Parameters
        LOG.warn("==== Parameters ======" + list);
        CloseableHttpResponse response = null;
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(list));
//            httpPost.setHeader("Connection","close");
            response = httpClient.execute(httpPost);
            LOG.warn("========HttpResponseProxy：========" + response.getStatusLine());
            HttpEntity entity = response.getEntity();
            String result = null;
            if (entity != null) {
                result = EntityUtils.toString(entity, "UTF-8");
                LOG.warn("========Response=======" + result);
            }
            EntityUtils.consume(entity);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * 向指定的url发送一次post请求,参数是字符串
     *
     * @param baseUrl    请求地址
     * @param postString 请求参数,格式是json.toString()
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestBody接收参数
     */
    public static String httpSyncPost(String baseUrl, String postString) {

        CloseableHttpClient httpClient = HttpClientFactory.getInstance().getHttpSyncClientPool().getHttpClient();
        HttpPost httpPost = new HttpPost(baseUrl);
        //parameters
        LOG.warn("==== Parameters ======" + postString);
        CloseableHttpResponse response = null;
        try {
            if (postString == null || "".equals(postString)) {
                throw new Exception("missing post String");
            }

            StringEntity stringEntity = new StringEntity(postString.toString(), utf8Charset);
            stringEntity.setContentEncoding("UTF-8");
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);

            response = httpClient.execute(httpPost);
            LOG.warn("========HttpResponseProxy：========" + response.getStatusLine());
            HttpEntity entity = response.getEntity();
            String result = null;
            if (entity != null) {
                result = EntityUtils.toString(entity, "UTF-8");
                LOG.warn("========Response=======" + result);
            }
            EntityUtils.consume(entity);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }


    /**
     * 向指定的url发送一次get请求,参数是List<NameValuePair>
     *
     * @param baseUrl 请求地址
     * @param list    请求参数,格式是List<NameValuePair>
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestParam接收参数
     */
    public static String httpSyncGet(String baseUrl, List<BasicNameValuePair> list) {

        CloseableHttpClient httpClient = HttpClientFactory.getInstance().getHttpSyncClientPool().getHttpClient();
        HttpGet httpGet = new HttpGet(baseUrl);
        //Parameters
        LOG.warn("==== Parameters ======" + list);
        CloseableHttpResponse response = null;
        try {

            if (list != null) {
                String getUrl = EntityUtils
                        .toString(new UrlEncodedFormEntity(list));
                httpGet.setURI(new URI(httpGet.getURI().toString()
                        + "?" + getUrl));
            } else {
                httpGet.setURI(new URI(httpGet.getURI().toString()));
            }

            response = httpClient.execute(httpGet);
            LOG.warn("========HttpResponseProxy：========" + response.getStatusLine());
            HttpEntity entity = response.getEntity();
            String result = null;
            if (entity != null) {
                result = EntityUtils.toString(entity, "UTF-8");
                LOG.warn("========Response=======" + result);
            }
            EntityUtils.consume(entity);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * 向指定的url发送一次get请求,参数是字符串
     *
     * @param baseUrl   请求地址
     * @param urlParams 请求参数,格式是String
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestParam接收参数
     */
    public static String httpSyncGet(String baseUrl, String urlParams) {

        CloseableHttpClient httpClient = HttpClientFactory.getInstance().getHttpSyncClientPool().getHttpClient();
        HttpGet httpGet = new HttpGet(baseUrl);
        //Parameters
        LOG.warn("==== Parameters ======" + urlParams);
        CloseableHttpResponse response = null;
        try {

            if (null != urlParams || "".equals(urlParams)) {

                httpGet.setURI(new URI(httpGet.getURI().toString()
                        + "?" + urlParams));
            } else {
                httpGet.setURI(new URI(httpGet.getURI().toString()));
            }

            response = httpClient.execute(httpGet);
            LOG.warn("========HttpResponseProxy：========" + response.getStatusLine());
            HttpEntity entity = response.getEntity();
            String result = null;
            if (entity != null) {
                result = EntityUtils.toString(entity, "UTF-8");
                LOG.warn("========Response=======" + result);
            }
            EntityUtils.consume(entity);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }


    /**
     * 向指定的url发送一次get请求,参数是字符串
     *
     * @param baseUrl 请求地址
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestParam接收参数
     */
    public static String httpSyncGet(String baseUrl) {

        CloseableHttpClient httpClient = HttpClientFactory.getInstance().getHttpSyncClientPool().getHttpClient();
        HttpGet httpGet = new HttpGet(baseUrl);

        CloseableHttpResponse response = null;
        try {
            httpGet.setURI(new URI(httpGet.getURI().toString()));
            response = httpClient.execute(httpGet);
            LOG.warn("========HttpResponseProxy：========" + response.getStatusLine());
            HttpEntity entity = response.getEntity();
            String result = null;
            if (entity != null) {
                result = EntityUtils.toString(entity, "UTF-8");
                LOG.warn("========Response=======" + result);
            }
            EntityUtils.consume(entity);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }


    /**
     * 向指定的url发送一次异步post请求,参数是字符串
     *
     * @param baseUrl    请求地址
     * @param postString 请求参数,格式是json.toString()
     * @param urlParams  请求参数,格式是String
     * @param callback   回调方法,格式是FutureCallback
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestParam接收参数
     */
    public static void httpAsyncPost(String baseUrl, String postString,
                                     String urlParams, FutureCallback callback) throws Exception {
        if (baseUrl == null || "".equals(baseUrl)) {
            LOG.warn("we don't have base url, check config");
            throw new Exception("missing base url");
        }
        CloseableHttpAsyncClient hc = HttpClientFactory.getInstance().getHttpAsyncClientPool()
                .getAsyncHttpClient();
        try {
            hc.start();
            HttpPost httpPost = new HttpPost(baseUrl);

//            httpPost.setHeader("Connection","close");

            if (null != postString) {
                LOG.debug("exeAsyncReq post postBody={" + postString + "}");
                StringEntity entity = new StringEntity(postString.toString(), utf8Charset);
                entity.setContentEncoding("UTF-8");
                entity.setContentType("application/json");
                httpPost.setEntity(entity);
            }

            if (null != urlParams) {

                httpPost.setURI(new URI(httpPost.getURI().toString()
                        + "?" + urlParams));
            }

            LOG.warn("exeAsyncReq getparams:" + httpPost.getURI());

            hc.execute(httpPost, callback);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 向指定的url发送一次异步post请求,参数是字符串
     *
     * @param baseUrl   请求地址
     * @param urlParams 请求参数,格式是List<BasicNameValuePair>
     * @param callback  回调方法,格式是FutureCallback
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestParam接收参数
     */
    public static void httpAsyncPost(String baseUrl, List<BasicNameValuePair> postBody,
                                     List<BasicNameValuePair> urlParams, FutureCallback callback) throws Exception {
        if (baseUrl == null || "".equals(baseUrl)) {
            LOG.warn("we don't have base url, check config");
            throw new Exception("missing base url");
        }

        try {
            CloseableHttpAsyncClient hc = HttpClientFactory.getInstance().getHttpAsyncClientPool()
                    .getAsyncHttpClient();

            hc.start();

            HttpPost httpPost = new HttpPost(baseUrl);

//            httpPost.setHeader("Connection","close");

            if (null != postBody) {
                LOG.debug("exeAsyncReq post postBody={" + postBody + "}");
                UrlEncodedFormEntity entity = new UrlEncodedFormEntity(
                        postBody, "UTF-8");
                httpPost.setEntity(entity);
            }

            if (null != urlParams) {

                String getUrl = EntityUtils
                        .toString(new UrlEncodedFormEntity(urlParams));

                httpPost.setURI(new URI(httpPost.getURI().toString()
                        + "?" + getUrl));
            }

            LOG.warn("exeAsyncReq getparams:" + httpPost.getURI());


            hc.execute(httpPost, callback);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 往总线post文件数据---------20191106-字节版本--回调包含标签-AsyncHttpClientPostFileCallback版本
     * 向指定的url发送一次异步put请求,参数是字符串--注意由于一部的需求特殊,这里用的put方法
     *
     * @param baseUrl  请求地址
     * @param callback 回调方法,格式是FutureCallback
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestParam接收参数
     */
    public static void httpAsyncPostFileToZx(String baseUrl, byte[] fileIs, AsyncHttpClientPostFileCallback callback) throws Exception {
        if (baseUrl == null || "".equals(baseUrl)) {
            LOG.warn("we don't have base url, check config");
            throw new Exception("missing base url");
        }

        HttpPut httpPut;

        CloseableHttpAsyncClient hc = null;

        try {
            hc = HttpClientFactory.getInstance().getHttpAsyncClientPool().getAsyncHttpClient();
            hc.start();

            HttpClientContext localContext = HttpClientContext.create();
            BasicCookieStore cookieStore = new BasicCookieStore();

            httpPut = new HttpPut(baseUrl);

//            httpPost.setHeader("Transfer-Encoding","close");
//            httpPost.setHeader("Content-Type", "binary/octet-stream");
            httpPut.setHeader("Content-Type", "binary/octet-stream");

            if (null != fileIs) {
                httpPut.setEntity(new ByteArrayEntity(fileIs));
            }
            LOG.info("File httpPut.getURI()===>>>" + httpPut.getURI());

            localContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);

            hc.execute(httpPut, localContext, callback);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 往总线post文件数据---------20191106-字节版本--回调包含标签-AsyncPostMailFilesCallback版本
     * 向指定的url发送一次异步put请求,参数是字符串--注意由于一部的需求特殊,这里用的put方法
     *
     * @param baseUrl  请求地址
     * @param callback 回调方法,格式是FutureCallback
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestParam接收参数
     */
    public static void httpAsyncPostFileToZx(String baseUrl, byte[] fileIs, AsyncPostMailFilesCallback callback) throws Exception {
        if (baseUrl == null || "".equals(baseUrl)) {
            LOG.warn("we don't have base url, check config");
            throw new Exception("missing base url");
        }

        HttpPut httpPut;

        CloseableHttpAsyncClient hc = null;

        try {
            hc = HttpClientFactory.getInstance().getHttpAsyncClientPool().getAsyncHttpClient();
            hc.start();

            HttpClientContext localContext = HttpClientContext.create();
            BasicCookieStore cookieStore = new BasicCookieStore();

            httpPut = new HttpPut(baseUrl);


//            httpPost.setHeader("Transfer-Encoding","close");
//            httpPost.setHeader("Content-Type", "binary/octet-stream");
            httpPut.setHeader("Content-Type", "binary/octet-stream");
            if (null != fileIs) {
                httpPut.setEntity(new ByteArrayEntity(fileIs));
            }
            LOG.info("File httpPut.getURI()===>>>" + httpPut.getURI());

            localContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);

            hc.execute(httpPut, localContext, callback);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        return callback;
    }

    /**
     * 向指定的url发送一次异步get请求,参数是String
     * 从minio异步获取文件,请求方式为GET
     * 获取文件成功后将其POST至ZX获取文件ID
     * 最后将ID补全入信息
     *
     * @param baseUrl  请求地址
     * @param callback 回调方法,格式是FutureCallback
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestParam接收参数
     */
    public static void httpAsyncGetFile(String baseUrl, FutureCallback callback) throws Exception {

        if (baseUrl == null || "".equals(baseUrl)) {
            LOG.warn("we don't have base url, check config");
            throw new Exception("missing base url");
        }
        CloseableHttpAsyncClient hc = HttpClientFactory.getInstance().getHttpAsyncClientPool().getAsyncHttpClient();
        try {
            hc.start();
            HttpGet httpGet = new HttpGet(baseUrl);

//            httpGet.setHeader("Connection", "close");//关闭长连接-20191204新增
            httpGet.setHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_CLOSE);//关闭长连接-20191204新增-使用字符代替

//            if (null != urlParams || "".equals(urlParams)) {
//
//                httpGet.setURI(new URI(httpGet.getURI().toString()
//                        + "?" + urlParams));
//            } else {
//                httpGet.setURI(new URI(httpGet.getURI().toString()));
//            }

//            LOG.info("exeAsyncReq getparams:" + httpGet.getURI());
            LOG.info("File httpGet.getURI()===>>>" + httpGet.getURI());
            hc.execute(httpGet, callback);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        Thread.sleep(50);
//        return callback.getAsyncHttpClientPostFileCallback();
//        return null;
    }

    /**
     * 向指定的url发送一次异步get请求,参数是String
     *
     * @param baseUrl   请求地址
     * @param urlParams 请求参数,格式是String
     * @param callback  回调方法,格式是FutureCallback
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestParam接收参数
     */
    public static void httpAsyncGet(String baseUrl, String urlParams, FutureCallback callback) throws Exception {

        if (baseUrl == null || "".equals(baseUrl)) {
            LOG.warn("we don't have base url, check config");
            throw new Exception("missing base url");
        }
        CloseableHttpAsyncClient hc = HttpClientFactory.getInstance().getHttpAsyncClientPool()
                .getAsyncHttpClient();
        try {


            hc.start();

            HttpGet httpGet = new HttpGet(baseUrl);

//            httpGet.setHeader("Connection","close");

            if (null != urlParams || "".equals(urlParams)) {

                httpGet.setURI(new URI(httpGet.getURI().toString()
                        + "?" + urlParams));
            } else {
                httpGet.setURI(new URI(httpGet.getURI().toString()));
            }

            LOG.warn("exeAsyncReq getparams:" + httpGet.getURI());


            hc.execute(httpGet, callback);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 向指定的url发送一次异步get请求,参数是List<BasicNameValuePair>
     *
     * @param baseUrl   请求地址
     * @param urlParams 请求参数,格式是List<BasicNameValuePair>
     * @param callback  回调方法,格式是FutureCallback
     * @return 返回结果, 请求失败时返回null
     * @apiNote http接口处用 @RequestParam接收参数
     */
    public static void httpAsyncGet(String baseUrl, List<BasicNameValuePair> urlParams, FutureCallback callback) throws Exception {
        if (baseUrl == null || "".equals(baseUrl)) {
            LOG.warn("we don't have base url, check config");
            throw new Exception("missing base url");
        }

        try {
            CloseableHttpAsyncClient hc = HttpClientFactory.getInstance().getHttpAsyncClientPool()
                    .getAsyncHttpClient();

            hc.start();

            HttpPost httpGet = new HttpPost(baseUrl);

//            httpGet.setHeader("Connection","close");

            if (null != urlParams) {

                String getUrl = EntityUtils
                        .toString(new UrlEncodedFormEntity(urlParams));

                httpGet.setURI(new URI(httpGet.getURI().toString()
                        + "?" + getUrl));
            }

            LOG.warn("exeAsyncReq getparams:" + httpGet.getURI());


            hc.execute(httpGet, callback);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static String OkSyncPost(String url, String json) throws IOException {

        OkHttpClient okClient = HttpClientFactory.getInstance().getOkClientPool().getHttpClient();

        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        try (Response response = okClient.newCall(request).execute()) {

            return response.body().string();
        }
    }

    public static void OkAsyncPost(String url, String json) throws IOException {
        OkHttpClient okClient = HttpClientFactory.getInstance().getOkClientPool().getHttpClient();

        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        Call call = okClient.newCall(request);
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {

                LOG.warn("OkAsyncPost回调:" + response.body().string());
            }
        });

    }


    public static void OkAsyncPost(String url, Map<String, String> map) throws IOException {
        OkHttpClient okClient = HttpClientFactory.getInstance().getOkClientPool().getHttpClient();

        FormBody.Builder formBodyBuilder = new FormBody.Builder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            formBodyBuilder.add(entry.getKey(), entry.getValue());
        }
        Request request = new Request.Builder()
                .url(url)
                .post(formBodyBuilder.build())
                .build();
        Call call = okClient.newCall(request);
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {

                LOG.warn("OkAsyncPost回调:" + response.body().string());
            }
        });

    }

    public static void OkAsyncPost(String url, Map<String, String> map, Callback callback) throws IOException {
        OkHttpClient okClient = HttpClientFactory.getInstance().getOkClientPool().getHttpClient();

        FormBody.Builder formBodyBuilder = new FormBody.Builder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            formBodyBuilder.add(entry.getKey(), entry.getValue());
        }

        Request request = new Request.Builder()
                .url(url)
                .post(formBodyBuilder.build())
                .build();
        Call call = okClient.newCall(request);
        call.enqueue(callback);

    }

    public static String OkSyncGet(String url) throws IOException {

        OkHttpClient okClient = HttpClientFactory.getInstance().getOkClientPool().getHttpClient();

        Request request = new Request.Builder()
                .url(url)
                .build();
        try (Response response = okClient.newCall(request).execute()) {

            return response.body().string();
        }
    }

    public static void OkAsyncGet(String url) throws IOException {

        OkHttpClient okClient = HttpClientFactory.getInstance().getOkClientPool().getHttpClient();

        Request request = new Request.Builder()
                .url(url)
                .build();
        Call call = okClient.newCall(request);
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {

                LOG.warn("OkAsyncGet回调:" + response.body().string());
            }
        });
    }


    /**
     * 获取单个文件并传递单个文件时使用
     *
     * @param getFileUrl
     * @param callback
     */
    public static void getFileAndPostFile(String getFileUrl, FutureCallback callback) {
        try {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        httpAsyncGetFile(getFileUrl, callback);
//                        LOG.info(Thread.currentThread().getName() + "===>run====>>>" + "success,now time is" + System.currentTimeMillis());
                    } catch (Exception e) {
                        LOG.error("getFileAndPostFile multithreading is error===>" + e);
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("getFileAndPostFile method is error===>" + e);
        }
    }

    /**
     * 生产AVRO数据入ZX(单条)--数据不包含schema
     * 静态,适用于异步与多线程的版本
     *
     * @param urlProducer
     * @param topic
     * @param dataJson
     * @param userAgent
     * @param msgSessionCookie
     * @return
     */
    public static void asyncProducerAvroToZX(String urlProducer, String topic, String dataJson, String userAgent, String msgSessionCookie, int count) {
        HttpPost httpPost = null;
        urlProducer = urlProducer.trim();
        byte[] resultArray = null;
        CloseableHttpAsyncClient httpClient = null;
        try {
            httpClient = HttpClientFactory.getInstance().getHttpAsyncClientPool().getAsyncHttpClient();
            httpClient.start();
            // get schema
            Schema schemaDataAvro = getSchemaFromHashMap(topic);//状态数据对象monitor-msg(一部状态数据对象)和INFLUX-SAPP-BPS-STAT-LOG(广东状态数据对象)会走此路
            ByteArrayOutputStream outAvro = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outAvro, null);
            DatumWriter<GenericRecord> fileWriter = new SpecificDatumWriter<GenericRecord>(schemaDataAvro);
            outAvro.reset();

            GenericRecord recordAvro = new GenericData.Record(schemaDataAvro);
            recordAvro = GetAvroRecordByTopicUtils.getAvroRecordByTopicName(topic, recordAvro, dataJson, schemaDataAvro);//单条,主数据
            logger.info("封装数据对象或文件标签AvroRecord后数据为===>" + recordAvro.toString() + "<===封装数据对象或文件标签AvroRecord后数据为");
            fileWriter.write(recordAvro, encoder);

            if (encoder != null) {
                encoder.flush();
            }

            if (outAvro != null) {
                resultArray = outAvro.toByteArray();

                outAvro.flush();
                outAvro.close();
                outAvro = null;
            }
            logger.info("数据对象或文件标签resultArray长度为:" + resultArray.length);
            logger.debug("原始数据==>" + dataJson + "<==," +
                    "原始数据checksum==>" + MD5Utils.md5Encode(dataJson) + "<==," +
                    "数据对象或文件标签resultArray长度为:" + resultArray.length + "," +
                    "数据对象或文件标签字节数组:" + Arrays.toString(resultArray));

            if (!("NTC-COLLECT-FILE-LOG".equals(topic))) {
                //传入的resultArray为数据对象的数组,返回的是拼接的整体的数组
                resultArray = getTagRecordAndMergeAllArray(topic, dataJson, resultArray);
            } else {
                //说明为NTC-COLLECT-FILE-LOG,此时不需要进行d_tag的获取,因为数据里不包含d_tag,数据本身就是d_tag,前面处理的resultArray就作为NTC-COLLECT-FILE-LOG的d_tag使用
                resultArray = alreadyGetFileTagRecordSoOnlyGetMergeAllArray(topic, resultArray);
            }

            httpPost = new HttpPost(urlProducer);
            // set header
            httpPost.addHeader("User-Agent", userAgent);

            httpPost.setHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_CLOSE);//关闭长连接-20191225新增-使用字符代替
            //一部状态回传需要此Cookie打开
//            if ("monitor-msg".equals(topic)) {
//                httpPost.addHeader("Cookie", msgSessionCookie);//广东文件消息测试时加上Cookie会验证不通过,即那边显示为两个Cookie,不加Cookie则验证通过
//            }

            httpPost.addHeader("Cookie", msgSessionCookie);//不设置Cookie时,广东测试出现报错,打开广东的Cookie设置测试一下,经测试,可用

            try {
                String md5Avro = MD5Utils.md5Encode(resultArray);
                httpPost.addHeader("Checksum", md5Avro);
                logger.info("请求端Checksum MD5 avro 加密为:" + md5Avro);
                logger.debug("原始数据==>" + dataJson + "<==," +
                        "原始数据checksum==>" + MD5Utils.md5Encode(dataJson) + "<==," +
                        "对应请求端Checksum MD5 avro 加密为:" + md5Avro);//20200521新增,自证清白
            } catch (Exception e) {
                logger.error("MD5Utils.md5Encode Method is error===>>> " + e);
            }
//            httpPost.addHeader("Content-Type", "application/avro+json;charset=UTF-8");
            httpPost.addHeader("Content-Type", "binary/octet-stream");

            //设置调度标签要求的参数,文件消息需要设置FilePath--------------------↓--------------------20191210新增
            httpPost.addHeader("X-Tag", getXTAG(dataJson, topic));//从json中获取-单条

            String filePathByTopicName = GetFilePathByTopicUtils.getFilePathByTopicName(dataJson, topic);
            if (!("-".equals(filePathByTopicName)) && !("[\"null\"]".equals(filePathByTopicName))) {
                httpPost.addHeader("FilePath", filePathByTopicName);//从json中获取
            }
            //设置调度标签要求的参数,文件消息需要设置FilePath-------------------↑---------------------20191210新增

            ByteArrayEntity payload = new ByteArrayEntity(resultArray);
            payload.setContentEncoding("utf-8");
            //payload.setContentType("text/xml; charset=UTF-8");
            // anti avro
            httpPost.setEntity(payload);

            logger.info("最终加载内容字节数组长度: " + resultArray.length);
//            logger.debug("封装数据==>" + dataJson + "<==最终加载内容字节数组长度: " + resultArray.length);//20200428进一步细化日志
            logger.debug("原始数据==>" + dataJson + "<==," +
                    "原始数据checksum==>" + MD5Utils.md5Encode(dataJson) + "<==," +
                    "最终加载内容字节数组长度: " + resultArray.length + "," +
                    "最终加载内容字节数组:" + Arrays.toString(resultArray));//20200521进一步细化日志

            //执行请求
            AsyncPostMsgCallBack asyncPostMsgCallBack = new AsyncPostMsgCallBack(urlProducer, topic, dataJson, userAgent, msgSessionCookie, count);
            httpClient.execute(httpPost, asyncPostMsgCallBack);

        } catch (MalformedURLException e) {
            //执行URL url = new URL()的异常
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            // 执行httpClient.execute(httpGet)的异常
            e.printStackTrace();
        } catch (IOException e) {
            // 执行httpClient.execute(httpGet)的异常
            logger.error("asyncProducerAvroToZX is IOException===>>>" + e.getMessage() + "<<<===IOException Message is==>" + dataJson + "<==");//进一步细化日志
            e.printStackTrace();
        } catch (Exception e) {
            //handle response here... try other servers
            logger.error("asyncProducerAvroToZX is Exception===>>>" + e.getMessage() + "<<<===Exception Message is==>" + dataJson + "<==");//进一步细化日志
            e.printStackTrace();
        }
    }

    /**
     * 使用hm缓存Schema
     *
     * @param topic
     * @return
     */
    private static Schema getSchemaFromHashMap(String topic) {
        if (schemaHashMap.containsKey(topic)) {
            return schemaHashMap.get(topic);
        } else {
            String schemaStr = GetAvroSchemaByTopicUtils.getAvroSchemaByTopicName(topic);
            Schema parseSchema = new Schema.Parser().parse(schemaStr);
            schemaHashMap.put(topic, parseSchema);
            return parseSchema;
        }
    }

    private static HashMap<String, Schema> schemaHashMap = new HashMap<String, Schema>();//用于存放Schema
    private static Logger logger = Logger.getLogger(HttpClientUtil.class);

    /**
     * 获取数据中的日志标签并将所有相关数据字节数组化后拼接返回
     *
     * @param topic
     * @param dataJson
     * @param dataResultArray
     * @return
     */
    private static byte[] getTagRecordAndMergeAllArray(String topic, String dataJson, byte[] dataResultArray) {
        byte[] dTagByteArray = null;//用于存放数据标签的avro的二进制流
        String tagTopicName = "log-tag";//除NTC-COLLECT-FILE-LOG外都是log-tag,因为都是在文件消息内部的d_tag获取的
        if ("monitor-msg".equals(topic) || "INFLUX-SAPP-BPS-STAT-LOG".equals(topic)) {//20200113暂定,一部和广东共用状态数据标签d_tag,但数据对象不同
            tagTopicName = "status-tag";
        }
        try {
            // get schema
            Schema schemaDTagAvro = getSchemaFromHashMap(tagTopicName);//使用缓存优化性能
            ByteArrayOutputStream outAvro = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outAvro, null);
            DatumWriter<GenericRecord> fileWriter = new SpecificDatumWriter<GenericRecord>(schemaDTagAvro);
            outAvro.reset();

            GenericRecord recordAvro = new GenericData.Record(schemaDTagAvro);

            //以下为临时,为了增加一个状态的临时d_tag,因为d_tag前端目前(20191222)传不了-----------↓--------------
            if (!("monitor-msg".equals(topic)) && !("INFLUX-SAPP-BPS-STAT-LOG".equals(topic))) {//20200113暂定,一部和广东共用状态数据标签d_tag,但数据对象不同
                //说明当前消息不是状态回传
                Map map = JSONObject.parseObject(dataJson, Map.class);
                Object d_tag = map.get("d_tag");
                if (d_tag != null) {
                    dataJson = d_tag.toString();//获取消息上的d_tag用于序列化成数据标签
                }
            } else {
                //说明为一部状态,目前前端没有,所以先自己创建一个状态的d_tag---用于一部;//20200113暂定,一部和广东共用状态数据标签d_tag,但数据对象不同
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String timeFormat = sdf.format(new Date(System.currentTimeMillis()));//格式2019-11-18 19:56:37
                dataJson = "{\"tag_version\":\"1.0\",\"data_subtype\":16387,\"data_type\":4,\"producer_id\":24832,\"timestamp\":\"yyyy-MM-dd HH:mm:ss\"}";
                Map map = JSONObject.parseObject(dataJson, Map.class);
                map.put("timestamp", timeFormat);
                dataJson = JSONObject.toJSONString(map);
            }
            //以下为临时,为了增加一个状态的临时d_tag,因为d_tag前端目前(20191222)传不了-----------↑--------------

            recordAvro = GetAvroRecordByTopicUtils.getAvroRecordByTopicName(tagTopicName, recordAvro, dataJson, schemaDTagAvro);//单条-"log-tag"或者"status-tag",直接传入Schema
            logger.info("封装日志标签或状态标签AvroRecord后数据为===>" + recordAvro.toString() + "<===封装日志标签或状态标签AvroRecord后数据为");
            fileWriter.write(recordAvro, encoder);

            if (encoder != null) {
                encoder.flush();
            }

            if (outAvro != null) {
                dTagByteArray = outAvro.toByteArray();

                outAvro.flush();
                outAvro.close();
                outAvro = null;
            }
            logger.info("日志标签或状态标签dTagByteArray长度为:" + dTagByteArray.length);
            logger.debug("标签数据==>" + dataJson + "<==," +
                    "标签数据checksum==>" + MD5Utils.md5Encode(dataJson) + "<==," +
                    "日志标签或状态标签dTagByteArray长度为:" + dTagByteArray.length + "," +
                    "日志标签或状态标签字节数组:" + Arrays.toString(dTagByteArray));//20200521进一步细化日志

            //获取数据对象类型编码,写成字节数组
            byte[] dataObjectTypeByteArray = sumHex(GetDataDictionaryCodeByTopicUtils.getDataObjectTypeCodeByTopicName(topic), 2);//长度为2

            //将数据标签长度,写成字节数组
            byte[] dataTagLengthByteArray = sumHex(dTagByteArray.length, 2);//长度为2

            //数据总长度(16+数据标签长度+数据对象长度),写成字节数组
            byte[] dataSumLengthByteArray = sumHex(16 + dTagByteArray.length + dataResultArray.length, 4);//长度为4

            //数据标签SchemaID,写成字节数组
            byte[] dataTagSchemaIDByteArray = sumHex(GetDataDictionaryCodeByTopicUtils.getDataTagSchemaIDByTopicName(topic), 4);//长度为4

            //数据对象SchemaID.写成字节数组
            byte[] dataObjectSchemaIDByteArray = sumHex(GetDataDictionaryCodeByTopicUtils.getDataObjectSchemaIDByTopicName(topic), 4);//长度为4

            //拼接这些字节数组并返回,顺序:数据对象类型+数据标签长度+数据总长度+数据标签SchemaID+数据对象SchemaID+数据标签数组+数据对象数组
            return byteMerger(dataObjectTypeByteArray, dataTagLengthByteArray, dataSumLengthByteArray, dataTagSchemaIDByteArray, dataObjectSchemaIDByteArray, dTagByteArray, dataResultArray);
        } catch (Exception e) {
            logger.error("HttpClientUtil getTagRecordAndMergeAllArray is error===>" + e.getMessage() + "<===message is==>" + dataJson + "<==");//细化日志
//            e.printStackTrace();
            return dataResultArray;//报错时返回数据对象数组,即本身不变
        }
    }

    /**
     * 由于已经获取了文件标签(是刚进入方法就作为数据对象处理的数据标签d_tag,因为NTC-COLLECT-FILE-LOG数据本身就是数据标签d_tag),所以只需要将所有相关数据字节数组化后拼接返回
     */
    private static byte[] alreadyGetFileTagRecordSoOnlyGetMergeAllArray(String topic, byte[] dataResultArray) {
        try {
            //获取数据对象类型编码,写成字节数组
            byte[] dataObjectTypeByteArray = sumHex(GetDataDictionaryCodeByTopicUtils.getDataObjectTypeCodeByTopicName(topic), 2);//长度为2

            //将数据标签长度,写成字节数组
            byte[] dataTagLengthByteArray = sumHex(dataResultArray.length, 2);//长度为2,由于是NTC-COLLECT-FILE-LOG.传来的数据对象dataResultArray其实就是最终上报数据的数据标签d_tag

            //数据总长度(16+数据标签长度+数据对象长度),写成字节数组
            byte[] dataSumLengthByteArray = sumHex(16 + dataResultArray.length, 4);//长度为4

            //数据标签SchemaID,写成字节数组
            byte[] dataTagSchemaIDByteArray = sumHex(GetDataDictionaryCodeByTopicUtils.getDataTagSchemaIDByTopicName(topic), 4);//长度为4

            //数据对象SchemaID.写成字节数组
            byte[] dataObjectSchemaIDByteArray = sumHex(GetDataDictionaryCodeByTopicUtils.getDataObjectSchemaIDByTopicName(topic), 4);//长度为4

            //拼接这些字节数组,顺序:数据对象类型+数据标签长度+数据总长度+数据标签SchemaID+数据对象SchemaID+数据标签数组,注意没有数据对象数组
            return byteMerger(dataObjectTypeByteArray, dataTagLengthByteArray, dataSumLengthByteArray, dataTagSchemaIDByteArray, dataObjectSchemaIDByteArray, dataResultArray);
        } catch (Exception e) {
            logger.error("HttpClientUtil alreadyGetFileTagRecordSoOnlyGetMergeAllArray is error===>" + e.getMessage() + "<===");//细化日志
//            e.printStackTrace();
            return dataResultArray;//报错时返回数据对象数组,即本身不变
        }
    }

    /**
     * 描述：把一个整数转为指定位数byte数组
     *
     * @param tu5    数字
     * @param length 位数
     * @return byte[]
     */
    public static byte[] sumHex(int tu5, int length) {
        byte[] bytes5 = new byte[length];
        while (length > 0) {
            length--;
//            bytes5[length] = (byte)(tu5 >> 8*(bytes5.length-length-1) & 0xFF);//大端
            bytes5[bytes5.length - length - 1] = (byte) (tu5 >> 8 * (bytes5.length - length - 1) & 0xFF);//小端
        }
        return bytes5;
    }

    /**
     * 合并字节数组
     *
     * @param byteList
     * @return
     */
    private static byte[] byteMerger(byte[]... byteList) {
        int lengthByte = 0;
        for (int i = 0; i < byteList.length; i++) {
            lengthByte += byteList[i].length;
        }
        byte[] allByte = new byte[lengthByte];
        int countLength = 0;
        for (int i = 0; i < byteList.length; i++) {
            byte[] b = byteList[i];
            System.arraycopy(b, 0, allByte, countLength, b.length);
            countLength += b.length;
        }
        return allByte;
    }

    /**
     * 获取X-Tag用于设置请求头
     *
     * @param dataJson
     * @return
     */
    private static String getXTAG(String dataJson, String topic) {
        if ("monitor-msg".equals(topic) || "INFLUX-SAPP-BPS-STAT-LOG".equals(topic)) {
            return RealtimeCountConfig.MONITOR_NOFILE_MSG_X_TAG;
        }

        Map map = JSONObject.parseObject(dataJson, Map.class);
//        String xTag = (String) map.get("x_tag");
        Object x_tag = map.get("x_tag");
        if (x_tag != null) {
            String xTag = x_tag.toString();
            if (StringUtil.isNotBlank(xTag)) {
                return xTag;
            } else {
                return "-";
            }
        } else {
            return "-";
        }
    }

}
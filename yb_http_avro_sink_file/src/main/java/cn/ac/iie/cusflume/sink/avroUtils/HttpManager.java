package cn.ac.iie.cusflume.sink.avroUtils;

import cn.ac.iie.cusflume.sink.CommonUtils.GetDataDictionaryCodeByTopicUtils;
import cn.ac.iie.cusflume.sink.CommonUtils.GetFilePathByTopicUtils;
import cn.ac.iie.cusflume.sink.HttpAsyncUtils.HttpClientUtil;
import cn.ac.iie.cusflume.sink.avroUtils.avroRecord.GetAvroRecordByTopicUtils;
import cn.ac.iie.cusflume.sink.avroUtils.avroSchema.GetAvroSchemaByTopicUtils;
import cn.ac.iie.cusflume.sink.bean.ac.Req.AcReqBody;
import cn.ac.iie.cusflume.sink.bean.ac.Res.AcResBody;
import cn.ac.iie.cusflume.sink.bean.producer.Res.ProResBody;
import cn.ac.iie.cusflume.sink.daoUtils.RealtimeCountConfig;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import javax.net.ssl.*;
import java.io.*;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.*;

//import org.apache.http.Header;


public class HttpManager {
    // 创建httpclient连接池
    private PoolingHttpClientConnectionManager httpClientConnectionManager = null;
    private CloseableHttpClient httpClient = null;
    //类初始化时，自动实例化，饿汉单例模式
    private static final HttpManager manager = new HttpManager();
    private static Logger logger = Logger.getLogger(HttpManager.class);

    private static HashMap<String, Schema> schemaHashMap = new HashMap<String, Schema>();//用于存放Schema

    public static HttpManager getInfoLoadInstance() {
        return manager;
    }

    private HttpManager() {
        //初始化httpClient
        initHttpClient();
        System.setProperty("sun.net.inetaddr.ttl", "300");
        System.setProperty("sun.net.inetaddr.negative.ttl", "10");
    }

    /**
     * 新版initHttpClient()---20200425注释
     */
    public void initHttpClient() {
        try {
            SSLContext ctx = SSLContext.getInstance(SSLConnectionSocketFactory.TLS);
            X509TrustManager tm = new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            };
            ctx.init(null, new TrustManager[]{tm}, null);
            SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(ctx, NoopHostnameVerifier.INSTANCE);
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.INSTANCE)
                    .register("https", socketFactory)
                    .build();
            /**
             * 新版httpClientConnectionManager
             */
            //创建httpclient连接池
            httpClientConnectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            //设置连接池最大数量
            httpClientConnectionManager.setMaxTotal(2000);
            //设置单个路由最大连接数量
            httpClientConnectionManager.setDefaultMaxPerRoute(400);
        } catch (Exception e) {
            e.printStackTrace();
        }

        httpClient = getHttpClient();
    }

    //请求重试机制
    HttpRequestRetryHandler myRetryHandler = new HttpRequestRetryHandler() {
        @Override
        public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
            if (executionCount >= 2) {
                // 超过两次则不再重试请求
                logger.error("http连接已重试" + executionCount + "次, 重试失败");
                return false;
            }
            if (exception instanceof InterruptedIOException) {
                // Timeout
                logger.info("InterruptedIOException, retry connection...");//新增-降低了上述的日志级别
                return true;
            }
            if (exception instanceof UnknownHostException) {
                // Unknown host
                return false;
            }
            if (exception instanceof ConnectTimeoutException) {
                logger.error("ConnectTimeoutException, 重试连接。。。");
                // Connection refused
                return true;
            }
            if (exception instanceof SSLException) {
                // SSL handshake exception
                return false;
            }
            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
            if (idempotent) {
                logger.error("request is idempotent, 重试连接。。。");
                // Retry if the request is considered idempotent
                return true;
            }
            return false;
        }
    };

    public CloseableHttpClient getHttpClient() {
        // 创建全局的requestConfig
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(3000)
                .setSocketTimeout(3000)
                //.setCookieSpec(CookieSpecs.BEST_MATCH)
                .build();
        // 声明重定向策略对象
        LaxRedirectStrategy redirectStrategy = new LaxRedirectStrategy();


       /* ConnectionKeepAliveStrategy myStrategy = (response, context) -> {
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
            return 10 * 1000;//如果没有约定，则默认定义时长为60s
        };*/

        /**
         * 原版
         */
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(httpClientConnectionManager)
                .setDefaultRequestConfig(requestConfig)
                .setRedirectStrategy(redirectStrategy)
                .setRetryHandler(myRetryHandler)
               // .setKeepAliveStrategy(myStrategy)
                .build();

        return httpClient;
    }

    public String getAddress() {
        //ArrayList<String> addrs = new ArrayList<String>();
//		String[] addrs = "http://10.208.133.172:10080,http://10.208.133.173:10080".split(",");
        String[] addrs = "http://10.208.133.172:10080,http://10.208.133.173:10080".split(",");

        Random rnd = new Random();
        Integer addrIndex = rnd.nextInt(addrs.length);
        return addrs[addrIndex].trim();
    }

    /**
     * 生产AVRO数据入ZX(批次)--数据不包含schema
     *
     * @param urlProducer
     * @param topic
     * @param schemaStr
     * @param avroBatchList
     * @param userAgent
     * @param msgSessionCookie
     * @return
     */
    public ProResBody producerAvroToZX(String urlProducer, String topic, String schemaStr, List<String> avroBatchList, String userAgent, String msgSessionCookie) {
        CloseableHttpResponse response = null;
        HttpPost httpPost = null;
        urlProducer = urlProducer.trim();
        ProResBody proResBody = null;
        byte[] resultArray = null;//用于存放avro的二进制流
//        JsonAvroConverter converter = new JsonAvroConverter();
        try {
            // get schema
            Schema schemaAvro = new Schema.Parser().parse(schemaStr);
            ByteArrayOutputStream outAvro = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outAvro, null);
            DatumWriter<GenericRecord> fileWriter = new SpecificDatumWriter<GenericRecord>(schemaAvro);
            outAvro.reset();
            for (String dataJson : avroBatchList) {
                GenericRecord recordAvro = new GenericData.Record(schemaAvro);
//                GenericRecord recordAvro = converter.convertToGenericDataRecord(dataJson.getBytes(), schemaAvro);
//                recordAvro = GetAvroRecordByTopicUtils.getAvroRecordByTopicName(topic, recordAvro, dataJson, schemaStr);//批量
                recordAvro = GetAvroRecordByTopicUtils.getAvroRecordByTopicName(topic, recordAvro, dataJson, schemaAvro);//批量

                logger.info("封装AvroRecord后数据为===>" + recordAvro.toString() + "<===封装AvroRecord后数据");

                fileWriter.write(recordAvro, encoder);
            }

            if (encoder != null) {
                encoder.flush();
            }

            if (outAvro != null) {
                resultArray = outAvro.toByteArray();

                outAvro.flush();
                outAvro.close();
                outAvro = null;
            }

            httpPost = new HttpPost(urlProducer);
            // set header
            httpPost.addHeader("User-Agent", userAgent);
            httpPost.addHeader("Cookie", msgSessionCookie);

            try {
                String md5Avro = MD5Utils.md5Encode(resultArray);
                httpPost.addHeader("Checksum", md5Avro);
                logger.info("请求端Checksum MD5 avro 加密为:" + md5Avro);
            } catch (Exception e) {
                logger.error("MD5Utils.md5Encode Method is error,this data is " + resultArray);
                e.printStackTrace();
            }
//            httpPost.addHeader("Content-Type", "binary/octet-stream");
//            httpPost.addHeader("Content-Type", "application/avro+json;charset=UTF-8");
            httpPost.addHeader("Content-Type", "binary/octet-stream");

            //新增调度标签--------------20191210----------↓-----------
            //注意批次发送这里不需要传FilePath头部
//            Map map = JSONObject.parseObject(avroBatchList.get(0), Map.class);
//            String xTag = (String) map.get("x_tag");
            httpPost.addHeader("X-Tag", getXTAG(avroBatchList.get(0), topic));//批次,第一条,因为必然存在至少一条
//            httpPost.addHeader("FilePath", "-");//因为无文件,所以设置为空;//20191216新增注释:批次发送的不设置FilePath头部
            //新增调度标签--------------20191210----------↑-----------

            ByteArrayEntity payload = new ByteArrayEntity(resultArray);
            payload.setContentEncoding("utf-8");
            //payload.setContentType("text/xml; charset=UTF-8");
            // anti avro
            httpPost.setEntity(payload);

            //%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%往下
            logger.info("加载内容字节数组长度: " + resultArray.length);
            //%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%往上

            //执行请求
            response = httpClient.execute(httpPost);
            try {
                int statuCode = response.getStatusLine().getStatusCode();
                HttpEntity entity = response.getEntity();
                String ret = EntityUtils.toString(entity);
                logger.info("返回的生产原始响应体String数据为:" + ret);
                proResBody = JSONObject.parseObject(ret, ProResBody.class);
                logger.info("封装入对象的生产响应体ProResBody为:" + JSONObject.toJSONString(proResBody));
                if (statuCode == 200) {
                    logger.info("数据加载成功, 返回码: " + statuCode);
                    AvroMonitorTimerTask.msgSuccessSum += avroBatchList.size();
                    EntityUtils.consume(entity);
                } else {
                    EntityUtils.consume(entity);
//                    logger.error("数据加载失败: " + ret + " --- code: " + statuCode + " ---失败数据为: \n" + records.get(0).toString());//单条测试用
                    logger.error("数据加载失败: " + ret + " --- code: " + statuCode + " ---失败数据为: \n" + avroBatchList);//测试用
                    AvroMonitorTimerTask.msgFailedSum += avroBatchList.size();
//                    logger.error("数据加载失败: " + ret + " --- code: " + statuCode + "\n");
                }
            } catch (Exception e) {
                logger.error("Get response from ZX is error===>>>" + e + "<<<===");
                e.printStackTrace();
            }
        } catch (MalformedURLException e) {
            //执行URL url = new URL()的异常
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            // 执行httpClient.execute(httpGet)的异常
            e.printStackTrace();
        } catch (IOException e) {
            // 执行httpClient.execute(httpGet)的异常
            logger.error("producerAvroToZX is IOException===>>>" + e + "<<<===");
            e.printStackTrace();
        } catch (Exception e) {
            //handle response here... try other servers
            logger.error("producerAvroToZX is Exception===>>>" + e + "<<<===");
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            httpPost.abort();
        }
        return proResBody;
    }

    /**
     * 生产AVRO数据入ZX(单条)--数据不包含schema
     * 带返回值,用于同步请求(非多线程)
     *
     * @param urlProducer
     * @param topic
     * @param schemaStr
     * @param dataJson
     * @param userAgent
     * @param msgSessionCookie
     * @return
     */
    public ProResBody producerAvroToZX(String urlProducer, String topic, String schemaStr, String dataJson, String userAgent, String msgSessionCookie) {
        CloseableHttpResponse response = null;
        HttpPost httpPost = null;
        urlProducer = urlProducer.trim();
        ProResBody proResBody = null;
        byte[] resultArray = null;//用于存放avro的二进制流
//        JsonAvroConverter converter = new JsonAvroConverter();
        try {
            // get schema

//            Schema schemaDataAvro = new Schema.Parser().parse(schemaStr);//旧版-20191224移除,改用hm获取
            Schema schemaDataAvro = getSchemaFromHashMap(topic);//新版-20191224-使用缓存优化性能
            ByteArrayOutputStream outAvro = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outAvro, null);
            DatumWriter<GenericRecord> fileWriter = new SpecificDatumWriter<GenericRecord>(schemaDataAvro);
            outAvro.reset();

            GenericRecord recordAvro = new GenericData.Record(schemaDataAvro);
//                GenericRecord recordAvro = converter.convertToGenericDataRecord(dataJson.getBytes(), schemaAvro);
//            recordAvro = GetAvroRecordByTopicUtils.getAvroRecordByTopicName(topic, recordAvro, dataJson, schemaStr);//单条
            recordAvro = GetAvroRecordByTopicUtils.getAvroRecordByTopicName(topic, recordAvro, dataJson, schemaDataAvro);//单条
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

            if (!("NTC-COLLECT-FILE-LOG".equals(topic))) {
                //传入的resultArray为数据对象的数组,返回的是拼接的整体的数组
                resultArray = getTagRecordAndMergeAllArray(topic, dataJson, resultArray);
            } else {
                //说明为NTC-COLLECT-FILE-LOG,此时不需要进行d_tag的获取,因为数据里不包含,前面处理的resultArray就作为NTC-COLLECT-FILE-LOG的d_tag使用
                resultArray = alreadyGetFileTagRecordSoOnlyGetMergeAllArray(topic, resultArray);
            }

            httpPost = new HttpPost(urlProducer);
            // set header
            httpPost.addHeader("User-Agent", userAgent);

            //一部状态回传需要此Cookie打开
//            if ("monitor-msg".equals(topic)) {
//                httpPost.addHeader("Cookie", msgSessionCookie);//广东文件消息测试时加上Cookie会验证不通过,即那边显示为两个Cookie,不加Cookie则验证通过
//            }

            httpPost.addHeader("Cookie", msgSessionCookie);//不设置Cookie时,广东测试出现报错,打开广东的Cookie设置测试一下,经测试,可用

            try {
                String md5Avro = MD5Utils.md5Encode(resultArray);
                httpPost.addHeader("Checksum", md5Avro);
                logger.info("请求端Checksum MD5 avro 加密为:" + md5Avro);
            } catch (Exception e) {
                logger.error("MD5Utils.md5Encode Method is error,this data is " + resultArray);
                e.printStackTrace();
            }
//            httpPost.addHeader("Content-Type", "binary/octet-stream");
//            httpPost.addHeader("Content-Type", "application/avro+json;charset=UTF-8");
            httpPost.addHeader("Content-Type", "binary/octet-stream");

            //设置调度标签要求的参数,文件消息需要设置FilePath--------------------↓--------------------20191210新增
            httpPost.addHeader("X-Tag", getXTAG(dataJson, topic));//从json中获取-单条

            String filePathByTopicName = GetFilePathByTopicUtils.getFilePathByTopicName(dataJson, topic);
            if (!("-".equals(filePathByTopicName)) && !("[\"null\"]".equals(filePathByTopicName))) {
                httpPost.addHeader("FilePath", filePathByTopicName);//从json中获取
            }
            //设置调度标签要求的参数,文件消息需要设置FilePath-------------------↑---------------------20191210新增

            //尝试消除  Invalid cookie header: "Set-Cookie:  SESSION=""; expires=Thu, 01 Jan 1970 00:00:00 GMT; Max-Age=0; Path=/". Invalid 'expires' attribute: Thu, 01 Jan 1970 00:00:00 GMT
            RequestConfig defaultConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build();
            httpPost.setConfig(defaultConfig);

            //测试用
            Header[] allHeaders = httpPost.getAllHeaders();
            for (int i = 0; i < allHeaders.length; i++) {
                logger.info("allHeaders[" + i + "].getName()====>>>" + allHeaders[i].getName() + "##### allHeaders[" + i + "].getValue()=======>>>" + allHeaders[i].getValue());
            }

            ByteArrayEntity payload = new ByteArrayEntity(resultArray);
            payload.setContentEncoding("utf-8");
            //payload.setContentType("text/xml; charset=UTF-8");
            // anti avro
            httpPost.setEntity(payload);

            //%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%往下
            logger.info("最终加载内容字节数组长度: " + resultArray.length);
            //%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%往上

            //执行请求
            response = httpClient.execute(httpPost);
            try {
                int statuCode = response.getStatusLine().getStatusCode();
                HttpEntity entity = response.getEntity();
                String ret = EntityUtils.toString(entity);
                logger.info("返回的生产原始响应体String数据为:" + ret);
                proResBody = JSONObject.parseObject(ret, ProResBody.class);
                logger.info("封装入对象的生产响应体ProResBody为:" + JSONObject.toJSONString(proResBody));
                if (statuCode == 200) {
                    logger.info("数据加载成功, 返回码: " + statuCode);
                    AvroMonitorTimerTask.msgSuccessSum++;
                    EntityUtils.consume(entity);
                } else {
                    EntityUtils.consume(entity);
//                    logger.error("数据加载失败: " + ret + " --- code: " + statuCode + " ---失败数据为: \n" + records.get(0).toString());//单条测试用
                    logger.error("数据加载失败: " + ret + " --- code: " + statuCode + " ---失败数据为: \n" + dataJson);//测试用
                    AvroMonitorTimerTask.msgFailedSum++;
//                    logger.error("数据加载失败: " + ret + " --- code: " + statuCode + "\n");
                }
            } catch (Exception e) {
                logger.error("Get response from ZX is error===>>>" + e + "<<<===");
                e.printStackTrace();
            }
        } catch (MalformedURLException e) {
            //执行URL url = new URL()的异常
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            // 执行httpClient.execute(httpGet)的异常
            e.printStackTrace();
        } catch (IOException e) {
            // 执行httpClient.execute(httpGet)的异常
            logger.error("producerAvroToZX is IOException===>>>" + e + "<<<===");
            e.printStackTrace();
        } catch (Exception e) {
            //handle response here... try other servers
            logger.error("producerAvroToZX is Exception===>>>" + e + "<<<===");
            logger.error("message is " + dataJson);
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            httpPost.abort();
        }
        return proResBody;
    }

    /**
     * 使用hm缓存Schema
     *
     * @param topic
     * @return
     */
    private Schema getSchemaFromHashMap(String topic) {
        if (schemaHashMap.containsKey(topic)) {
            return schemaHashMap.get(topic);
        } else {
            String schemaStr = GetAvroSchemaByTopicUtils.getAvroSchemaByTopicName(topic);
            Schema parseSchema = new Schema.Parser().parse(schemaStr);
            schemaHashMap.put(topic, parseSchema);
            return parseSchema;
        }
    }

    /**
     * 获取X-Tag用于设置请求头
     *
     * @param dataJson
     * @return
     */
    private String getXTAG(String dataJson, String topic) {
        if ("monitor-msg".equals(topic)) {
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

    //我自己修改的部分-20190910-JSON版本
    public void producerJsonToZX(String urlProducer, String topic, String data, String userAgent, String MsgSessionCookie) {
        CloseableHttpResponse response = null;
        HttpPost httpPost = null;
        urlProducer = urlProducer.trim();
        ProResBody proResBody;
        try {
            httpPost = new HttpPost(urlProducer);
//			httpPost.addHeader("Connection","keep-alive");//可能影响入库
//			httpPost.addHeader("Accept-Encoding", "gzip, deflate");//可能影响入库
            //httpPost.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36");
            httpPost.addHeader("User-Agent", userAgent);
            httpPost.addHeader("Cookie", MsgSessionCookie);
            try {
                httpPost.addHeader("Checksum", MD5Utils.md5Encode(data));
                logger.info("请求端Checksum MD5 加密为:" + MD5Utils.md5Encode(data));
            } catch (Exception e) {
                logger.error("MD5Utils.md5Encode Method is error,this data is " + data);
                e.printStackTrace();
            }
            httpPost.addHeader("Content-Type", "binary/octet-stream");
            httpPost.addHeader("Topic", topic);
            StringEntity payload = new StringEntity(data, Charset.forName("utf-8"));//原版
//			StringEntity payload = new StringEntity(data);//自己修改测试版20190219
            //payload.setContentType("text/xml; charset=UTF-8");
            payload.setContentEncoding("utf-8");//原版,测试中暂不使用20190219
            httpPost.setEntity(payload);
            logger.info("加载内容: " + data);
            //执行请求
            response = httpClient.execute(httpPost);
            try {
                int statuCode = response.getStatusLine().getStatusCode();
                HttpEntity entity = response.getEntity();
                String ret = EntityUtils.toString(entity);
                logger.info("原本的返回的响应体String数据为:" + ret);
//                ret = ret.replace("\\\"", "\"")
//                        .replace("\"{", "{")
//                        .replace("}\"}", "}}");//注意这里和AC时不一样
                proResBody = JSONObject.parseObject(ret, ProResBody.class);
                logger.info("封装入对象的响应体ProResBody:" + proResBody.toString());
                if (statuCode == 200) {
                    logger.info("数据加载成功, 返回码: " + statuCode);
                    EntityUtils.consume(entity);
                } else {
                    EntityUtils.consume(entity);
                    logger.error("数据加载失败: " + ret + " --- code: " + statuCode + " ---失败数据为: \n" + data);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (MalformedURLException e) {
            //执行URL url = new URL()的异常
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            // 执行httpClient.execute(httpGet)的异常
            e.printStackTrace();
        } catch (IOException e) {
            // 执行httpClient.execute(httpGet)的异常
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            httpPost.abort();
        }
    }

    /**
     * ZX认证,获取sessionId
     *
     * @param checkMsgUrl
     * @param userAgent
     * @param xTag
     * @param requestType
     * @return
     */
    public AcResBody checkAcByZx(String checkMsgUrl, String userAgent, String xTag, int requestType) {
        CloseableHttpResponse response = null;
        HttpPost httpPost = null;
        AcResBody acResBody = null;
        String url = checkMsgUrl.trim();

        //bean类型
        AcReqBody acReqBody = new AcReqBody();
        acReqBody.setRequestType(requestType);
        String data = JSONObject.toJSONString(acReqBody);//data为json版本

        try {
            httpPost = new HttpPost(url);
            httpPost.addHeader("User-Agent", userAgent);
            httpPost.addHeader("X-Tag", xTag);//根据最新文档,目前已经不需要此头-20191217
            httpPost.addHeader("Content-Type", "application/json");
           // httpPost.setHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
            StringEntity payload = new StringEntity(data, Charset.forName("utf-8"));
            //payload.setContentType("text/xml; charset=UTF-8");
            payload.setContentEncoding("utf-8");
            httpPost.setEntity(payload);
            logger.info("入库验证加载内容请求体: " + data);
            //执行请求
            response = httpClient.execute(httpPost);
            try {
                int statuCode = response.getStatusLine().getStatusCode();
                HttpEntity entity = response.getEntity();
                String ret = EntityUtils.toString(entity);
                logger.info("返回的验证原始响应体的String数据===>" + ret);//{"code":200,"msg":"请求成功","data":{"status":0,"reason":"审核通过"}}
                acResBody = JSONObject.parseObject(ret, AcResBody.class);

                Header[] allHeaders = response.getAllHeaders();
                for (Header header : allHeaders) {
                    if ("Set-Cookie".equals(header.getName())) {
                        acResBody.setSessionId(header.getValue().split(";")[0]);
                    }
                }

                logger.info("封装的验证响应体对象AcResBody数据为===>" + acResBody.toString());
                if (statuCode == 200) {
                    logger.info("数据加载成功, 返回码: " + statuCode);
                    EntityUtils.consume(entity);
                } else {
                    EntityUtils.consume(entity);
                    logger.error("数据加载失败: " + ret + " --- code: " + statuCode + " ---失败数据为: \n" + data);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (MalformedURLException e) {
            //执行URL url = new URL()的异常
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            // 执行httpClient.execute(httpGet)的异常
            e.printStackTrace();
        } catch (IOException e) {
            // 执行httpClient.execute(httpGet)的异常
            e.printStackTrace();
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consumeQuietly(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    logger.error("释放链接错误: " + e.getMessage());
                }
            }
            httpPost.abort();
        }
        return acResBody;
    }

    //老版
    public void postToDataCenter(String url, String topic, String data) {
        CloseableHttpResponse response = null;
        HttpPost httpPost = null;
        url = url.trim();
        try {
            httpPost = new HttpPost(url);
//			httpPost.addHeader("Connection","keep-alive");//可能影响入库
//			httpPost.addHeader("Accept-Encoding", "gzip, deflate");//可能影响入库
            //httpPost.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36");

//			httpPost.addHeader("User", RealtimeCountConfig.DATACENTER_USERNAME);
//            httpPost.addHeader("Password", RealtimeCountConfig.DATACENTER_PASSWORD);
            httpPost.addHeader("Topic", topic);
            httpPost.addHeader("Schema-Version", "2");//测试新增20190219,原版没有
            httpPost.addHeader("Format", "csv");
//            httpPost.addHeader("Row-Split", "\\n");
//            httpPost.addHeader("Field-Split", "\\t");
            httpPost.addHeader("Row-Split", "\\n");
            httpPost.addHeader("Field-Split", ",");
//            StringEntity payload = new StringEntity(data, Charset.forName("utf-8"));//原版
            StringEntity payload = new StringEntity(data);//自己修改测试版20190219
            //payload.setContentType("text/xml; charset=UTF-8");
//            payload.setContentEncoding("utf-8");//原版,测试中暂不使用20190219
            httpPost.setEntity(payload);
            logger.info("数据中心加载内容: " + data);
            //执行请求
            response = httpClient.execute(httpPost);
            try {
                int statuCode = response.getStatusLine().getStatusCode();
                //Header[] headers = response.getAllHeaders();
                //logger.info("<<response header>>:");
                //System.out.println("<<response header>>:");
                //for(int i=0; i<headers.length; i++){
                //	logger.info(headers[i].getName() +"  :  "+headers[i].getValue());
                //System.out.println(headers[i].getName() +"  :  "+headers[i].getValue());
                //}
                HttpEntity entity = response.getEntity();
                if (statuCode == 200) {
                    logger.info("数据中心加载成功, 返回码: " + statuCode);
                    System.out.println("数据中心加载成功, 返回码: " + statuCode);
                    EntityUtils.consume(entity);
                } else {
                    String ret = EntityUtils.toString(entity);
                    EntityUtils.consume(entity);
                    logger.info("数据中心加载失败: " + ret + " --- code: " + statuCode + " ---失败数据为: \n" + data);
                    System.out.println("数据中心加载失败: " + ret + " --- code: " + statuCode + " ---失败数据为: \n" + data);
                    logger.error("数据中心加载失败: " + ret + " --- code: " + statuCode);
                    System.out.println("数据中心加载失败: " + ret + " --- code: " + statuCode);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (MalformedURLException e) {
            //执行URL url = new URL()的异常
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            // 执行httpClient.execute(httpGet)的异常
            e.printStackTrace();
        } catch (IOException e) {
            // 执行httpClient.execute(httpGet)的异常
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            httpPost.abort();
            /**
             * httpclient的链接有线程池管理，这里不用直接关闭
             */
//			try {//关闭连接
//				httpClient.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
        }
    }

    /**
     * 获取数据总共字节数
     *
     * @param avroBatchList
     * @return
     */
    private long getStrBytes(List<String> avroBatchList) {
        long countThisListStr = 0L;
        for (String str : avroBatchList) {
            countThisListStr += str.getBytes().length;
        }
        return countThisListStr;
    }

    /**
     * 获取数据中的日志标签并将所有相关数据字节数组化后拼接返回
     *
     * @param topic
     * @param dataJson
     * @param dataResultArray
     * @return
     */
    private byte[] getTagRecordAndMergeAllArray(String topic, String dataJson, byte[] dataResultArray) {
        byte[] dTagByteArray = null;//用于存放数据标签的avro的二进制流
        String tagTopicName = "log-tag";//除NTC-COLLECT-FILE-LOG外都是log-tag,因为都是在文件消息内部的d_tag获取的
        if ("monitor-msg".equals(topic)) {
            tagTopicName = "status-tag";
        }
        try {
            // get schema
            //---------20191224移除,改用hm获取-----------↓------
//            String schemaTag = GetAvroSchemaByTopicUtils.getAvroSchemaByTopicName(tagTopicName);//单条-"log-tag"或者"status-tag"
//            Schema schemaDTagAvro = new Schema.Parser().parse(schemaTag);//旧版
            //---------20191224移除-----------↑------

            Schema schemaDTagAvro = getSchemaFromHashMap(tagTopicName);//新版-20191224-使用缓存优化性能
            ByteArrayOutputStream outAvro = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outAvro, null);
            DatumWriter<GenericRecord> fileWriter = new SpecificDatumWriter<GenericRecord>(schemaDTagAvro);
            outAvro.reset();

            GenericRecord recordAvro = new GenericData.Record(schemaDTagAvro);

            //以下为临时,为了增加一个状态的临时d_tag,因为d_tag前端目前(20191222)传不了-----------↓--------------
            if (!("monitor-msg".equals(topic))) {
                //说明当前消息不是状态回传
                Map map = JSONObject.parseObject(dataJson, Map.class);
                Object d_tag = map.get("d_tag");
                if (d_tag != null) {
                    dataJson = d_tag.toString();//获取消息上的d_tag用于序列化成数据标签
                }
            } else {
                //说明为状态,目前前端没有,所以先自己创建一个状态的d_tag
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String timeFormat = sdf.format(new Date(System.currentTimeMillis()));//格式2019-11-18 19:56:37
                dataJson = "{\"tag_version\":\"1.0\",\"data_subtype\":16387,\"data_type\":4,\"producer_id\":24832,\"timestamp\":\"yyyy-MM-dd HH:mm:ss\"}";
                Map map = JSONObject.parseObject(dataJson, Map.class);
                map.put("timestamp", timeFormat);
                dataJson = JSONObject.toJSONString(map);
            }
            //以下为临时,为了增加一个状态的临时d_tag,因为d_tag前端目前(20191222)传不了-----------↑--------------

//            recordAvro = GetAvroRecordByTopicUtils.getAvroRecordByTopicName(tagTopicName, recordAvro, dataJson, schemaTag);//单条-"log-tag"或者"status-tag"
            recordAvro = GetAvroRecordByTopicUtils.getAvroRecordByTopicName(tagTopicName, recordAvro, dataJson, schemaDTagAvro);//单条-"log-tag"或者"status-tag"
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
            logger.info("日志标签dTagByteArray长度为:" + dTagByteArray.length);

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
            logger.error("HttpManager getTagRecordAndMergeAllArray is error===>" + e + "<===");
            e.printStackTrace();
            return dataResultArray;//报错时返回数据对象数组,即本身不变
        }
    }

    /**
     * 由于已经获取了文件标签,所以只需要将所有相关数据字节数组化后拼接返回
     */
    private byte[] alreadyGetFileTagRecordSoOnlyGetMergeAllArray(String topic, byte[] dataResultArray) {
        try {
            //获取数据对象类型编码,写成字节数组
            byte[] dataObjectTypeByteArray = sumHex(GetDataDictionaryCodeByTopicUtils.getDataObjectTypeCodeByTopicName(topic), 2);//长度为2

            //将数据标签长度,写成字节数组
            byte[] dataTagLengthByteArray = sumHex(dataResultArray.length, 2);//长度为2,由于是NTC-COLLECT-FILE-LOG.传来的数据对象其实就是数据标签

            //数据总长度(16+数据标签长度+数据对象长度),写成字节数组
            byte[] dataSumLengthByteArray = sumHex(16 + dataResultArray.length, 4);//长度为4

            //数据标签SchemaID,写成字节数组
            byte[] dataTagSchemaIDByteArray = sumHex(GetDataDictionaryCodeByTopicUtils.getDataTagSchemaIDByTopicName(topic), 4);//长度为4

            //数据对象SchemaID.写成字节数组
            byte[] dataObjectSchemaIDByteArray = sumHex(GetDataDictionaryCodeByTopicUtils.getDataObjectSchemaIDByTopicName(topic), 4);//长度为4

            //拼接这些字节数组,顺序:数据对象类型+数据标签长度+数据总长度+数据标签SchemaID+数据对象SchemaID+数据标签数组+数据对象数组
            return byteMerger(dataObjectTypeByteArray, dataTagLengthByteArray, dataSumLengthByteArray, dataTagSchemaIDByteArray, dataObjectSchemaIDByteArray, dataResultArray);
        } catch (Exception e) {
            logger.error("HttpManager alreadyGetFileTagRecordSoOnlyGetMergeAllArray is error===>" + e + "<===");
            e.printStackTrace();
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
}

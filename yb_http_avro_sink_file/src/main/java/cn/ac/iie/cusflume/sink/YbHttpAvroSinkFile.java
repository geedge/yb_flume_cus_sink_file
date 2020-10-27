package cn.ac.iie.cusflume.sink;

import cn.ac.iie.cusflume.sink.HttpAsyncUtils.HttpClientUtil;
import cn.ac.iie.cusflume.sink.avroUtils.AvroMonitorTimerTask;
import cn.ac.iie.cusflume.sink.avroUtils.DataCenterLoad;
import cn.ac.iie.cusflume.sink.bean.ac.Res.AcResBody;
import cn.ac.iie.cusflume.sink.bean.configBean.ConfigInfo;
import cn.ac.iie.cusflume.sink.daoUtils.RealtimeCountConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

public class YbHttpAvroSinkFile extends AbstractSink implements Configurable {

    private static Logger logger = Logger.getLogger(YbHttpAvroSinkFile.class);
    protected static ExecutorService pool = Executors.newFixedThreadPool(RealtimeCountConfig.HTTP_ASYNC_PARALLELISM);
    private static DataCenterLoad dcl;

    private static String singleProduceUrl;//单条发送接口
    private static String batchProduceUrl; //批量发送接口

    private String postFileUrl;//发送文件路径,配置文件获取,仅发送文件时需要---若只发送消息,则此路径与postMsgUrl设置相同即可

    private static int batchSize;//配置文件获取,每次从channel中取出的数据条数

    //http验证
    private static String checkMsgUrl;//消息认证地址,配置文件获取,发送文件与发送消息皆需要

    private String checkFileUrl;//文件认证地址,配置文件获取,仅发送文件时需要---若只发送消息,则此路径与checkUrl设置相同即可---暂未设置配置文件获取步骤

    private static String userAgent;//业务系统编码systemId,该字段为系统的唯一编码,配置文件获取

    private static String xTag;//标签编码tag,在总线中唯一标识该标签,配置文件获取--20191217笔记--貌似现在已经不需要这个参数作为头部了
    private static String dataJson;

    private static String msgSessionCookie;//消息会话标识,由响应返回
    private static String fileSessionCookie;//文件会话标识,由响应返回,仅发送文件时需要---若只发送消息,则fileSessionCookie会一直为空-即仅广东需要

    private static String monitorSessionCookie;//状态回传会话标识,由响应返回

    private static String monitorMsgXTag = RealtimeCountConfig.MONITOR_MSG_SINK_FILE_XTAG;//20191217更新-因为此参数为总线提供,此处固定,提取到realtime_service_config.properties配置文件中

    private static ConfigInfo configInfo = new ConfigInfo();//用于存放文件与消息的验证,发送以及会话标识

    //http入库
    private static String topicName;//配置文件获取,表示入库表结构为哪一个topic,后面也将根据此topic获取avro的schema

    private static boolean monitorStart = false;

    private static boolean checkTimerStart = false;//定时获取Cookie启动器





    /**
     * 用于存放验证以及连接的url的各组成部分，方便调取
     */
    private static HashMap<String, String> urlToolHm;

    public YbHttpAvroSinkFile() {
        logger.warn("YbHttpAvroSinkFile start ... ...");
    }

    @Override
    public synchronized void start() {
        super.start();
        dcl = new DataCenterLoad();

        /**
         * 拆解初始化获取的url后缀，用于填充urlToolHm，用于后续动态负载均衡中的url变更
         */
        if (StringUtils.isNotBlank(checkMsgUrl)
                && StringUtils.isNotBlank(singleProduceUrl)
                && StringUtils.isNotBlank(batchProduceUrl)) {
            urlToolHm = new HashMap<>();
            makeUrlSplitMap(checkMsgUrl, "check");
            makeUrlSplitMap(singleProduceUrl, "post_single");
            makeUrlSplitMap(batchProduceUrl, "post_batch");

        } else {
            logger.error("Starting YbHttpAvroSinkFile is error==>checkMsgUrl and postMsgUrl can not be null!!!!");
        }

        new Thread(new RetrySendMessage()).start();
        logger.warn("Starting YbHttpAvroSinkFile ... ...");
    }

    @Override
    public void configure(Context context) {
        try {
            singleProduceUrl = context.getString("singleProduceUrl", "");
            Preconditions.checkNotNull("".equals(singleProduceUrl), "singleProduceUrl must be set!!");
            logger.info("Read Post Message URL from configuration : " + singleProduceUrl);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Endpoint Message URL invalid", e);
        } catch (Exception e) {
            logger.error("Get singleProduceUrl is error : " + e);
        }

        try {
            batchProduceUrl = context.getString("batchProduceUrl", "");
            Preconditions.checkNotNull("".equals(batchProduceUrl), "batchProduceUrl must be set!!");
            logger.info("Read Post Message URL from configuration : " + batchProduceUrl);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Endpoint Message URL invalid", e);
        } catch (Exception e) {
            logger.error("Get batchProduceUrl is error : " + e);
        }

        try {
            batchSize = context.getInteger("batchSize", 100);
            Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
            logger.info("Read BatchSize from configuration : " + batchSize);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("BatchSize invalid", e);
        } catch (Exception e) {
            logger.error("Get batchSize is error : " + e);
        }

        try {
            checkMsgUrl = context.getString("checkMsgUrl", "");
            Preconditions.checkNotNull("".equals(checkMsgUrl), "checkMsgUrl must be set!!");
            logger.info("Read Check Message Url from configuration : " + checkMsgUrl);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Check Message URL invalid", e);
        } catch (Exception e) {
            logger.error("Get checkMsgUrl is error : " + e);
        }

        try {
            userAgent = context.getString("userAgent", "");
            Preconditions.checkNotNull("".equals(userAgent), "userAgent must be set!!");
            logger.info("Read UserAgent from configuration : " + userAgent);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("UserAgent invalid", e);
        } catch (Exception e) {
            logger.error("Get userAgent is error : " + e);
        }

        try {
            xTag = context.getString("xTag", "");
            Preconditions.checkNotNull("".equals(xTag), "xTag must be set!!");
            logger.info("Read XTag from configuration : " + xTag);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("XTag invalid", e);
        } catch (Exception e) {
            logger.error("Get xTag is error : " + e);
        }

        try {
            topicName = context.getString("topicName", "");
            Preconditions.checkNotNull("".equals(topicName), "topicName must be set!!");
            logger.info("Read topicName from configuration : " + topicName);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("topicName invalid", e);
        } catch (Exception e) {
            logger.error("Get topicName is error : " + e);
        }

    }


    @Override
    public synchronized void stop() {
        super.stop();
        logger.warn("Stopping YbHttpAvroSinkFile ... ...");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        try {
            logger.debug("Current Process Thread number ID ：" + Thread.currentThread().getId());
            transaction = channel.getTransaction();
            transaction.begin();
            Event event = null;
            String content = null;
            List<String> contents = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {
                    content = new String(event.getBody());
                    contents.add(content);
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }
            if (contents.size() > 0) {
                switch (topicName) {
                    //作为单条发送-新分类-20191219
                    /**
                     * 非文件消息：NTC-CONN-RECORD-LOG,NTC-COLLECT-DNS-LOG,NTC-COLLECT-SSL-LOG
                     * 文件类消息：NTC-COLLECT-FILE-LOG（有独立文件标签）、
                     *  NTC-COLLECT-HTTP-DOC-LOG、NTC-COLLECT-HTTP-AV-LOG、NTC-COLLECT-FTP-DOC-LOG、
                     *  NTC-COLLECT-MAIL-LOG、NTC-COLLECT-TELNET-LOG
                     *  状态消息：INFLUX-SAPP-BPS-STAT-LOG（读取回写的influxDB合计数据用作状态上传）
                     */
                    case "NTC-CONN-RECORD-LOG":
                    case "NTC-COLLECT-DNS-LOG":
                    case "NTC-COLLECT-SSL-LOG":
                    case "NTC-COLLECT-FILE-LOG":
                    case "NTC-COLLECT-HTTP-DOC-LOG":
                    case "NTC-COLLECT-HTTP-AV-LOG":
                    case "NTC-COLLECT-FTP-DOC-LOG":
                    case "NTC-COLLECT-MAIL-LOG":
                    case "NTC-COLLECT-TELNET-LOG":
                    case "INFLUX-SAPP-BPS-STAT-LOG":
                        sendMsgLog(transaction, contents);
                        break;
                    default:
                        logger.error("YbHttpAvroSinkFile can't find this topic:" + topicName + ".Please confirm this topicName is correct!!!");
                        break;
                }
            } else {
                transaction.commit();
            }
        } catch (Exception e) {
            try {
                if (transaction != null) {
                    transaction.rollback();
                }
            } catch (Exception e2) {
                logger.error("Exception in rollback. Rollback might not have been successful.", e2);
            }
            logger.error("Failed to commit transaction.Transaction rolled back.", e);
            Throwables.propagate(e);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
        return result;
    }

    /**
     * 获取消息SessionCookie
     */
    private static void getMsgSessionCookie() {
        AcResBody acCheckMsgResBody;
        int requestType = 10;
        int statusCode = 0;
        acCheckMsgResBody = acCheck(checkMsgUrl, userAgent, xTag, requestType);//getMsgSessionCookie()
        if (acCheckMsgResBody != null) {
            statusCode = acCheckMsgResBody.getCode();
        }
        if (statusCode == 200) {
            if (StringUtils.isNotBlank(acCheckMsgResBody.getSessionId())) {
                logger.info("AC msg successfully,msg sessionId is ===>" + acCheckMsgResBody.getSessionId());
                msgSessionCookie = acCheckMsgResBody.getSessionId();
            }
        } else if (statusCode == 0) {
            logger.error("This statusCode is 0,so AC msg from ZX acCheckMsgResBody maybe null or not be set");
        } else {
            logger.error("AC msg from ZX is error,statusCode is " + statusCode + "(case)=" + acCheckMsgResBody.getCode() + "(getMethod)<===");
            logger.error("This " + statusCode + " ResponseBody(contain sessionId) is ===>" + acCheckMsgResBody.toString() + "<===");
        }
        updateConfigInfo();
    }

    /**
     * 获取状态回传SessionCookie
     */
    private static void getMonitorSessionCookie() {
        AcResBody acCheckMsgMonitorResBody;
        int requestType = 10;
        int statusCode = 0;
        acCheckMsgMonitorResBody = acCheck(checkMsgUrl, userAgent, monitorMsgXTag, requestType);//getMonitorSessionCookie()
        if (acCheckMsgMonitorResBody != null) {
            statusCode = acCheckMsgMonitorResBody.getCode();
        }
        if (statusCode == 200) {
            if (StringUtils.isNotBlank(acCheckMsgMonitorResBody.getSessionId())) {
                logger.info("getMonitorSessionCookie-Thread.currentThread().getName()===>" + Thread.currentThread().getName());
                logger.info("AC msgMonitor successfully,msgMonitor sessionId is ===>" + acCheckMsgMonitorResBody.getSessionId());
                monitorSessionCookie = acCheckMsgMonitorResBody.getSessionId();
            }
        } else if (statusCode == 0) {
            logger.error("This statusCode is 0,so AC msgMonitor from ZX acCheckMsgMonitorResBody maybe null or not be set");
        } else {
            logger.error("AC msgMonitor from ZX is error,statusCode is " + statusCode + "(case)=" + acCheckMsgMonitorResBody.getCode() + "(getMethod)<===");
            logger.error("This " + statusCode + " ResponseBody(contain sessionId) is ===>" + acCheckMsgMonitorResBody.toString() + "<===");
        }
        updateConfigInfo();//getMonitorSessionCookie()更新
    }

    private static ConfigInfo updateConfigInfo() {
        configInfo.setCheckMsgUrl(checkMsgUrl);
        configInfo.setSingleProduceUrl(singleProduceUrl);
        configInfo.setBatchProduceUrl(batchProduceUrl);

        configInfo.setMsgSessionCookie(msgSessionCookie);

        configInfo.setMonitorSessionCookie(monitorSessionCookie);//缓存monitorSessionCookie

        configInfo.setTopicName(topicName);
        configInfo.setUserAgent(userAgent);
        configInfo.setxTag(xTag);
        configInfo.setBatchSize(batchSize);

        return configInfo;
    }

    /**
     * 往zx发送文件数据的消息,即发送文件的message数据(结构化数据)
     * 本来是作为文件消息发送,现该方法主要用于单条发送数据-20191224
     * @param transaction
     * @param contents
     */
    private void sendMsgLog(Transaction transaction, List<String> contents) {

        try {

            logger.debug("Current sendMsgLog Thread number ID ：" + Thread.currentThread().getId());

            /**
             * 获取状态回传sessionID,检查认证是否存在
             */
           if (StringUtils.isBlank(monitorSessionCookie)
                || StringUtils.isBlank(msgSessionCookie)) {
                getMonitorSessionCookie();//sendMsgLog-首次获取monitorSessionCookie
                getMsgSessionCookie();//sendMsgLog-msgSessionCookie为空,首次接入验证
                if (!checkTimerStart) {
                    checkCookieEveryWeek();//sendMsgLog-第一次启动检测到monitorSessionCookie为空时启动任务但不进行验证,后续间隔一段时间后开始验证,每次申请monitorSessionCookie和msgSessionCookie两个Cookie
                    checkTimerStart = true;
                    logger.debug("CheckMsgAndFileCookie Timer is started......");
                }

                if (!monitorStart) {//消息定时上报
                    AvroMonitorTimerTask.monitorMsg(monitorSessionCookie, singleProduceUrl,
                            "monitor-msg", 1, userAgent, topicName);//sendMsgLog-日志消息
                    monitorStart = true;
                    logger.debug("MonitorMsg Timer is started......");
                }


               logger.info("AC msg sessionId already exists,msg sessionId is ===>" + msgSessionCookie);
            }

            AvroMonitorTimerTask.addTotalNum(contents.size());
            AvroMonitorTimerTask.addReadyPostNum(contents.size());
            if (isSingle(topicName)) {
                for (String content : contents) {
                    pool.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                HttpClientUtil.singleAsyncProduceMessage(singleProduceUrl, topicName, content,
                                        userAgent, msgSessionCookie, 1, System.currentTimeMillis());//初始发送count计数为0
                            } catch (Exception e) {
                                logger.error("sendMsgLog multi-thread is error==>" + e + "<==,Thread is==>" + Thread.currentThread().getName() + "<==.");
                            }
                        }
                    });
                }
            } else {
                long beginAvroTime = System.currentTimeMillis();
                xTag = HttpClientUtil.getXTAG(contents.get(0), topicName);
                byte[] msgResults = HttpClientUtil.encapsulationData(topicName, contents);

                logger.info("Batch Avro and Encapsulation Data Time(ms)："+ (System.currentTimeMillis() - beginAvroTime) );
                HttpClientUtil.batchAsyncProduceMessage(batchProduceUrl, topicName, msgResults, xTag,
                        userAgent, msgSessionCookie, contents.size(), System.currentTimeMillis());
            }

        } catch (Exception e) {
                logger.error("YbHttpAvroSinkFile Send Msg is error===>" + e + "<===");
                transaction.commit();
        } finally {
            if (transaction != null) {
                transaction.commit();
            }
            AvroMonitorTimerTask.subReadyPostNum(contents.size());
       }
    }

    /**
     * 单条消息发送处理逻辑
     * @param topicName
     * @return
     */
    private static boolean isSingle(String topicName) {

        if (topicName.equals("NTC-COLLECT-FILE-LOG")
                || topicName.equals("NTC-COLLECT-HTTP-DOC-LOG")
                || topicName.equals("NTC-COLLECT-HTTP-AV-LOG")
                || topicName.equals("NTC-COLLECT-FTP-DOC-LOG")
                || topicName.equals("NTC-COLLECT-MAIL-LOG")
                || topicName.equals("NTC-COLLECT-TELNET-LOG")
                || topicName.equals("INFLUX-SAPP-BPS-STAT-LOG")) //监控消息，几分钟1条

            return true;
        else {
            return false;
        }
    }



    /**
     * 总线入库前验证
     *
     * @param checkMsgUrl
     * @param userAgent
     * @param xTag
     * @param requestType
     *
     * @return
     */
    private static AcResBody acCheck(String checkMsgUrl, String userAgent, String xTag, int requestType) {
        return dcl.zxAcCheck(checkMsgUrl, userAgent, xTag, requestType);
    }

    /**
     * 验证定时器,每隔一段时间验证一次-验证获取新的Cookie
     */
    private void checkCookieEveryWeek() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    getMsgSessionCookie();//定时进行消息Cookie验证更新-7天
                    getMonitorSessionCookie();//定时进行状态上报Cookie验证更新-7天
                } catch (Exception e) {
                    logger.error("CheckCookieEveryWeek to zx everyWeek is error===>>>" + e + "<===");
                }
            }
        }, 1000 * 60 * 60 * 24 * 7, 1000 * 60 * 60 * 24 * 7);//每隔7天执行一次
    }


    class RetrySendMessage implements Runnable {

        @Override
        public void run() {
            logger.debug("Current Consumer Thread number ID ：" + Thread.currentThread().getId());
            while(true) {
                if (!HttpClientUtil.getRetryQueue().isEmpty()) {
                    logger.info("当前队列大小：" + HttpClientUtil.getRetryQueue().size());
                    HttpClientUtil.singleAsyncProduceMessage(singleProduceUrl, topicName,
                            HttpClientUtil.getRetryQueue().poll(), userAgent, msgSessionCookie, 1, System.currentTimeMillis());

                }
            }
        }
    }



    /**
     * 动态负载均衡变更cookie-20200818
     *
     * @param redirectUrlPort
     */
    public static void changeUrl(String redirectUrlPort) {

        /**
         * 变更postMsgUrl与checkMsgUrl
         */
        singleProduceUrl = redirectUrlPort + urlToolHm.get("post_single_suf_path");
        batchProduceUrl = redirectUrlPort + urlToolHm.get("post_batch_suf_path");
        checkMsgUrl = redirectUrlPort + urlToolHm.get("check_suf_path");

        /**
         * 变更url后需要重新获取cookie
         */
        updateCookie();

        logger.warn("YbHttpAvroSinkFile->changeUrl->"
                        + "change singleProduceUrl:" + singleProduceUrl
                + "change batchProduceUrl:" + batchProduceUrl
                        + ",change checkMsgUrl:" + checkMsgUrl);
    }



    /**
     * 动态负载均衡更新cookie
     */
    public static void updateCookie() {
        getMonitorSessionCookie();//动态负载均衡修改url,重新获取cookie
        getMsgSessionCookie();//动态负载均衡修改url,重新获取cookie

        logger.warn("YbHttpAvroSinkFile->updateCookie update cookie,singleProduceUrl:" + singleProduceUrl
                + "batchProduceUrl:" + batchProduceUrl
                + ",checkMsgUrl:" + checkMsgUrl
                + ",获取monitorSessionCookie:" + monitorSessionCookie
                + ",获取msgSessionCookie:" + msgSessionCookie);
    }

    /**
     * 用于切分url,便于后续动态负载均衡的url变更
     *
     * @param oldUrlPath
     * @param urlType
     */
    private static void makeUrlSplitMap(String oldUrlPath, String urlType) {
        String[] split = oldUrlPath.replace("http://", "")
                            .replace("https://", "").split("/", 2);

        if (split.length == 2) {
            /*
            验证地址：{"check_pre_https":"https://","check_pre_http":"http://","check_old_url_port":"192.168.126.4:22451","check_suf_path":"/v1/system/connect"}
            生产地址：{"post_suf_path":"/v1/data/sendData","post_pre_http":"http://","post_old_url_port":"192.168.126.4:22451","post_pre_https":"https://"}
            一般会存放到一起：{"post_suf_path":"/v1/data/sendData","check_pre_https":"https://","post_pre_http":"http://","check_pre_http":"http://","check_old_url_port":"192.168.126.4:22451","post_old_url_port":"192.168.126.4:22451","check_suf_path":"/v1/system/connect","post_pre_https":"https://"}
            */
            urlToolHm.put(urlType + "_pre_http", "http://");
            urlToolHm.put(urlType + "_pre_https", "https://");
            urlToolHm.put(urlType + "_old_url_port", split[0]);
            urlToolHm.put(urlType + "_suf_path", "/" + split[1]);
        } else {
            logger.error("makeUrlSplitMap-->String[] split size is not correct(split.length must be 2).");
        }

    }

}

package cn.ac.iie.cusflume.sink.avroUtils;


import cn.ac.iie.cusflume.sink.HttpAsyncUtils.HttpClientUtil;
import cn.ac.iie.cusflume.sink.avroUtils.avroMonitorBean.*;
import cn.ac.iie.cusflume.sink.bean.producer.Res.ProResBody;
import cn.ac.iie.cusflume.sink.daoUtils.InfluxdbUtils;
import cn.ac.iie.cusflume.sink.daoUtils.RealtimeCountConfig;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AvroMonitorTimerTask {

    private static Logger logger = Logger.getLogger(AvroMonitorTimerTask.class);

    //消息计数
    public static long msgSuccessSum = 0;
    public static long msgFailedSum = 0;
    public static long msgTotalSum = 0;
    public static long msgReadyPostSum = 0;

    //文件计数
    public static long fileSuccessSum = 0;
    public static long fileFailedSum = 0;
    public static long fileTotalSum = 0;
    public static long fileReadyPostSum = 0;

    //文件字节数计数
    public static long fileBytesSuccessSum = 0;
    public static long fileBytesFailedSum = 0;
    public static long fileBytesTotalSum = 0;
    public static long fileBytesReadyPostSum = 0;

    public static boolean startFileMonitor = false;//默认false

    /**
     * 消息---Java自定义定时器
     */
    public static void monitorMsg(String monitorSessionCookie, String postMonitorUrl, String monitorCategory, int batchSize, String userAgent, String topicType) {//20200428新增
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                AvroMonitorTimerTask.msgTotalSum++;
                AvroMonitorTimerTask.msgReadyPostSum++;

                try {
                    if ("yb".equals(RealtimeCountConfig.MONITOR_TYPE)) {//只有当类型为一部(yb)时才进行状态上报
                        String sendMsg = getJson(RealtimeCountConfig.MONITOR_SYSTEM_COMPONENT_CODE, RealtimeCountConfig.MONITOR_SYSTEM_COMPONENT_CODE_FLUME, topicType);//新版-20200428
                        logger.info("Send monitor message is===>>>" + sendMsg + "<<<===");
                        HttpClientUtil.asyncProducerAvroToZX(postMonitorUrl, monitorCategory, sendMsg, userAgent, monitorSessionCookie, 0, System.currentTimeMillis());//静态方法无返回值用于多线程,初始发送count计数为0
                    }
                } catch (Exception e) {
                    logger.error("Send monitorMsg to zx is error===>>>" + e + "<===");
                }
                /**
                 * 20200427移除一部"YB monitorMsg"的info日志,加入"last min"日志,方便脚本在warn状态下采集
                 * 此时RealtimeCountConfig.MONITOR_TYPE填写yb,则消息上报与warn统计信息都有
                 * 若填写gd,则只有warn统计信息,不会有状态上报
                 */
//                if ("gd".equals(RealtimeCountConfig.MONITOR_TYPE)) {//只有当类型为广东(gd)时才进行warn类型日志计数打印,此时需要搭配外部脚本计数
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
                String timeFormat = sdf.format(new Date(System.currentTimeMillis()));
                long lastMinTime = Long.parseLong(timeFormat) - 1;
                //发送消息统计情况-warn类型便于脚本收集信息打印
                logger.warn("last min " + lastMinTime + " monitorMsg count==>msgSuccessSum:{ " + msgSuccessSum + " },==>msgFailedSum:{ " + msgFailedSum + " },==>msgReadyPostSum:{ " + msgReadyPostSum + " },==>msgTotalSum:{ " + msgTotalSum + " }.");
//                }

                if (RealtimeCountConfig.MONITOR_CLEAN_TYPE == 1) {//1表示定时重置
                    //重置为0
                    msgSuccessSum = 0;
                    msgFailedSum = 0;
                    msgTotalSum = 0;
                    msgReadyPostSum = 0;
                }
            }
        }, 60000, 60000);
    }

    /**
     * 消息和文件-单线程版
     */
    public static void monitorMsgAndFile() {
        Timer timer = new Timer();
//        InfluxdbUtils influxdbUtils = new InfluxdbUtils();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
                String timeFormat = sdf.format(new Date(System.currentTimeMillis()));
                long lastMinTime = Long.parseLong(timeFormat) - 1;

                //发送消息及文件的统计情况-仅计算将要推送与成功推送的文件个数与字节数
                logger.warn("last min " + lastMinTime + " monitorMsgAndFile count==>msgSuccessSum:{ " + msgSuccessSum + " },==>msgFailedSum:{ " + msgFailedSum + " },==>msgReadyPostSum:{ " + msgReadyPostSum + " }," +
                        "==>fileSuccessSum:{ " + fileSuccessSum + " },==>fileReadyPostSum:{ " + fileReadyPostSum + " }," +
                        "==>fileBytesSuccessSum:{ " + fileBytesSuccessSum + " },==>fileBytesReadyPostSum:{ " + fileBytesReadyPostSum + " }.");

                //重置为0
                msgSuccessSum = 0;//根据响应计算-索引14
                msgFailedSum = 0;//根据响应计算-索引16
                msgReadyPostSum = 0;//直接在推送处++,多个文件对应一个消息-索引18

                fileSuccessSum = 0;//同字节数-索引20
                fileReadyPostSum = 0;//同字节数-索引22

                fileBytesSuccessSum = 0;//根据ID个数计算(id是推送成功后返回的)-索引24
                fileBytesReadyPostSum = 0;//根据流的个数计算-索引26
            }
//        }, 60000, 300000);//正式
        }, 60000, 60000);//测试
    }

    private static String getJson(String systemComponentCode, String systemComponentCodeFlume, String topicType) {
        SystemStatus systemStatus = new SystemStatus();
        /**
         * 第一部分-设置时间-time
         */
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String timeFormat = sdf.format(new Date(System.currentTimeMillis()));//格式2019-11-18 19:56:37
        systemStatus.setTime(timeFormat);

        /**
         * 第二部分-设置系统状态-system
         */
        RealTimeStatus realTimeStatus = new RealTimeStatus();
        realTimeStatus.setId(systemComponentCode);
        realTimeStatus.setStatus("在线");

        //无问题时不设置状态
        if (msgFailedSum >= 100000L) {
            List<RtAlarm> rtAlarmsList = new LinkedList<RtAlarm>();
            RtAlarm rtAlarm = new RtAlarm();
            rtAlarm.setLevel("低");
            rtAlarm.setTime(timeFormat);
            rtAlarm.setType("运行出现失败数据");
            rtAlarm.setDetail("系统正常运行但是出现了失败数据");
            rtAlarmsList.add(rtAlarm);
            realTimeStatus.setAlarms(rtAlarmsList);
        } else {
            List<RtAlarm> rtAlarmsList = new LinkedList<RtAlarm>();
            realTimeStatus.setAlarms(rtAlarmsList);
        }

        LinkedList<RtStatValue> rtStatValuesList = new LinkedList<>();
        RtStatValue rtStatValue = new RtStatValue();
        rtStatValue.setName("当前批次数据量");

        LinkedList<RtNameValue> rtNameValuesList = new LinkedList<>();
        //总数
        RtNameValue totalCount = new RtNameValue();
        totalCount.setName("总数据量");
        totalCount.setValue(msgTotalSum + "");
        //成功
        RtNameValue successCount = new RtNameValue();
        successCount.setName("成功");
        successCount.setValue(msgSuccessSum + "");
        //准备发送
        RtNameValue readyPostCount = new RtNameValue();
        readyPostCount.setName("准备导入");
        readyPostCount.setValue(msgReadyPostSum + "");
        //失败
        RtNameValue failCount = new RtNameValue();
        failCount.setName("失败");
        failCount.setValue(msgFailedSum + "");

        rtNameValuesList.add(totalCount);
        rtNameValuesList.add(successCount);
        rtNameValuesList.add(readyPostCount);
        rtNameValuesList.add(failCount);
        rtStatValue.setContent(rtNameValuesList);

        rtStatValuesList.add(rtStatValue);
        realTimeStatus.setStats(rtStatValuesList);
        systemStatus.setSystem(realTimeStatus);


        /**
         * 第三部分-设置组件状态-components
         */
        LinkedList<RealTimeStatus> realTimeStatusesList = new LinkedList<>();
//        RealTimeStatus realTimeStatus1 = new RealTimeStatus();
//        realTimeStatus1.setId(xTag);
//        realTimeStatus1.setStatus("专用设备");
//
//        LinkedList<RtAlarm> rtAlarmsList2 = new LinkedList<>();
//        RtAlarm rtAlarm2 = new RtAlarm();
//        rtAlarm2.setLevel("Low");
//        rtAlarm2.setTime(timeFormat);
//        rtAlarm2.setType("Working");
//        rtAlarm2.setDetail("Component is Working");
//        rtAlarmsList2.add(rtAlarm2);
//        realTimeStatus1.setAlarms(rtAlarmsList2);
//
//        LinkedList<RtStatValue> rtStatValuesList2 = new LinkedList<>();
//        RtStatValue rtStatValue2 = new RtStatValue();
//        rtStatValue2.setName("Last 5 MinS Message Data Counts");
//
//        LinkedList<RtNameValue> rtNameValuesList2 = new LinkedList<>();
//        //总数
//        RtNameValue totalComponentCount = new RtNameValue();
//        totalComponentCount.setName("TotalComponentMsg");
//        totalComponentCount.setValue(msgTotalSum + "");
//        //成功
//        RtNameValue successComponentCount = new RtNameValue();
//        successComponentCount.setName("SuccessfulComponentMsg");
//        successComponentCount.setValue(msgSuccessSum + "");
//        //准备发送
//        RtNameValue readyPostComponentCount = new RtNameValue();
//        readyPostComponentCount.setName("ReadyPostComponentMsg");
//        readyPostComponentCount.setValue(msgReadyPostSum + "");
//        //失败
//        RtNameValue failComponentCount = new RtNameValue();
//        failComponentCount.setName("FailedComponentMsg");
//        failComponentCount.setValue(msgFailedSum + "");
//
//        rtNameValuesList2.add(totalComponentCount);
//        rtNameValuesList2.add(successComponentCount);
//        rtNameValuesList2.add(readyPostComponentCount);
//        rtNameValuesList2.add(failComponentCount);
//
//        rtStatValue2.setContent(rtNameValuesList2);
//
//        rtStatValuesList2.add(rtStatValue2);
//        realTimeStatus1.setStats(rtStatValuesList2);
//        realTimeStatusesList.add(realTimeStatus1);

        /**
         * 20200428新增--↓--增加Flume状态
         */

        RealTimeStatus realTimeStatus1 = new RealTimeStatus();
        realTimeStatus1.setId(systemComponentCodeFlume);
        realTimeStatus1.setStatus("正常");

        //无问题时不设置状态
        if (msgFailedSum >= 100000L) {
            LinkedList<RtAlarm> rtAlarmsList2 = new LinkedList<>();
            RtAlarm rtAlarm2 = new RtAlarm();
            rtAlarm2.setLevel("低");
            rtAlarm2.setTime(timeFormat);
            rtAlarm2.setType("设备处理数据出现异常");
            rtAlarm2.setDetail("设备正常运行但出现了失败数据,数据种类:" + topicType);
            rtAlarmsList2.add(rtAlarm2);
            realTimeStatus1.setAlarms(rtAlarmsList2);
        } else {
            LinkedList<RtAlarm> rtAlarmsList2 = new LinkedList<>();
            realTimeStatus1.setAlarms(rtAlarmsList2);
        }


        LinkedList<RtStatValue> rtStatValuesList2 = new LinkedList<>();
        RtStatValue rtStatValue2 = new RtStatValue();
        rtStatValue2.setName("设备处理信息");

        LinkedList<RtNameValue> rtNameValuesList2 = new LinkedList<>();
        //数据种类-其实就是topic种类
        RtNameValue componentType = new RtNameValue();
        componentType.setName("数据种类");
        componentType.setValue(topicType);
//        //成功
//        RtNameValue successComponentCount = new RtNameValue();
//        successComponentCount.setName("SuccessfulComponentMsg");
//        successComponentCount.setValue(msgSuccessSum + "");
//        //准备发送
//        RtNameValue readyPostComponentCount = new RtNameValue();
//        readyPostComponentCount.setName("ReadyPostComponentMsg");
//        readyPostComponentCount.setValue(msgReadyPostSum + "");
//        //失败
//        RtNameValue failComponentCount = new RtNameValue();
//        failComponentCount.setName("FailedComponentMsg");
//        failComponentCount.setValue(msgFailedSum + "");

        rtNameValuesList2.add(componentType);
//        rtNameValuesList2.add(successComponentCount);
//        rtNameValuesList2.add(readyPostComponentCount);
//        rtNameValuesList2.add(failComponentCount);

        rtStatValue2.setContent(rtNameValuesList2);

        rtStatValuesList2.add(rtStatValue2);
        realTimeStatus1.setStats(rtStatValuesList2);
        realTimeStatusesList.add(realTimeStatus1);

        /**
         * 20200428新增--↑--增加Flume状态
         */
        systemStatus.setComponents(realTimeStatusesList);//中间全部注释表示不设置Component,未注释时(有代码时)表示设置Component

//        return JSONObject.toJSONString(systemStatus);
        return systemStatus.toString();
    }

    public static long getFileBytesFromInputStream(InputStream fileIS) {
        try {
            byte[] bytes = IOUtils.toByteArray(fileIS);
            return bytes.length;
        } catch (Exception e) {
            logger.error("GetFileBytes is error,return 0,error is===>" + e);
            return 0;
        }
    }

    public static ByteArrayOutputStream getByteArrayOutputStream(InputStream input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = input.read(buffer)) > -1) {
            baos.write(buffer, 0, len);
        }
        baos.flush();
        baos.close();
        return baos;
    }


    public static LinkedList<ByteArrayOutputStream> getByteArrayOutputStream(LinkedList<InputStream> attachmentsResultIsList) throws IOException {
        LinkedList<ByteArrayOutputStream> byteArrayOutputStreamsList = new LinkedList<>();
        for (int i = 0; i < attachmentsResultIsList.size(); i++) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int len;
            while ((len = attachmentsResultIsList.get(i).read(buffer)) > -1) {
                baos.write(buffer, 0, len);
            }
            baos.flush();
            baos.close();
            byteArrayOutputStreamsList.add(baos);
        }

        return byteArrayOutputStreamsList;
    }

    public static void main(String[] args) {
        msgSuccessSum = 2;
        msgFailedSum = 1;
        msgTotalSum = 3;
        msgReadyPostSum = 3;
//        String sendMsg = AvroMonitorTimerTask.getJson("userAgent", "xTag");
        String sendMsg = AvroMonitorTimerTask.getJson(RealtimeCountConfig.MONITOR_SYSTEM_COMPONENT_CODE, RealtimeCountConfig.MONITOR_SYSTEM_COMPONENT_CODE_FLUME, "NTC-CONN-RECORD-LOG");
        System.out.println(sendMsg);
    }

}
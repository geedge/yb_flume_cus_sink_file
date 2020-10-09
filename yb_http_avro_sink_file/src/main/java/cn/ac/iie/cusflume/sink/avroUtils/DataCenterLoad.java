package cn.ac.iie.cusflume.sink.avroUtils;

import cn.ac.iie.cusflume.sink.HttpAsyncUtils.HttpClientUtil;
import cn.ac.iie.cusflume.sink.avroUtils.avroSchema.GetAvroSchemaByTopicUtils;
import cn.ac.iie.cusflume.sink.bean.ac.Res.AcResBody;
import cn.ac.iie.cusflume.sink.bean.producer.Res.ProResBody;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//import org.apache.log4j.Logger;


public class DataCenterLoad {
    //private static final Logger logger = Logger.getLogger(DataCenterLoad.class);
    private static final String url = HttpManager.getInfoLoadInstance().getAddress();
    private static Logger logger = Logger.getLogger(DataCenterLoad.class);

    public DataCenterLoad() {
    }

    //总线入库认证
    public AcResBody zxAcCheck(String checkMsgUrl, String userAgent, String xTag, int requestType) {
        AcResBody acResBody = null;
        try {
            acResBody = HttpManager.getInfoLoadInstance().checkAcByZx(checkMsgUrl, userAgent, xTag, requestType);
        } catch (Exception e) {
            logger.error("DataCenterLoad zxAcCheck is error,so this return acResBody maybe null,error is===>" + e + "<===");
            e.printStackTrace();
        }
        return acResBody;
    }

    /**
     * flume zx验证之测试Avro数据导入---用于消息传输(批)
     *
     * @param urlProducer
     * @param topicName
     * @param jsonDataList
     * @param batchSize
     * @param userAgent
     * @param msgSessionCookie
     * @return
     */
    public ProResBody avroDataLoad(String urlProducer, String topicName, List<String> jsonDataList, int batchSize, String userAgent, String msgSessionCookie) {
        String time5 = generateTimeWithInterval();
        List<String> avroBatchList = new ArrayList<String>();
        //根据topic获取对应schema
        String topicAvroSchema = GetAvroSchemaByTopicUtils.getAvroSchemaByTopicName(topicName);//批次
        int nums = 0;
        ProResBody proResBody = null;
        try {
            for (String jsonData : jsonDataList) {
                avroBatchList.add(jsonData);
                nums++;
//                if (nums >= batchSize) {
//                    //logger.info("start to post data to zx---------> " + jsonDataList);
//                    HttpManager.getInfoLoadInstance().producerAvroToZX(urlProducer, topicName, topicAvroSchema, avroBatchList, userAgent, msgSessionCookie);
//                    avroBatchList.clear();
//                    nums = 0;
//                }
            }
            if (nums != 0) {
                proResBody = HttpManager.getInfoLoadInstance().producerAvroToZX(urlProducer, topicName, topicAvroSchema, avroBatchList, userAgent, msgSessionCookie);
                avroBatchList.clear();
                nums = 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return proResBody;
    }

    /**
     * flume zx验证之测试Avro数据导入---用于文件补全后传输(单条)
     * 也用于一部状态回传(定时单条)
     * 单条时传入的topicAvroSchema为""
     * 此为带返回值用于同步请求的版本
     *
     * @param urlProducer
     * @param topicName
     * @param jsonData
     * @param batchSize
     * @param userAgent
     * @param msgSessionCookie
     * @return
     */
    public ProResBody avroDataLoad(String urlProducer, String topicName, String jsonData, int batchSize, String userAgent, String msgSessionCookie) {//原本的方法用于同步单线程
        //根据topic获取对应schema
//        String topicAvroSchema = GetAvroSchemaByTopicUtils.getAvroSchemaByTopicName(topicName);//单条
        String topicAvroSchema = "";//单条-20191224修改,不再从这里获取,HttpManager中统一获取
        ProResBody proResBody = null;
        try {
            proResBody = HttpManager.getInfoLoadInstance().producerAvroToZX(urlProducer, topicName, topicAvroSchema, jsonData, userAgent, msgSessionCookie);//原本的方法有返回值用于同步单线程
        } catch (Exception e) {
            e.printStackTrace();
        }
        return proResBody;
    }

    //flume zx验证之测试json数据导入
    public void jsonDataLoad(String urlProducer, List<String> jsonDataList, int batchSize, String userAgent, String sessionCookie) {
        StringBuffer sb = new StringBuffer();
        String time5 = generateTimeWithInterval();
        int nums = 0;
        for (String jsonData : jsonDataList) {
            try {
                //换另一种行分隔符与字段分隔符
                String aItem = jsonData + "\t" + time5;
                sb.append(aItem + "\n");
                nums++;
                if (nums >= batchSize) {
                    String data = sb.substring(0, sb.length() - 1);
                    //输出的topic
                    logger.info("start to post data to zx---------> " + data);
//                    HttpManager.getInfoLoadInstance().postToDataCenter(url, "DF_PZ_FLOW_REPORT", data);//原本的方法
                    HttpManager.getInfoLoadInstance().producerJsonToZX(urlProducer, "SESSION-TEST-COMPLETED-LOG", data, userAgent, sessionCookie);
//                    CSVAlarm.getInfoLoadInstance().csvDataLoad(url, "DF_PZ_FLOW_REPORT", data);//自己根据获得接口又使用的方法
                    sb.setLength(0);
                    nums = 0;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            if (nums != 0) {
                String data = sb.substring(0, sb.length() - 1);
//                HttpManager.getInfoLoadInstance().postToDataCenter(url, "DF_PZ_FLOW_REPORT", data);//这里topic位置需要根据实际情况修改
                HttpManager.getInfoLoadInstance().producerJsonToZX(urlProducer, "SESSION-TEST-COMPLETED-LOG", data, userAgent, sessionCookie);
//                CSVAlarm.getInfoLoadInstance().csvDataLoad(url, "DF_PZ_FLOW_REPORT", data);//自己根据获得接口又使用的方法
                sb.setLength(0);
                nums = 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //新写的数据导入方法20190109:
    public void dfPzFlowBatchStorage(Map<String, Long> pzMap) {
        //String sql = " insert into DF_PZ_REPORT(STAT_ID, ACTIVE_SYS, CFG_ID, SERVICE, SUM, REPORT_TIME) " +
        //		" VALUES(SEQ_DF_PZ_REPORT.NEXTVAL, ?, ?, ?, ?, ?)";
        StringBuffer sb = new StringBuffer();
        String time5 = generateTimeWithInterval();
        int nums = 0;
        for (String key : pzMap.keySet()) {
            try {
                String[] options = key.split("~=~");
                if (options[0] != null && options[0] != "" && options[1] != null && options[1] != "" && options[2] != null && options[2] != "" && options[3] != null && options[3] != "" && options[4] != null && options[4] != "" && options[5] != null && options[5] != "") {
                    //若包含log名称,则因为log名称在索引0处,所以这里从1开始
//                String aItem = options[1] + "\t" + options[2] + "\t" + options[3] + "\t" + options[4] + "\t" + options[5] + "\t" + options[6] + "\t" + pzMap.get(key) + "\t" + time5;
                    //若不包含log名称,则直接从0开始
//                String aItem = options[0] + "\t" + options[1] + "\t" + options[2] + "\t" + options[3] + "\t" + options[4] + "\t" + options[5] + "\t" + pzMap.get(key) + "\t" + time5;
//                sb.append(aItem + "\n");
                    //换另一种行分隔符与字段分隔符
                    String aItem = options[0] + "," + options[1] + "," + options[2] + "," + options[3] + "," + options[4] + "," + options[5] + "," + pzMap.get(key) + "," + time5;
                    sb.append(aItem + "\n");
                    nums++;
                    if (nums >= 1000) {
                        String data = sb.substring(0, sb.length() - 1);
                        //输出的topic
                        logger.info("start to post data to dc---------> " + data);
                        System.out.println("start to post data to dc---------> " + data);
                        HttpManager.getInfoLoadInstance().postToDataCenter(url, "DF_PZ_FLOW_REPORT", data);//原本的方法
//                    CSVAlarm.getInfoLoadInstance().csvDataLoad(url, "DF_PZ_FLOW_REPORT", data);//自己根据获得接口又使用的方法
                        sb.setLength(0);
                        nums = 0;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            if (nums != 0) {
                String data = sb.substring(0, sb.length() - 1);
                HttpManager.getInfoLoadInstance().postToDataCenter(url, "DF_PZ_FLOW_REPORT", data);//这里topic位置需要根据实际情况修改
//                CSVAlarm.getInfoLoadInstance().csvDataLoad(url, "DF_PZ_FLOW_REPORT", data);//自己根据获得接口又使用的方法
                sb.setLength(0);
                nums = 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //原本的:
    public void dfPzBatchStorage(Map<String, Long> pzMap) {
        //String sql = " insert into DF_PZ_REPORT(STAT_ID, ACTIVE_SYS, CFG_ID, SERVICE, SUM, REPORT_TIME) " +
        //		" VALUES(SEQ_DF_PZ_REPORT.NEXTVAL, ?, ?, ?, ?, ?)";
        StringBuffer sb = new StringBuffer();
        String time5 = generateTimeWithInterval();
        int nums = 0;
        for (String key : pzMap.keySet()) {
            try {
                String[] options = key.split("~=~");
                String aItem = options[2] + "\t" + options[1] + "\t" + options[3] + "\t" + pzMap.get(key) + "\t" + time5;
                sb.append(aItem + "\n");
                nums++;
                if (nums >= 1000) {
                    String data = sb.substring(0, sb.length() - 1);
                    HttpManager.getInfoLoadInstance().postToDataCenter(url, "t_xa_df_pz_report_dt", data);
                    sb.setLength(0);
                    nums = 0;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            if (nums != 0) {
                String data = sb.substring(0, sb.length() - 1);
                HttpManager.getInfoLoadInstance().postToDataCenter(url, "t_xa_df_pz_report_dt", data);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private String generateTimeWithInterval() {
        Long stamp = System.currentTimeMillis() + 300000L;
        Long stamp5 = stamp / 300000 * 300000;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(stamp5);
    }


//    public static void main(String[] args) {
//        String topicAvroSchema = GetAvroSchemaByTopicUtils.getAvroSchemaByTopicName("NTC-CONN-RECORD-LOG");
//        System.out.println(topicAvroSchema);
//    }
}

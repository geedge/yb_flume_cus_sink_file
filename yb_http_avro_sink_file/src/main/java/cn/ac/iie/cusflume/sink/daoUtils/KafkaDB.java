package cn.ac.iie.cusflume.sink.daoUtils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * 发送数据的kafka工具类
 */
public class KafkaDB {
    private static Logger logger = Logger.getLogger(KafkaDB.class);

    /**
     * kafka生产者，用于向kafka中发送消息
     */
    private static Producer<String, String> producer;

    /**
     * kafka生产者适配器（单例），用来代理kafka生产者发送消息
     */
    private static KafkaDB kafkaDB;

    private KafkaDB() {
        getProducer();
    }

    public static KafkaDB getInstance() {
        if (kafkaDB == null) {
            kafkaDB = new KafkaDB();
        }
        return kafkaDB;
    }

    //将无法处理的数据发往kafka,由另外一个程序处理.
//    public void postFailMsgToKafka(String topicName, String jsonStr, Long timeSend) {

    /**
     * @param topicName 处理失败的数据发往topic的名称
     * @param jsonStr
     */
    public void postFailMsgToKafka(String topicName, String jsonStr) {
        try {
            switch (topicName) {
                case "NTC-COLLECT-MAIL-LOG":
                    producer.send(new ProducerRecord<>(RealtimeCountConfig.FAIL_NTC_COLLECT_MAIL_LOG, jsonStr));
                    break;
                case "NTC-COLLECT-HTTP-DOC-LOG":
                    producer.send(new ProducerRecord<>(RealtimeCountConfig.FAIL_NTC_COLLECT_HTTP_DOC_LOG, jsonStr));
                    break;
                case "NTC-COLLECT-FTP-DOC-LOG":
                    producer.send(new ProducerRecord<>(RealtimeCountConfig.FAIL_NTC_COLLECT_FTP_DOC_LOG, jsonStr));
                    break;
                default:
                    logger.warn("KafkaDB postFailMsgToKafka kafkaTopicName is unknown!===>>>" + topicName + "<<<=== And this message is===>>>" + jsonStr + "<<<===");
                    break;
            }
        } catch (Exception e) {
            logger.error("KafkaDB postFailMsgToKafka send to kafka error!===>>>" + e);
            e.printStackTrace();
        }
        producer.flush();
    }

    /**
     * 根据kafka生产者配置信息初始化kafka消息生产者,只初始化一次
     */
    private void getProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", RealtimeCountConfig.BOOTSTRAP_OUTPUT_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("linger.ms", "2");
        properties.put("request.timeout.ms", 20000);
        properties.put("batch.size", 262144);
        properties.put("buffer.memory", 33554432);
        producer = new KafkaProducer<String, String>(properties);
    }

    //1分钟
    private static String generateTimeWithInterval() {
        Long stamp = System.currentTimeMillis() + 60000L;
        Long stamp1 = stamp / 60000 * 60000;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(stamp1);
    }

    //专属于GetNtcOriBoltDC的时间戳计时规整器
//    private String generateTimeWithIntervalForGetNtcOriBoltDC() {
//        Long timeControllerLon = RealtimeCountConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS * 1000L;
//        int timeControllerInt = RealtimeCountConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS * 1000;
//        Long stamp = System.currentTimeMillis() + timeControllerLon;
//        Long stamp1 = stamp / timeControllerInt * timeControllerInt;
//        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
////        return stamp5 / 1000;
//        return df.format(stamp1);
//    }

}

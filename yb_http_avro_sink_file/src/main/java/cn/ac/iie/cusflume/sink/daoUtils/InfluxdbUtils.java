package cn.ac.iie.cusflume.sink.daoUtils;

import org.apache.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;

import java.net.InetAddress;


public class InfluxdbUtils {
    private static Logger logger = Logger.getLogger(InfluxdbUtils.class);

    public static InfluxDB client;

    //全量版
//    public static Boolean insert(long successMsgSum, long failMsgSum, long readyPostMsgSum, long totalMsgSum,
//                                 long successFileSum, long failFileSum, long readyPostFileSum, long totalFileSum,
//                                 long successFileBytesSum, long failFileBytesSum, long readyPostFileBytesSum, long totalFileBytesSum) {
    //精简版
    public static Boolean insert(long successMsgSum, long failMsgSum, long readyPostMsgSum,
//    public Boolean insert(long successMsgSum, long failMsgSum, long readyPostMsgSum,
//                                 long totalMsgSum,
                                 long successFileSum,
//                                 long failFileSum,
                                 long readyPostFileSum,
//                                 long totalFileSum,
                                 long successFileBytesSum,
//                                 long failFileBytesSum,
                                 long readyPostFileBytesSum
//                                 long totalFileBytesSum
    ) {
        InetAddress addrThis = null;
        try {
            addrThis = InetAddress.getLocalHost();
            String ip = addrThis.getHostAddress();
            /**
             * 消息计数
             */
            Point point1 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
                    .tag("hostname", ip).tag("state", "success").tag("action", RealtimeCountConfig.INFLUX_ACTION_MSG)
                    .field("sum", successMsgSum)
                    .build();
            Point point2 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
                    .tag("hostname", ip).tag("state", "fail").tag("action", RealtimeCountConfig.INFLUX_ACTION_MSG)
                    .field("sum", failMsgSum)
                    .build();
            Point point3 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
                    .tag("hostname", ip).tag("state", "ready_post").tag("action", RealtimeCountConfig.INFLUX_ACTION_MSG)
                    .field("sum", readyPostMsgSum)
                    .build();
//            Point point4 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
//                    .tag("hostname", ip).tag("state", "receive").tag("action", RealtimeCountConfig.INFLUX_ACTION_MSG)
//                    .field("sum", totalMsgSum)
//                    .build();

            /**
             * 文件计数
             */
            Point point5 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
                    .tag("hostname", ip).tag("state", "success").tag("action", RealtimeCountConfig.INFLUX_ACTION_FILE)
                    .field("sum", successFileSum)
                    .build();
//            Point point6 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
//                    .tag("hostname", ip).tag("state", "fail").tag("action", RealtimeCountConfig.INFLUX_ACTION_FILE)
//                    .field("sum", failFileSum)
//                    .build();
            Point point7 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
                    .tag("hostname", ip).tag("state", "ready_post").tag("action", RealtimeCountConfig.INFLUX_ACTION_FILE)
                    .field("sum", readyPostFileSum)
                    .build();
//            Point point8 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
//                    .tag("hostname", ip).tag("state", "receive").tag("action", RealtimeCountConfig.INFLUX_ACTION_FILE)
//                    .field("sum", totalFileSum)
//                    .build();

            /**
             * 文件大小计数
             */
            Point point9 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
                    .tag("hostname", ip).tag("state", "success").tag("action", RealtimeCountConfig.INFLUX_ACTION_FILE_BYTES)
                    .field("sum", successFileBytesSum)
                    .build();
//            Point point10 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
//                    .tag("hostname", ip).tag("state", "fail").tag("action", RealtimeCountConfig.INFLUX_ACTION_FILE_BYTES)
//                    .field("sum", failFileBytesSum)
//                    .build();
            Point point11 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
                    .tag("hostname", ip).tag("state", "ready_post").tag("action", RealtimeCountConfig.INFLUX_ACTION_FILE_BYTES)
                    .field("sum", readyPostFileBytesSum)
                    .build();
//            Point point12 = Point.measurement(RealtimeCountConfig.INFLUX_TABLE)
//                    .tag("hostname", ip).tag("state", "receive").tag("action", RealtimeCountConfig.INFLUX_ACTION_FILE_BYTES)
//                    .field("sum", totalFileBytesSum)
//                    .build();

            client = InfluxDBFactory.connect(RealtimeCountConfig.INFLUX_ADDRESS, RealtimeCountConfig.INFLUX_USERNAME, RealtimeCountConfig.INFLUX_PASSWORD);
            Pong pong = client.ping();
            if (pong != null) {
                logger.info("Connect InfluxDB Success!");
                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point1);
                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point2);
                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point3);
//                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point4);

                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point5);
//                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point6);
                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point7);
//                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point8);

                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point9);
//                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point10);
                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point11);
//                client.write(RealtimeCountConfig.INFLUX_DATABASE, "", point12);
                return true;
            } else {
                logger.error("Connect InfluxDB Failed!");
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}

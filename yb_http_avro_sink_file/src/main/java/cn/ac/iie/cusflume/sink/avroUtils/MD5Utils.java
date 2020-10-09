package cn.ac.iie.cusflume.sink.avroUtils;

import org.apache.log4j.Logger;

import java.security.MessageDigest;

/**
 * 描述:转换MD5工具类
 *
 * @author Administrator
 * @create 2018-08-13 15:11
 */
public class MD5Utils {
    private static Logger logger = Logger.getLogger(MD5Utils.class);

    //    public static String md5Encode(String msg) throws Exception {
    public static String md5Encode(byte[] msgBytes) throws Exception {
        try {
//            byte[] msgBytes = msg.getBytes("utf-8");
            /*
             * 声明使用Md5算法,获得MessaDigest对象
             */
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            /*
             * 使用指定的字节更新摘要
             */
            md5.update(msgBytes);
            /*
             * 完成哈希计算,获得密文
             */
            byte[] digest = md5.digest();
            /*
             * 以上两行代码等同于
             * byte[] digest = md5.digest(msgBytes);
             */
            return byteArr2hexString(digest);
        } catch (Exception e) {
            logger.error("Error in conversion MD5! This msgBytes is " + msgBytes);
//            e.printStackTrace();
            return "";
        }
    }

    public static String md5Encode(String msg) throws Exception {
        try {
            byte[] msgBytes = msg.getBytes("utf-8");
            /*
             * 声明使用Md5算法,获得MessaDigest对象
             */
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            /*
             * 使用指定的字节更新摘要
             */
            md5.update(msgBytes);
            /*
             * 完成哈希计算,获得密文
             */
            byte[] digest = md5.digest();
            /*
             * 以上两行代码等同于
             * byte[] digest = md5.digest(msgBytes);
             */
            return byteArr2hexString(digest);
        } catch (Exception e) {
            logger.error("Error in conversion MD5! This msg is " + msg);
//            e.printStackTrace();
            return "";
        }
    }

    /**
     * 将byte数组转化为16进制字符串形式
     *
     * @param bys 字节数组
     * @return 字符串
     */
    public static String byteArr2hexString(byte[] bys) {
        StringBuffer hexVal = new StringBuffer();
        int val = 0;
        for (byte by : bys) {
            //将byte转化为int  如果byte是一个负数就必须要和16进制的0xff做一次与运算
            val = ((int) by) & 0xff;
            if (val < 16) {
                hexVal.append("0");
            }
            hexVal.append(Integer.toHexString(val));
        }

        return hexVal.toString();

    }


//    public static void main(String[] args) {
//        try {
//            String bbb = MD5Utils.md5Encode("aaa");
//            System.out.println(bbb);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}

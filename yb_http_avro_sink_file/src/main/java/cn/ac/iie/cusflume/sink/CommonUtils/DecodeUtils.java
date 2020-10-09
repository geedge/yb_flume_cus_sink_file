package cn.ac.iie.cusflume.sink.CommonUtils;


import com.zdjizhi.utils.StringUtil;
import org.apache.log4j.Logger;

import java.util.Base64;


public class DecodeUtils {
    private static Logger logger = Logger.getLogger(DecodeUtils.class);

    public static String base64Str(String encodedText, String subjectCharset) {
        Base64.Decoder decoder = Base64.getDecoder();
        String sub;
        try {
            if (StringUtil.isBlank(subjectCharset)) {
                sub = new String(decoder.decode(encodedText), "UTF-8");
            } else if ("ISO8859-16".equals(subjectCharset)) {
                sub = new String(decoder.decode(encodedText), "ISO8859-1");
            } else {
                sub = new String(decoder.decode(encodedText), subjectCharset);
            }
            return sub;
        } catch (Exception e) {
            logger.warn("This encodedText===>" + encodedText + "<===, " +
                    "This subjectCharset===>" + subjectCharset + "<===, " +
                    "Transform base64 String failed===>" + e.getMessage() + "<===.");
            return "";
//            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        String s = base64Str("eWJqQlRURmdVclNRbmJJLnR4dA==", " ");
//        System.out.println(s);
//    }

}

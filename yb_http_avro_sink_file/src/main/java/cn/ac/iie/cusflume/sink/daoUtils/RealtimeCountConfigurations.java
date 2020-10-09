package cn.ac.iie.cusflume.sink.daoUtils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

//import com.nis.util.StringUtil;


public final class RealtimeCountConfigurations {

	private static Properties propCommon = new Properties();//0
	private static Properties propService = new Properties();//1

	public static String getStringProperty(Integer type, String key) {
		if(type == 0){
			return propCommon.getProperty(key);
		} else if(type == 1){
			return propService.getProperty(key);
		} else {
			return null;
		}
	}
	

	public static Integer getIntProperty(Integer type, String key) {
		if(type == 0){
			return Integer.parseInt(propCommon.getProperty(key));
		} else if(type == 1){
			return Integer.parseInt(propService.getProperty(key));
		} else {
			return null;
		}
	}

	public static Long getLongProperty(Integer type, String key) {
		if(type == 0){
			return Long.parseLong(propCommon.getProperty(key));
		} else if(type == 1){
			return Long.parseLong(propService.getProperty(key));
		} else {
			return null;
		}
	}
	
	public static Boolean getBooleanProperty(Integer type, String key) {
		if(type == 0){
			return propCommon.getProperty(key).toLowerCase().trim().equals("true");
		} else if(type == 1){
			return propService.getProperty(key).toLowerCase().trim().equals("true");
		} else {
			return null;
		}
	}

	static {
		try {
			InputStream resourceAsStream_realtime_service = RealtimeCountConfigurations.class.getClassLoader().getResourceAsStream("realtime_service_config.properties");//0,存放kafka相关配置
			if (resourceAsStream_realtime_service != null) {
				propCommon.load(new InputStreamReader(resourceAsStream_realtime_service, StandardCharsets.UTF_8));
			}

			InputStream resourceAsStream_config = RealtimeCountConfigurations.class.getClassLoader().getResourceAsStream("flume_config.properties");//1,存放flume自身相关配置(包括多线程)
			if (resourceAsStream_config != null) {
				propService.load(new InputStreamReader(resourceAsStream_config, StandardCharsets.UTF_8));
			}

			/*prop.load(new FileInputStream(System.getProperty("user.dir") 
					+ File.separator + "config"+File.separator + "config.properties"));*/
			System.out.println("realtime_service_config.properties加载成功");
			System.out.println("flume_config.properties加载成功");
					
		} catch (Exception e) {
			propCommon = null;
			propService = null;
			System.err.println("RealtimeCountConfigurations配置文件加载失败");
		}
	}
}

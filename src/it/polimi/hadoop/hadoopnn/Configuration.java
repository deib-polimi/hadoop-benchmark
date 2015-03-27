package it.polimi.hadoop.hadoopnn;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;

public class Configuration {
	
	private static final Logger logger = LoggerFactory.getLogger(Configuration.class);
	
	public static String ACCESS_KEY_ID = "";
	public static String SECRET_ACCESS_KEY = "";
	public static BasicAWSCredentials AWS_CREDENTIALS = null;
	public static String REGION = "us-west-1";
	
	public static final String SECURITY_GROUP_NAME = "HadoopNN";
	public static final String SECURITY_GROUP_DESC = "Security Group for HadoopNN";
	
	public static final String CONFIGURATION = "configuration.properties";
	public static final String FIREWALL_RULES = "firewallrules.csv";
	
	public static String ACTUAL_CONFIGURATION = CONFIGURATION;
	
	static {
		try {
			URL url = Configuration.class.getResource(CONFIGURATION);
			if (url == null)
				url = Configuration.class.getResource("/" + CONFIGURATION);
			loadConfiguration(url.getFile());
		} catch (Exception e) {
			logger.error("Error while configuring the system.", e);
		}
	}
	
	
	public static void saveConfiguration(String filePath) throws IOException {
		FileOutputStream fos = new FileOutputStream(filePath);
		Properties prop = new Properties();
		
		prop.put("ACCESS_KEY_ID", ACCESS_KEY_ID);
		prop.put("SECRET_ACCESS_KEY", SECRET_ACCESS_KEY);
		prop.put("REGION", REGION);
		
		prop.store(fos, "HadoopNN configuration properties");
		fos.flush();
	}
	
	public static void loadConfiguration(String filePath) throws IOException {
		Properties prop = new Properties();
		FileInputStream fis = new FileInputStream(filePath);
		prop.load(fis);
		
		ACCESS_KEY_ID = prop.getProperty("ACCESS_KEY_ID", ACCESS_KEY_ID);
		SECRET_ACCESS_KEY = prop.getProperty("SECRET_ACCESS_KEY", SECRET_ACCESS_KEY);
		if (ACCESS_KEY_ID != null && ACCESS_KEY_ID.length() > 0 && SECRET_ACCESS_KEY != null && SECRET_ACCESS_KEY.length() > 0)
			AWS_CREDENTIALS = new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
		else
			AWS_CREDENTIALS = null;
		REGION = prop.getProperty("REGION", REGION);
		
		ACTUAL_CONFIGURATION = filePath;
	}
	
	public static List<String> checkValidity() {
		ArrayList<String> errors = new ArrayList<String>();
		
		return errors;
	}
}

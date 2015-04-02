package it.polimi.hadoop.hadoopnn;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;

public class Configuration {
	
	private static final Logger logger = LoggerFactory.getLogger(Configuration.class);
	
	public static AWSCredentials AWS_CREDENTIALS = null;
	public static String REGION = "us-west-1";
	
	public static final String SECURITY_GROUP_NAME = "HadoopNN";
	public static final String SECURITY_GROUP_DESC = "Security Group for HadoopNN";
	
	public static final String CREDENTIALS = "credentials.properties";
	public static final String CONFIGURATION = "configuration.properties";
	public static final String FIREWALL_RULES = "firewallrules.csv";

	public static final String SECURITY_GROUP_FILE_NAME = "securitygroupcreated.txt";
	
	static {
		try {
			loadConfiguration(CONFIGURATION);
		} catch (Exception e) {
			logger.error("Error while configuring the system.", e);
		}
		
		try {
			AWS_CREDENTIALS = new PropertiesCredentials(getInputStream(CREDENTIALS));
		} catch (Exception e) {
			AWS_CREDENTIALS = null;
		}
	}
	
	public static InputStream getInputStream(String filePath) {
		InputStream is = Configuration.class.getResourceAsStream(filePath);
		if (is == null)
			is = Configuration.class.getResourceAsStream("/" + filePath);
		return is;
	}
	
	public static void saveConfiguration(String filePath) throws IOException {
		FileOutputStream fos = new FileOutputStream(filePath);
		Properties prop = new Properties();
		
		prop.put("REGION", REGION);
		
		prop.store(fos, "HadoopNN configuration properties");
		fos.flush();
	}
	
	public static void loadConfiguration(String filePath) throws IOException {
		Properties prop = new Properties();
		InputStream is = getInputStream(filePath);
		prop.load(is);
		
		REGION = prop.getProperty("REGION", REGION);
	}
	
	public static List<String> checkValidity() {
		ArrayList<String> errors = new ArrayList<String>();
		
		return errors;
	}
}

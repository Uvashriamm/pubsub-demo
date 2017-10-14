package com.example.utils;
 
import java.util.Properties;

public class Constants {
	protected static final Properties PROPERTIES = Utils.extractProperties();
	public static final String APPLICATION_NAME = PROPERTIES.getProperty("applicationname");
	public static final String KEYFILE = PROPERTIES.getProperty("keyfile");
	public static final String PROJECTID = PROPERTIES.getProperty("projectId");	
	public static final String DATASETID = PROPERTIES.getProperty("datasetId");	
	public static final int PULLMAXMSG = Integer.parseInt(PROPERTIES.getProperty("pullmaxmsg"));

	public static final String TOPIC = PROPERTIES.getProperty("topic");	

	public static final String SUBSCRIPTION = PROPERTIES.getProperty("subscription");	

	public static final String TABLE = PROPERTIES.getProperty("tableId");	
	
	public static final String BUCKET_NAME = PROPERTIES.getProperty("bucketname");	
	
	public static final String BUCKET_FOLDERNAME = PROPERTIES.getProperty("bucketfoldername");
}

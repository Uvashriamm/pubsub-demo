package com.example.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.InputStreamContent; 
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject; 

public class GcsUtils {
	private static final Logger LOGGER = Logger.getLogger(GcsUtils.class.getName()); 
	
	private GcsUtils() {
		
	}
	 public static StorageObject uploadSimple(Storage storage, String bucketName, String objectName,
		      String data) throws UnsupportedEncodingException, IOException {
		    return uploadSimple(storage, bucketName, objectName, new ByteArrayInputStream(
		        data.getBytes("UTF-8")), "text/plain");
		  }
	 public static StorageObject uploadSimple(Storage storage, String bucketName, String objectName,
		      File data) throws FileNotFoundException, IOException {
		    return uploadSimple(storage, bucketName, objectName, new FileInputStream(data),
		        "application/octet-stream");
		  }
	 public static StorageObject uploadSimple(Storage storage, String bucketName, String objectName,
		      InputStream data, String contentType) throws IOException {
		    InputStreamContent mediaContent = new InputStreamContent(contentType, data);
		    Storage.Objects.Insert insertObject = storage.objects().insert(bucketName, null, mediaContent)
		        .setName(objectName);
 
		    insertObject.getMediaHttpUploader().setDisableGZipContent(true);
		    return insertObject.execute();
		  }
	 
	public static Storage getService() {
		InputStream in = null;
		try {
			in = new FileInputStream(new File(Constants.KEYFILE));
			GoogleCredential credential = GoogleCredential.fromStream(in).createScoped(StorageScopes.all());
			Storage storage = new Storage.Builder(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(),
					new RetryHttpInitializerWrapper(credential)).setApplicationName(Constants.APPLICATION_NAME).build();
    		return storage;
		} catch(Exception ex) {
			LOGGER.info(ex.getMessage());
		}
		return null;
	}
 
}

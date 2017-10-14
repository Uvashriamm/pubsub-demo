package com.example;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import com.example.utils.Constants;
import com.example.utils.GcsUtils;
import com.example.utils.PubSubUtils;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.storage.model.StorageObject; 

public class TestCloudStorage {
	private static final Logger LOGGER = Logger.getLogger(TestCloudStorage.class.getName());

	public static void main(String[] args) {
		try {
			String msgPayload = FileUtils.readFileToString(new File("src/main/resources/PartyPayload.json"));
			List<String> messageIds = PubSubUtils.publishMessage(Constants.TOPIC, msgPayload);
			LOGGER.info("MessageId[" + messageIds + "]");

			
			List<ReceivedMessage> messages = PubSubUtils.pullMessage(Constants.SUBSCRIPTION);
			List<String> ackIds = new ArrayList<String>(); 
			
			//TODO: Dynamic file name - otherwise it will be replaced
			for (ReceivedMessage receivedMessage : messages) {
				String msg = new String(receivedMessage.getMessage().decodeData(), StandardCharsets.UTF_8.name());
				ackIds.add(receivedMessage.getAckId()); 
				InputStream stream = new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8.name()));
				StorageObject object = GcsUtils.uploadSimple(GcsUtils.getService(), 
						Constants.BUCKET_NAME,
						Constants.BUCKET_FOLDERNAME+"/sample.json", //FileName
						stream,
						"application/octet-stream");
				
				if(object.getName() != null) {
					PubSubUtils.acknowledgeMessage(Constants.SUBSCRIPTION, ackIds);
					LOGGER.info("Response[Success]");
				}
			}

		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}

}

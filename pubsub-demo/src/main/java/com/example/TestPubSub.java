package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.logging.Logger;

import com.example.utils.BigQueryUtils;
import com.example.utils.Constants;
import com.example.utils.PubSubUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;

public class TestPubSub {
	private static final Logger LOGGER = Logger.getLogger(TestPubSub.class.getName());

	public static void main(String[] args) {
		try {
			String message = "{\"name\": \"John\", \"age\": \"30\", \"cars\": [ \"Test\", \"BMW\", \"Fiat\" ]}";
			List<String> messageIds = PubSubUtils.publishMessage(Constants.TOPIC, message);
			LOGGER.info("MessageId[" + messageIds + "]");

			List<ReceivedMessage> messages = PubSubUtils.pullMessage(Constants.SUBSCRIPTION);
			List<String> ackIds = new ArrayList<String>();
			TableId tableId = TableId.of(Constants.DATASETID, Constants.TABLE);
			
			for (ReceivedMessage receivedMessage : messages) {
				String msg = new String(receivedMessage.getMessage().decodeData(), "UTF-8");
				ackIds.add(receivedMessage.getAckId());
				@SuppressWarnings("unchecked")
				HashMap<String, Object> mapResult = new ObjectMapper().readValue(msg, HashMap.class);
				InsertAllResponse response = BigQueryUtils.getService().insertAll(
						InsertAllRequest.newBuilder(tableId).addRow(UUID.randomUUID().toString(), mapResult).build());
				if (response.hasErrors()) {
					for (Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
						LOGGER.info("Error[" + entry + "]");
					}
				} else {
					PubSubUtils.acknowledgeMessage(Constants.SUBSCRIPTION, ackIds);
					LOGGER.info("Response[Success]");
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.info(e.getMessage());
		}

	}

}

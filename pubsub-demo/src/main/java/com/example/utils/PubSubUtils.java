package com.example.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.common.collect.ImmutableList; 

public final class PubSubUtils {
	private static final Logger LOGGER = Logger.getLogger(PubSubUtils.class.getName());

	/**
	 * Get the PubSub connection for the configured application name
	 * 
	 * @return Pubsub
	 * @throws IOException
	 */
	private PubSubUtils() {
	}

	public static Pubsub getPubSubClient() {
		InputStream in = null;
		try {
			in = new FileInputStream(new File(Constants.KEYFILE));
			GoogleCredential credential = GoogleCredential.fromStream(in).createScoped(PubsubScopes.all());
			return new Pubsub.Builder(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(),
					new RetryHttpInitializerWrapper(credential)).setApplicationName(Constants.APPLICATION_NAME).build();
		} catch (Exception ex) {
			LOGGER.info(ex.getMessage());
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				LOGGER.info(e.getMessage());
			}
		}

		return null;
	}

	/**
	 * Create the Topic
	 * 
	 * @param Pubsub
	 * @param topicName
	 * @throws GoogleJsonResponseException
	 * @throws Exception
	 */
	public static void createTopic(Pubsub client, String topicName) throws Exception {
		try {
			client.projects().topics().create(topicName, new Topic()).execute();
		} catch (Exception ex) {
			throw new Exception(ex);
		}
	}

	/**
	 * Get the Topic
	 * 
	 * @param client
	 * @param topicName
	 * @return Topic
	 * @throws GoogleJsonResponseException
	 * @throws Exception
	 */
	public static Topic getTopic(Pubsub client, String topicName) throws Exception {
		Topic topic = null;
		try {
			topic = client.projects().topics().get(topicName).execute();
		} catch (Exception ex) {
			throw new Exception(ex);
		}
		return topic;
	}

	/**
	 * Get the full Directory of the Topic
	 * 
	 * @param topicName
	 * @return full directory of the topic
	 */
	public static String getTopicName(String topicName) {
		return String.format("projects/%s/topics/%s", Constants.PROJECTID, topicName);
	}

	/**
	 * Create the subscription
	 * 
	 * @param client
	 * @param topicName
	 * @param subscriptionName
	 * @throws Exception
	 */
	public static void createSubscription(Pubsub client, String topicName, String subscriptionName)
			throws Exception {
		try {
			Subscription subscription = new Subscription().setTopic(topicName);
			client.projects().subscriptions().create(subscriptionName, subscription).execute();
		} catch (Exception ex) {
			throw new Exception(ex);
		}
	}

	/**
	 * Get the Subcription
	 * 
	 * @param client
	 * @param subscriptionName
	 * @throws Exception
	 */
	public static void getSubscription(Pubsub client, String subscriptionName) throws Exception {
		try {
			client.projects().subscriptions().get(subscriptionName).execute();
		} catch (Exception ex) {
			throw new Exception(ex);
		}
	}

	/**
	 * Get the Subcription with directory
	 * 
	 * @param subscriptionName
	 * @return full directory of the Subcription
	 */
	public static String getSubscriptionName(String subscriptionName) {
		return String.format("projects/%s/subscriptions/%s", Constants.PROJECTID, subscriptionName);
	}

	/**
	 * Publish the message into the topic
	 * 
	 * @param topicName
	 * @param message
	 * @return List of messageIds
	 * @throws Exception
	 */
	public static List<String> publishMessage(String topicName, String message) throws Exception {
		List<String> messageIds = null;
		try {
			Pubsub client = getPubSubClient();
			PubsubMessage pubsubMessage = new PubsubMessage();
			pubsubMessage.encodeData(message.getBytes("UTF-8"));
			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessages(ImmutableList.of(pubsubMessage));
			PublishResponse publishResponse = client.projects().topics()
					.publish(PubSubUtils.getTopicName(topicName), publishRequest).execute();
			messageIds = publishResponse.getMessageIds();
			return messageIds;
		} catch (Exception ex) {
			throw new Exception(ex);
		}
	}

	/**
	 * Pull the message from the subscription
	 * 
	 * @param subscriptionName
	 * @return List of messages with ackId
	 * @throws Exception
	 */
	public static List<ReceivedMessage> pullMessage(String subscriptionName) throws Exception {
		List<ReceivedMessage> messages = null;
		try {
			Pubsub client = getPubSubClient();
			PullRequest pullRequest = new PullRequest().setReturnImmediately(true).setMaxMessages(Constants.PULLMAXMSG);
			PullResponse response = client.projects().subscriptions()
					.pull(PubSubUtils.getSubscriptionName(subscriptionName), pullRequest).execute();
			messages = response.getReceivedMessages();
		} catch (Exception ex) {
			throw new Exception(ex);
		}
		return messages;
	}

	/**
	 * Send acknowledge for the received messages
	 * 
	 * @param subscriptionName
	 * @param ackIds
	 * @throws Exception
	 */

	public static void acknowledgeMessage(String subscriptionName, List<String> ackIds) throws Exception {
		try {
			LOGGER.info("Acknowledged[" + ackIds +"]");
			Pubsub client = getPubSubClient();
			AcknowledgeRequest ackRequest = new AcknowledgeRequest().setAckIds(ackIds);
			client.projects().subscriptions().acknowledge(PubSubUtils.getSubscriptionName(subscriptionName), ackRequest).execute();
		} catch (Exception ex) {
			throw new Exception(ex);
		}
	}
}

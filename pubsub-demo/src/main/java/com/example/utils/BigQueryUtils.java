package com.example.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

public class BigQueryUtils {
	private static final Logger LOGGER = Logger.getLogger(BigQueryUtils.class.getName());
	
	private BigQueryUtils() {
	}

	public static BigQuery getService() {
		InputStream in = null;
		try {
			in = new FileInputStream(new File(Constants.KEYFILE));
			GoogleCredentials credential = GoogleCredentials.fromStream(in).createScoped(BigqueryScopes.all());

			BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credential).setProjectId("pearson-mdm-dev")
					.build().getService();
			return bigquery;
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
}

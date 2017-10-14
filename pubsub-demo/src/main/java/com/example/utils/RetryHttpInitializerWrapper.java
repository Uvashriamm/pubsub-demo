package com.example.utils;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.common.base.Preconditions;

import java.io.IOException;

public class RetryHttpInitializerWrapper implements HttpRequestInitializer {
	private static final int ONEMINITUES = 60000;

	private final Credential wrappedCredential;
	private final Sleeper sleeper;

	public RetryHttpInitializerWrapper(final Credential wrappedCredential) {
		this(wrappedCredential, Sleeper.DEFAULT);
	}

	RetryHttpInitializerWrapper(final Credential wrappedCredential, final Sleeper sleeper) {
		this.wrappedCredential = Preconditions.checkNotNull(wrappedCredential);
		this.sleeper = sleeper;
	}
	

	public final void initialize(final HttpRequest request) {
		request.setReadTimeout(2 * ONEMINITUES);
		final HttpUnsuccessfulResponseHandler backoffHandler = new HttpBackOffUnsuccessfulResponseHandler(
				new ExponentialBackOff()).setSleeper(sleeper);
		request.setInterceptor(wrappedCredential);
		request.setUnsuccessfulResponseHandler(new HttpUnsuccessfulResponseHandler() {
			 
			public boolean handleResponse(final HttpRequest request, final HttpResponse response,
					final boolean supportsRetry) throws IOException {
				if (wrappedCredential.handleResponse(request, response, supportsRetry)) {
					return true;
				} else if (backoffHandler.handleResponse(request, response, supportsRetry)) {
					return true;
				} else {
					return false;
				}
			}
		});
		request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(new ExponentialBackOff()).setSleeper(sleeper));
	}
}

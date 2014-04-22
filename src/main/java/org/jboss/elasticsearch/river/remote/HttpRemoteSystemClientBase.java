/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Base class used for HTTP based remote clients. Solves authentication etc.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public abstract class HttpRemoteSystemClientBase implements IRemoteSystemClient {

	protected static final String CFG_PASSWORD = "pwd";

	protected static final String CFG_USERNAME = "username";

	protected static final String CFG_TIMEOUT = "timeout";

	protected ESLogger myLogger = null;

	protected HttpClient httpclient;

	protected boolean isAuthConfigured = false;

	protected IDocumentIndexStructureBuilder indexStructureBuilder;

	/**
	 * DO NOT FORGET to call this from {@link #init(Map, boolean, IPwdLoader)} in your subclass!!!!
	 * 
	 * @param logger to beused
	 * @param config to be read
	 * @param pwdLoader to be used (can be null)
	 * @param url base url to be used for authentication config - host part is used
	 * @return username for authentication if any configured
	 */
	protected String initHttpClient(ESLogger logger, Map<String, Object> config, IPwdLoader pwdLoader, String url) {
		this.myLogger = logger;

		PoolingClientConnectionManager connectionManager = new PoolingClientConnectionManager();
		connectionManager.setDefaultMaxPerRoute(20);
		connectionManager.setMaxTotal(20);

		DefaultHttpClient httpclientImpl = new DefaultHttpClient(connectionManager);
		httpclient = httpclientImpl;
		HttpParams params = httpclient.getParams();
		params.setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET, "UTF-8");

		Integer timeout = new Long(Utils.parseTimeValue(config, CFG_TIMEOUT, 5, TimeUnit.SECONDS)).intValue();
		if (timeout != null) {
			params.setParameter(CoreConnectionPNames.SO_TIMEOUT, timeout);
			params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, timeout);
		}

		String remoteUsername = Utils.trimToNull(XContentMapValues.nodeStringValue(config.get(CFG_USERNAME), null));
		String remotePassword = XContentMapValues.nodeStringValue(config.get(CFG_PASSWORD), null);
		if (remoteUsername != null) {
			if (remotePassword == null && pwdLoader != null)
				remotePassword = pwdLoader.loadPassword(remoteUsername);
			if (remotePassword != null) {
				try {
					URL urlParsed = new URL(url);
					String host = urlParsed.getHost();
					httpclientImpl.getCredentialsProvider().setCredentials(new AuthScope(host, AuthScope.ANY_PORT),
							new UsernamePasswordCredentials(remoteUsername, remotePassword));
					isAuthConfigured = true;
				} catch (MalformedURLException e) {
					// this should never happen due validation before
				}
			} else {
				logger.warn("Password not found so authentication is not used!");
				remoteUsername = null;
			}
		} else {
			remoteUsername = null;
		}
		return remoteUsername;
	}

	/**
	 * Get url from configuration and validate it for format, and for presence.
	 * 
	 * @param config to get URL from
	 * @param cfgProperyName name of config property with URL
	 * @param mandatory if URL is mandatory so validation is performed
	 * @return url
	 * @throws SettingsException in case of validation error
	 */
	protected static String getUrlFromConfig(Map<String, Object> config, String cfgProperyName, boolean mandatory)
			throws SettingsException {
		String url = null;
		if (config != null)
			url = Utils.trimToNull(XContentMapValues.nodeStringValue(config.get(cfgProperyName), null));
		if (mandatory && url == null) {
			throw new SettingsException("remote/" + cfgProperyName + " element of configuration structure not found or empty");
		}
		if (url != null) {
			try {
				new URL(url);
			} catch (MalformedURLException e) {
				throw new SettingsException("Parameter remote/" + cfgProperyName + " is malformed URL " + e.getMessage());
			}
		}
		return url;
	}

	/**
	 * Perform defined HTTP GET request.
	 * 
	 * @param url to perform GET request for
	 * @param headers to be used for request. Can be null.
	 * @return response from server if successful
	 * @throws HttpCallException in case of failed http call
	 * @throws Exception in case of unsuccessful call
	 */
	protected HttpResponseContent performHttpGetCall(String url, Map<String, String> headers) throws Exception,
			HttpCallException {

		myLogger.debug("Going to perform remote system HTTP GET request to the the {}", url);

		URIBuilder builder = new URIBuilder(url);
		HttpGet method = new HttpGet(builder.build());
		if (headers != null) {
			for (String headerName : headers.keySet())
				method.addHeader(headerName, headers.get(headerName));
		}
		try {

			BasicHttpContext localcontext = new BasicHttpContext();
			if (isAuthConfigured) {
				// Preemptive authentication enabled - see
				// http://hc.apache.org/httpcomponents-client-ga/tutorial/html/authentication.html#d5e1032
				HttpHost targetHost = new HttpHost(builder.getHost(), builder.getPort(), builder.getScheme());
				AuthCache authCache = new BasicAuthCache();
				BasicScheme basicAuth = new BasicScheme();
				authCache.put(targetHost, basicAuth);
				localcontext.setAttribute(ClientContext.AUTH_CACHE, authCache);
			}

			HttpResponse response = httpclient.execute(method, localcontext);
			int statusCode = response.getStatusLine().getStatusCode();
			byte[] responseContent = null;
			if (response.getEntity() != null) {
				responseContent = EntityUtils.toByteArray(response.getEntity());
			}
			if (statusCode != HttpStatus.SC_OK) {
				throw new HttpCallException(url, statusCode, responseContent != null ? new String(responseContent) : "");
			}
			Header h = response.getFirstHeader("Content-Type");

			return new HttpResponseContent(h != null ? h.getValue() : null, responseContent);
		} finally {
			method.releaseConnection();
		}
	}

	public static final class HttpResponseContent {
		public String contentType;
		public byte[] content;

		public HttpResponseContent(String contentType, byte[] content) {
			super();
			this.contentType = contentType;
			this.content = content;
		}

		@Override
		public String toString() {
			return "HttpResponseContent [contentType=" + contentType + ", content=" + content != null ? new String(content)
					: "" + "]";
		}

	}

	public static final class HttpCallException extends Exception {
		int statusCode;

		public HttpCallException(String url, int statusCode, String responseContent) {
			super("Failed remote system HTTP GET request to the url '" + url + "'. HTTP error code: " + statusCode
					+ " Response body: " + responseContent);
			this.statusCode = statusCode;
		}

		public int getStatusCode() {
			return statusCode;
		}

	}

	@Override
	public void setIndexStructureBuilder(IDocumentIndexStructureBuilder indexStructureBuilder) {
		this.indexStructureBuilder = indexStructureBuilder;
	}

	@Override
	public IDocumentIndexStructureBuilder getIndexStructureBuilder() {
		return indexStructureBuilder;
	}

}

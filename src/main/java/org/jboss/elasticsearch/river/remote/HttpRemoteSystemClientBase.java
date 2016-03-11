/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
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

	protected static final String CFG_EMBED_URL_API_KEY = "embedUrlApiKey";

	protected static final String CFG_EMBED_URL_API_KEY_USERNAME = "embedUrlApiKeyUsername";

	protected ESLogger myLogger = null;

	protected CloseableHttpClient httpclient;

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

		PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
		connManager.setDefaultMaxPerRoute(20);
		connManager.setMaxTotal(20);

		ConnectionConfig connectionConfig = ConnectionConfig.custom().setCharset(Consts.UTF_8).build();
		connManager.setDefaultConnectionConfig(connectionConfig);

		HttpClientBuilder clientBuilder = HttpClients.custom().setConnectionManager(connManager);

		Integer timeout = new Long(Utils.parseTimeValue(config, CFG_TIMEOUT, 5, TimeUnit.SECONDS)).intValue();

		if (timeout != null) {
			RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).build();
			clientBuilder.setDefaultRequestConfig(requestConfig);
		}

		String remoteUsername = Utils.trimToNull(XContentMapValues.nodeStringValue(config.get(CFG_USERNAME), null));
		String remotePassword = XContentMapValues.nodeStringValue(config.get(CFG_PASSWORD), null);
		HashMap <String,String> dan;
		if (remoteUsername != null) {
			if (remotePassword == null && pwdLoader != null) {
				remotePassword = (pwdLoader.loadKey(remoteUsername)).get("pwd");

			}
			if (remotePassword != null) {
				try {
					URL urlParsed = new URL(url);
					String host = urlParsed.getHost();
					CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
					credentialsProvider.setCredentials(new AuthScope(host, AuthScope.ANY_PORT), new UsernamePasswordCredentials(
							remoteUsername, remotePassword));
					clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
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
		httpclient = clientBuilder.build();
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

		return performHttpCall(url, headers, HttpMethodType.GET);
	}
	
	/**
     * Perform defined HTTP POST request.
     * 
     * @param url to perform POST request for
     * @param headers to be used for request. Can be null.
     * @return response from server if successful
     * @throws HttpCallException in case of failed http call
     * @throws Exception in case of unsuccessful call
     */
    protected HttpResponseContent performHttpPostCall(String url, Map<String, String> headers) throws Exception,
            HttpCallException {

        return performHttpCall(url, headers, HttpMethodType.POST);
    }
	
	/**
     * This method performs a HTTP request with the defined GET or POST method. Using GET as default if not defined.
     * @param url to perform GET request for
     * @param headers to be used for request. Can be null.
     * @param method either GET(default) or POST http method type.
     * @return response from server if successful
     * @throws HttpCallException in case of failed http call
     * @throws Exception in case of unsuccessful call
     */
    protected HttpResponseContent performHttpCall(String url, Map<String, String> headers, HttpMethodType methodType) 
           throws Exception, HttpCallException {
        
        myLogger.debug("Going to perform remote system HTTP request to the the {}", url);
        
        HttpRequestBase method = null;
        URIBuilder builder = null;
        if ( methodType!=null && methodType.compareTo(HttpMethodType.POST)==0 ) {
            
            // For POST method we need to migrate URL parameters to POST params.
            builder = new URIBuilder(url);
            String urlWithoutParams = url.split("\\?")[0];
            HttpPost postMethod = new HttpPost(urlWithoutParams);
            postMethod.setEntity(new UrlEncodedFormEntity(builder.getQueryParams()));
            method = postMethod;
            
        } else {
            
            builder = new URIBuilder(url);
            method = new HttpGet(builder.build());
            
        }
        
        if (headers != null) {
            for (String headerName : headers.keySet())
                method.addHeader(headerName, headers.get(headerName));
        }
        CloseableHttpResponse response = null;
        try {
            HttpHost targetHost = new HttpHost(builder.getHost(), builder.getPort(), builder.getScheme());
     
            HttpClientContext localcontext = HttpClientContext.create();
            if (isAuthConfigured) {
                AuthCache authCache = new BasicAuthCache();
                BasicScheme basicAuth = new BasicScheme();
                authCache.put(targetHost, basicAuth);
                localcontext.setAuthCache(authCache);
            }
     
            response = httpclient.execute(targetHost, method, localcontext);
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
            if (response != null)
                response.close();
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
	
	
	/**
	 * This small helper enum describes the only two supported http call methods GET and POST.
	 */
	public static enum HttpMethodType {
	    GET, POST;
	}

}

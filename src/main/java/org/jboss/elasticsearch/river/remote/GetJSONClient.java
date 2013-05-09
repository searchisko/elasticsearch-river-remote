/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
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
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Class used to call remote system by http GET operation with JSON response.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class GetJSONClient implements IRemoteSystemClient {

	protected static final String CFG_GET_DOCS_RES_FIELD_TOTALCOUNT = "getDocsResFieldTotalcount";

	protected static final String CFG_GET_DOCS_RES_FIELD_DOCUMENTS = "getDocsResFieldDocuments";

	protected static final String CFG_PASSWORD = "pwd";

	protected static final String CFG_USERNAME = "username";

	protected static final String CFG_TIMEOUT = "timeout";

	protected static final String CFG_GET_SPACES_RESPONSE_FIELD = "getSpacesResField";

	protected static final String CFG_URL_GET_SPACES = "urlGetSpaces";

	protected static final String CFG_URL_GET_DOCUMENTS = "urlGetDocuments";

	private static final ESLogger logger = Loggers.getLogger(GetJSONClient.class);

	private DefaultHttpClient httpclient;

	protected String urlGetSpaces;

	protected String getSpacesResField;

	protected String getDocsResFieldDocuments;

	protected String getDocsResFieldTotalcount;

	protected String urlGetDocuments;

	protected boolean isAuthConfigured = false;

	protected IDocumentIndexStructureBuilder indexStructureBuilder;

	/**
	 * Default constructor.
	 */
	public GetJSONClient() {

	}

	@Override
	public void init(Map<String, Object> config, boolean spaceListLoadingEnabled) {
		urlGetDocuments = XContentMapValues.nodeStringValue(config.get(CFG_URL_GET_DOCUMENTS), null);
		if (Utils.isEmpty(urlGetDocuments)) {
			throw new SettingsException("remote/urlGetDocuments element of configuration structure not found or empty");
		}

		URL urlGetDocumentsUrl = null;
		try {
			urlGetDocumentsUrl = new URL(urlGetDocuments);
		} catch (MalformedURLException e) {
			throw new SettingsException("Parameter remote/urlGetDocuments is malformed " + e.getMessage());
		}
		getDocsResFieldDocuments = Utils.trimToNull(XContentMapValues.nodeStringValue(
				config.get(CFG_GET_DOCS_RES_FIELD_DOCUMENTS), null));
		getDocsResFieldTotalcount = Utils.trimToNull(XContentMapValues.nodeStringValue(
				config.get(CFG_GET_DOCS_RES_FIELD_TOTALCOUNT), null));

		URL urlGetSpacesUrl = null;
		if (spaceListLoadingEnabled) {
			urlGetSpaces = XContentMapValues.nodeStringValue(config.get(CFG_URL_GET_SPACES), null);
			if (Utils.isEmpty(urlGetSpaces)) {
				throw new SettingsException("remote/urlGetSpaces element of configuration structure not found or empty");
			}
			try {
				urlGetSpacesUrl = new URL(urlGetSpaces);
			} catch (MalformedURLException e) {
				throw new SettingsException("Parameter remote/urlGetSpaces is malformed " + e.getMessage());
			}
			getSpacesResField = Utils.trimToNull(XContentMapValues.nodeStringValue(config.get(CFG_GET_SPACES_RESPONSE_FIELD),
					null));
		}

		PoolingClientConnectionManager connectionManager = new PoolingClientConnectionManager();
		connectionManager.setDefaultMaxPerRoute(20);
		connectionManager.setMaxTotal(20);

		httpclient = new DefaultHttpClient(connectionManager);
		HttpParams params = httpclient.getParams();
		params.setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET, "UTF-8");

		Integer timeout = new Long(Utils.parseTimeValue(config, CFG_TIMEOUT, 5, TimeUnit.SECONDS)).intValue();
		if (timeout != null) {
			params.setParameter(CoreConnectionPNames.SO_TIMEOUT, timeout);
			params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, timeout);
		}

		String remoteUsername = Utils.trimToNull(XContentMapValues.nodeStringValue(config.get(CFG_USERNAME), null));
		String remotePassword = Utils.trimToNull(XContentMapValues.nodeStringValue(config.get(CFG_PASSWORD), null));
		if (remoteUsername != null) {
			String host = urlGetDocumentsUrl.getHost();
			httpclient.getCredentialsProvider().setCredentials(new AuthScope(host, AuthScope.ANY_PORT),
					new UsernamePasswordCredentials(remoteUsername, remotePassword));
			isAuthConfigured = true;
		} else {
			remoteUsername = null;
		}

		logger
				.info(
						"Configured GET JSON remote client. Spaces listing URL '{}', documents listing url '{}', remote system user '{}'.",
						urlGetSpacesUrl != null ? urlGetSpacesUrl : "unused", urlGetDocumentsUrl,
						remoteUsername != null ? remoteUsername : "Anonymous access");

	}

	@Override
	@SuppressWarnings("unchecked")
	public List<String> getAllSpaces() throws Exception {
		byte[] responseData = performGetRESTCall(urlGetSpaces);
		logger.debug("Get Spaces REST response data: {}", new String(responseData));

		Object responseParsed = parseJSONResponse(responseData);

		List<String> ret = new ArrayList<String>();
		if (getSpacesResField != null) {
			if (responseParsed instanceof Map) {
				responseParsed = XContentMapValues.extractValue(getSpacesResField, ((Map<String, Object>) responseParsed));
			} else {
				throw new Exception(
						"Get Spaces REST response structure is unsupported (we need a Map to take configured getSpacesResponseField from it) "
								+ responseParsed);
			}
		}
		try {
			if (responseParsed instanceof List) {
				List<Object> l = (List<Object>) responseParsed;
				for (Object s : l) {
					ret.add(s.toString());
				}
			} else if (responseParsed instanceof Map) {
				Map<String, Object> l = (Map<String, Object>) responseParsed;
				for (String s : l.keySet()) {
					ret.add(s);
				}
			} else {
				throw new Exception("Get Spaces REST response structure is unsupported " + responseParsed);
			}
		} catch (ClassCastException e) {
			throw new Exception("Get Spaces REST response structure is unsupported " + responseParsed);
		}
		return ret;
	}

	/**
	 * Parse JSON response into Object Structure.
	 * 
	 * @param responseData to parse
	 * @return parsed response (May be Map, or List, or simple value)
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	protected Object parseJSONResponse(byte[] responseData) throws UnsupportedEncodingException, IOException {
		XContentParser parser = null;
		// wrap response so we are able to process JSON array of values on root level of response, which is not supported
		// by XContentFactory
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("{ \"wrapit\" : ").append(new String(responseData, "UTF-8")).append("}");
			responseData = sb.toString().getBytes("UTF-8");
			parser = XContentFactory.xContent(XContentType.JSON).createParser(responseData);
			Map<String, Object> wrappedResponseParsed = parser.mapAndClose();
			return wrappedResponseParsed.get("wrapit");
		} finally {
			if (parser != null)
				parser.close();
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public ChangedDocumentsResults getChangedDocuments(String spaceKey, int startAt, Date updatedAfter) throws Exception {
		String url = enhanceUrlGetDocuments(urlGetDocuments, spaceKey, updatedAfter, startAt);
		byte[] responseData = performGetRESTCall(url);

		logger.debug("Get Documents REST response data: {}", new String(responseData));

		try {
			Object responseParsed = parseJSONResponse(responseData);
			Integer total = null;
			if (getDocsResFieldTotalcount != null) {
				Object totalObj = XContentMapValues.extractValue(getDocsResFieldTotalcount, (Map) responseParsed);
				if (totalObj != null) {
					if (totalObj instanceof Integer)
						total = (Integer) totalObj;
					else
						try {
							total = Integer.parseInt(totalObj.toString());
						} catch (NumberFormatException e) {
							throw new Exception(
									"Value from configured getDocsResFieldTotalcount field is not convertable to number: " + totalObj);
						}
				} else {
					throw new Exception("Configured getDocsResFieldTotalcount field has no value");
				}
			}

			List<Map<String, Object>> documents = null;
			if (getDocsResFieldDocuments != null) {
				documents = (List<Map<String, Object>>) XContentMapValues.extractValue(getDocsResFieldDocuments,
						(Map) responseParsed);
			} else {
				documents = (List<Map<String, Object>>) responseParsed;
			}

			return new ChangedDocumentsResults(documents, startAt, total);
		} catch (ClassCastException e) {
			throw new Exception("Get Documents REST response structure is invalid " + responseData);
		}
	}

	protected static String enhanceUrlGetDocuments(String url, String spaceKey, Date updatedAfter, int startAt) {
		url = url.replaceAll("\\{space\\}", spaceKey);
		url = url.replaceAll("\\{updatedAfter\\}", updatedAfter != null ? updatedAfter.getTime() + "" : "");
		url = url.replaceAll("\\{startAtIndex\\}", startAt + "");
		return url;
	}

	/**
	 * Perform defined REST call to remote REST API.
	 * 
	 * @param url to perform GET request for
	 * @return response from server if successful
	 * @throws Exception in case of unsuccessful call
	 */
	protected byte[] performGetRESTCall(String url) throws Exception {

		logger.debug("Going to perform remote system HTTP GET REST API call to the the {}", url);

		URIBuilder builder = new URIBuilder(url);
		HttpGet method = new HttpGet(builder.build());
		method.addHeader("Accept", "application/json");
		try {

			// Preemptive authentication enabled - see
			// http://hc.apache.org/httpcomponents-client-ga/tutorial/html/authentication.html#d5e1032
			HttpHost targetHost = new HttpHost(builder.getHost(), builder.getPort(), builder.getScheme());
			AuthCache authCache = new BasicAuthCache();
			BasicScheme basicAuth = new BasicScheme();
			authCache.put(targetHost, basicAuth);
			BasicHttpContext localcontext = new BasicHttpContext();
			localcontext.setAttribute(ClientContext.AUTH_CACHE, authCache);

			HttpResponse response = httpclient.execute(method, localcontext);
			int statusCode = response.getStatusLine().getStatusCode();
			byte[] responseContent = null;
			if (response.getEntity() != null) {
				responseContent = EntityUtils.toByteArray(response.getEntity());
			}
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Failed remote system REST API call. HTTP error code: " + statusCode + " Response body: "
						+ new String(responseContent));
			}
			return responseContent;
		} finally {
			method.releaseConnection();
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

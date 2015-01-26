/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.BasicHttpContext;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SettingsException;
import org.jboss.elasticsearch.river.remote.HttpRemoteSystemClientBase.HttpCallException;
import org.jboss.elasticsearch.river.remote.HttpRemoteSystemClientBase.HttpResponseContent;
import org.jboss.elasticsearch.river.remote.exception.RemoteDocumentNotFoundException;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Unit test for {@link HttpRemoteSystemClientBase}
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class HttpRemoteSystemClientBaseTest {

	@Test
	public void set_getIndexStructureBuilder() {
		HttpRemoteSystemClientBase tested = getTested();

		IDocumentIndexStructureBuilder indexStructureBuilder = Mockito.mock(IDocumentIndexStructureBuilder.class);
		tested.setIndexStructureBuilder(indexStructureBuilder);
		Assert.assertEquals(indexStructureBuilder, tested.getIndexStructureBuilder());
	}

	@Test
	public void getUrlFromConfig_success() {

		Assert.assertNull(HttpRemoteSystemClientBase.getUrlFromConfig(null, "myprop", false));

		Map<String, Object> config = new HashMap<String, Object>();
		Assert.assertNull(HttpRemoteSystemClientBase.getUrlFromConfig(config, "myprop", false));

		config.put("myprop", "");
		Assert.assertNull(HttpRemoteSystemClientBase.getUrlFromConfig(config, "myprop", false));

		config.put("myprop", "   ");
		Assert.assertNull(HttpRemoteSystemClientBase.getUrlFromConfig(config, "myprop", false));

		config.put("myprop", "http://www.test.org");
		Assert.assertEquals("http://www.test.org", HttpRemoteSystemClientBase.getUrlFromConfig(config, "myprop", false));
	}

	@Test(expected = SettingsException.class)
	public void getUrlFromConfig_fail_mandatory() {
		HttpRemoteSystemClientBase.getUrlFromConfig(null, "myprop", true);
	}

	@Test(expected = SettingsException.class)
	public void getUrlFromConfig_fail_mandatory2() {
		Map<String, Object> config = new HashMap<String, Object>();
		HttpRemoteSystemClientBase.getUrlFromConfig(config, "myprop", true);
	}

	@Test(expected = SettingsException.class)
	public void getUrlFromConfig_fail_mandatory3() {
		Map<String, Object> config = new HashMap<String, Object>();
		config.put("myprop", "");
		HttpRemoteSystemClientBase.getUrlFromConfig(config, "myprop", true);
	}

	@Test(expected = SettingsException.class)
	public void getUrlFromConfig_fail_mandatory4() {
		Map<String, Object> config = new HashMap<String, Object>();
		config.put("myprop", "   ");
		HttpRemoteSystemClientBase.getUrlFromConfig(config, "myprop", true);
	}

	@Test(expected = SettingsException.class)
	public void getUrlFromConfig_fail_format() {
		Map<String, Object> config = new HashMap<String, Object>();
		config.put("myprop", "adf");
		HttpRemoteSystemClientBase.getUrlFromConfig(config, "myprop", true);
	}

	@Test
	public void performHttpGetCall_succes_noHeaders() throws HttpCallException, Exception {
		HttpRemoteSystemClientBase tested = getTested();
		tested.myLogger = Loggers.getLogger("test logger");
		tested.httpclient = Mockito.mock(CloseableHttpClient.class);

		Mockito.when(
				tested.httpclient.execute(Mockito.any(HttpHost.class), Mockito.any(HttpGet.class),
						Mockito.any(BasicHttpContext.class))).thenAnswer(
				prepereHttpResponseAnswer(HttpStatus.SC_OK, "response", "text/plain", null, false));

		HttpResponseContent ret = tested.performHttpGetCall("http://test.org", null);
		Assert.assertEquals("response", new String(ret.content));
		Assert.assertEquals("text/plain", new String(ret.contentType));
		Mockito.verify(tested.httpclient).execute(Mockito.any(HttpHost.class), Mockito.any(HttpGet.class),
				Mockito.any(BasicHttpContext.class));
		Mockito.verifyNoMoreInteractions(tested.httpclient);
	}

	@Test
	public void performHttpGetCall_succes_headers_auth() throws HttpCallException, Exception {
		HttpRemoteSystemClientBase tested = getTested();
		tested.myLogger = Loggers.getLogger("test logger");
		tested.httpclient = Mockito.mock(CloseableHttpClient.class);
		tested.isAuthConfigured = true;

		Map<String, String> headers = new HashMap<String, String>();
		headers.put("Accept", "application/json");

		Mockito.when(
				tested.httpclient.execute(Mockito.any(HttpHost.class), Mockito.any(HttpGet.class),
						Mockito.any(BasicHttpContext.class))).thenAnswer(
				prepereHttpResponseAnswer(HttpStatus.SC_OK, "response", "text/any", headers, true));

		HttpResponseContent ret = tested.performHttpGetCall("http://test.org", headers);
		Assert.assertEquals("response", new String(ret.content));
		Assert.assertEquals("text/any", new String(ret.contentType));
		Mockito.verify(tested.httpclient).execute(Mockito.any(HttpHost.class), Mockito.any(HttpGet.class),
				Mockito.any(BasicHttpContext.class));
		Mockito.verifyNoMoreInteractions(tested.httpclient);
	}

	@Test
	public void performHttpGetCall_error_entity() throws HttpCallException, Exception {
		HttpRemoteSystemClientBase tested = getTested();
		tested.myLogger = Loggers.getLogger("test logger");
		tested.httpclient = Mockito.mock(CloseableHttpClient.class);

		Mockito.when(
				tested.httpclient.execute(Mockito.any(HttpHost.class), Mockito.any(HttpGet.class),
						Mockito.any(BasicHttpContext.class))).thenAnswer(
				prepereHttpResponseAnswer(HttpStatus.SC_NOT_FOUND, "response", "text/plain", null, false));

		try {
			tested.performHttpGetCall("http://test.org", null);
			Assert.fail("HttpCallException expected");
		} catch (HttpCallException e) {
			Assert
					.assertEquals(
							"Failed remote system HTTP GET request to the url 'http://test.org'. HTTP error code: 404 Response body: response",
							e.getMessage());
			Mockito.verify(tested.httpclient).execute(Mockito.any(HttpHost.class), Mockito.any(HttpGet.class),
					Mockito.any(BasicHttpContext.class));
			Mockito.verifyNoMoreInteractions(tested.httpclient);
		}
	}

	@Test
	public void performHttpGetCall_error_no_entity() throws HttpCallException, Exception {
		HttpRemoteSystemClientBase tested = getTested();
		tested.myLogger = Loggers.getLogger("test logger");
		tested.httpclient = Mockito.mock(CloseableHttpClient.class);

		Mockito.when(
				tested.httpclient.execute(Mockito.any(HttpHost.class), Mockito.any(HttpGet.class),
						Mockito.any(BasicHttpContext.class))).thenAnswer(
				prepereHttpResponseAnswer(HttpStatus.SC_NOT_FOUND, null, null, null, false));

		try {
			tested.performHttpGetCall("http://test.org", null);
			Assert.fail("HttpCallException expected");
		} catch (HttpCallException e) {
			Assert.assertEquals(
					"Failed remote system HTTP GET request to the url 'http://test.org'. HTTP error code: 404 Response body: ",
					e.getMessage());
			Assert.assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
			Mockito.verify(tested.httpclient).execute(Mockito.any(HttpHost.class), Mockito.any(HttpGet.class),
					Mockito.any(BasicHttpContext.class));
			Mockito.verifyNoMoreInteractions(tested.httpclient);
		}
	}

	private Answer<HttpResponse> prepereHttpResponseAnswer(final int statusCode, final String responseContent,
			final String responseContentType, final Map<String, String> headersExpected, final boolean authExpected) {
		return new Answer<HttpResponse>() {

			@Override
			public HttpResponse answer(InvocationOnMock invocation) throws Throwable {

				HttpGet method = (HttpGet) invocation.getArguments()[1];

				if (headersExpected == null) {
					Assert.assertEquals(0, method.getAllHeaders().length);
				} else {
					for (String hn : headersExpected.keySet()) {
						Assert.assertEquals(headersExpected.get(hn), method.getFirstHeader(hn).getValue());
					}
				}

				HttpClientContext localcontext = (HttpClientContext) invocation.getArguments()[2];
				if (authExpected) {
					Assert.assertTrue(localcontext.getAuthCache() instanceof BasicAuthCache);
				} else {
					Assert.assertNull(localcontext.getAuthCache());
				}

				HttpResponse ret = Mockito.mock(CloseableHttpResponse.class);
				StatusLine sl = Mockito.mock(StatusLine.class);
				Mockito.when(sl.getStatusCode()).thenReturn(statusCode);
				Mockito.when(ret.getStatusLine()).thenReturn(sl);

				HttpEntity entity = null;
				if (responseContent != null)
					entity = new StringEntity(responseContent);
				Mockito.when(ret.getEntity()).thenReturn(entity);

				if (responseContentType != null) {
					Mockito.when(ret.getFirstHeader("Content-Type")).thenReturn(new Header() {

						@Override
						public String getValue() {
							return responseContentType;
						}

						@Override
						public String getName() {
							return "Content-Type";
						}

						@Override
						public HeaderElement[] getElements() throws ParseException {
							return null;
						}
					});
				}

				return ret;
			}

		};
	}

	private HttpRemoteSystemClientBase getTested() {
		HttpRemoteSystemClientBase ret = new HttpRemoteSystemClientBase() {

			@Override
			public void init(IESIntegration es, Map<String, Object> config, boolean spaceListLoadingEnabled,
					IPwdLoader pwdLoader) throws SettingsException {
			}

			@Override
			public List<String> getAllSpaces() throws Exception {
				return null;
			}

			@Override
			public ChangedDocumentsResults getChangedDocuments(String spaceKey, int startAt, boolean fullUpdate,
					Date updatedAfter) throws Exception {
				return null;
			}

			@Override
			public Object getChangedDocumentDetails(String spaceKey, String documentId, Map<String, Object> document)
					throws RemoteDocumentNotFoundException, Exception {
				return null;
			}

		};
		return ret;
	}

}

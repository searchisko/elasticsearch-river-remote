/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.jackson.core.JsonParseException;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.SettingsException;
import org.jboss.elasticsearch.river.remote.HttpRemoteSystemClientBase.HttpCallException;
import org.jboss.elasticsearch.river.remote.HttpRemoteSystemClientBase.HttpMethodType;
import org.jboss.elasticsearch.river.remote.exception.RemoteDocumentNotFoundException;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

/**
 * Unit test for {@link GetJSONClient}.
 *
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class GetJSONClientTest {

	public static final void main(String... args) throws Exception {
		GetJSONClient tested = new GetJSONClient();
		Map<String, Object> config = new HashMap<>();
		config.put(
				GetJSONClient.CFG_URL_GET_DOCUMENTS,
				"https://issues.jboss.org/rest/api/2/search?jql="
						+ URLEncoder.encode("project=ORG and updatedDate >= \"2014/09/15 01:00\"", "UTF-8"));
		config.put(GetJSONClient.CFG_USERNAME, "");
		config.put(GetJSONClient.CFG_PASSWORD, "");
		config.put(GetJSONClient.CFG_TIMEOUT, "50s");
		config.put(GetJSONClient.CFG_GET_DOCS_RES_FIELD_DOCUMENTS, "issues");
		config.put(GetJSONClient.CFG_GET_DOCS_RES_FIELD_TOTALCOUNT, "total");
		tested.init(mockEsIntegrationComponent(), config, false, null);

		ChangedDocumentsResults ret = tested.getChangedDocuments("ORG", 0, true, null);

		System.out.println("Documents count: " + ret.getDocumentsCount());

	}

	@Test
	public void init() {

		try {
			Map<String, Object> config = new HashMap<String, Object>();
			GetJSONClient tested = new GetJSONClient();
			tested.init(mockEsIntegrationComponent(), config, false, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "bad url format");
			GetJSONClient tested = new GetJSONClient();
			tested.init(mockEsIntegrationComponent(), config, false, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		// case - basic config, no getSpaces required, no authentication
		{
			IESIntegration esMock = mockEsIntegrationComponent();
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			tested.init(esMock, config, false, null);
			Assert.assertEquals("http://test.org/documents", tested.urlGetDocuments);
			Assert.assertNull(tested.urlGetSpaces);
			Assert.assertNull(tested.getSpacesResField);
			Assert.assertFalse(tested.isAuthConfigured);
			Assert.assertEquals(GetJSONClient.HEADER_ACCEPT_DEFAULT, tested.headers.get("Accept"));
			Mockito.verify(esMock).createLogger(GetJSONClient.class);
		}

		// case - error - getSpaces is required but not configured!
		try {
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			tested.init(mockEsIntegrationComponent(), config, true, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}
		try {
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_SPACES, "bad url format");
			tested.init(mockEsIntegrationComponent(), config, true, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		// case - basic config, getSpaces required, authentication with pwd in config, accept header changed
		{
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_SPACES, "http://test.org/spaces");
			config.put(GetJSONClient.CFG_GET_SPACES_RESPONSE_FIELD, "");
			config.put(GetJSONClient.CFG_USERNAME, "myuser");
			config.put(GetJSONClient.CFG_PASSWORD, "paaswd");
			config.put(GetJSONClient.CFG_HEADER_ACCEPT, "app/json");

			Map<String,Object> valueForRootResFields = new HashMap<String,Object>(1);
			valueForRootResFields.put("dev", "container_info.dev");
			config.put(GetJSONClient.CFG_GET_ROOT_RES_FIELDS_MAPPING, valueForRootResFields);

			IPwdLoader pwdLoaderMock = Mockito.mock(IPwdLoader.class);
			tested.init(mockEsIntegrationComponent(), config, true, pwdLoaderMock);
			Assert.assertEquals("http://test.org/documents", tested.urlGetDocuments);
			Assert.assertEquals("http://test.org/spaces", tested.urlGetSpaces);
			Assert.assertNull(tested.getSpacesResField);
			Assert.assertEquals("app/json", tested.headers.get("Accept"));
			Assert.assertTrue(tested.isAuthConfigured);
			Assert.assertNotNull(tested.getRootResFieldsMapping);
			Assert.assertEquals("container_info.dev", tested.getRootResFieldsMapping.get("dev"));
			Mockito.verifyZeroInteractions(pwdLoaderMock);
		}

		// case - basic config, getSpaces required, authentication with pwd from loader
		{
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_SPACES, "http://test.org/spaces");
			config.put(GetJSONClient.CFG_USERNAME, "myuser");
			IPwdLoader pwdLoaderMock = Mockito.mock(IPwdLoader.class);
			Mockito.when(pwdLoaderMock.loadPassword("myuser")).thenReturn("paaswd");
			tested.init(mockEsIntegrationComponent(), config, true, pwdLoaderMock);
			Assert.assertEquals("http://test.org/documents", tested.urlGetDocuments);
			Assert.assertEquals("http://test.org/spaces", tested.urlGetSpaces);
			Assert.assertNull(tested.getSpacesResField);
			Assert.assertEquals(GetJSONClient.HEADER_ACCEPT_DEFAULT, tested.headers.get("Accept"));
			Assert.assertTrue(tested.isAuthConfigured);
			Mockito.verify(pwdLoaderMock).loadPassword("myuser");
		}

		// case - basic config, authentication put pwd not defined
		{
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_SPACES, "http://test.org/spaces");
			config.put(GetJSONClient.CFG_USERNAME, "myuser");
			tested.init(mockEsIntegrationComponent(), config, true, null);
			Assert.assertEquals("http://test.org/documents", tested.urlGetDocuments);
			Assert.assertEquals("http://test.org/spaces", tested.urlGetSpaces);
			Assert.assertNull(tested.getSpacesResField);
			Assert.assertEquals(GetJSONClient.HEADER_ACCEPT_DEFAULT, tested.headers.get("Accept"));
			Assert.assertFalse(tested.isAuthConfigured);
		}

		// case - basic config, api key in password file
		{
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_SPACES, "http://test.org/spaces");
			config.put(GetJSONClient.CFG_EMBED_URL_API_KEY_USERNAME, "stackoverflow");
			IPwdLoader pwdLoaderMock = Mockito.mock(IPwdLoader.class);
			Mockito.when(pwdLoaderMock.loadPassword("stackoverflow")).thenReturn("paaswd");
			tested.init(mockEsIntegrationComponent(), config, true, pwdLoaderMock);
			Assert.assertEquals("http://test.org/documents", tested.urlGetDocuments);
			Assert.assertEquals("http://test.org/spaces", tested.urlGetSpaces);
			Assert.assertNull(tested.getSpacesResField);
			Assert.assertEquals(GetJSONClient.HEADER_ACCEPT_DEFAULT, tested.headers.get("Accept"));
			Mockito.verify(pwdLoaderMock).loadPassword("stackoverflow");
		}


		// case - error when both detail UIRLa nd detail field are provided
		try {
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS_FIELD, "detailfield");
			tested.init(mockEsIntegrationComponent(), config, false, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		// case - invalid numbers for updatedAfterInitialValue
		try {
            GetJSONClient tested = new GetJSONClient();
            Map<String, Object> config = new HashMap<String, Object>();
            config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
            config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS, "http://test.org/documents");
            config.put(GetJSONClient.CFG_UPDATED_AFTER_INITIAL_VALUE, "notAnumber");
            tested.init(mockEsIntegrationComponent(), config, false, null);
            Assert.fail("SettingsException not thrown");
        } catch (SettingsException e) {
            // OK
        }

		// case - invalid numbers for updatedBeforeTimeSpanFromUpdatedAfter
        try {
            GetJSONClient tested = new GetJSONClient();
            Map<String, Object> config = new HashMap<String, Object>();
            config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
            config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS, "http://test.org/documents");
            config.put(GetJSONClient.CFG_UPDATED_BEFORE_TIME_SPAN_FROM_UPDATED_AFTER, "notAnumber");
            tested.init(mockEsIntegrationComponent(), config, false, null);
            Assert.fail("SettingsException not thrown");
        } catch (SettingsException e) {
            // OK
        }

	}

	@Test
	public void getAllSpaces() throws Exception {

		// case - incorrect JSON response format
		try {
			IRemoteSystemClient tested = getAllSpaces_createTested("[ \"ORG\", \"PPP\"", null);
			tested.getAllSpaces();
			Assert.fail("JsonParseException expected");
		} catch (JsonParseException e) {
			// OK
		}

		// case - incorrect response content given to configured field
		try {
			IRemoteSystemClient tested = getAllSpaces_createTested("{ \"spaces\" : 10 }", "spaces");
			tested.getAllSpaces();
			Assert.fail("Exception expected");
		} catch (Exception e) {
			// OK
		}
		// case - error because field is required but response doesn't contains map
		try {
			IRemoteSystemClient tested = getAllSpaces_createTested("[ \"ORG\", \"PPP\", 10]", "spaces");
			tested.getAllSpaces();
			Assert.fail("Exception expected");
		} catch (Exception e) {
			// OK
		}

		// case - simple array in JSON response
		{
			IRemoteSystemClient tested = getAllSpaces_createTested("[ \"ORG\", \"PPP\", 10]", null);
			List<String> ret = tested.getAllSpaces();
			Assert.assertNotNull(ret);
			Assert.assertEquals(3, ret.size());
			Assert.assertTrue(ret.contains("ORG"));
			Assert.assertTrue(ret.contains("PPP"));
			Assert.assertTrue(ret.contains("10"));
		}

		// case - simple map in JSON response
		{
			IRemoteSystemClient tested = getAllSpaces_createTested("{ \"ORG\" : {}, \"PPP\" : {}}", null);
			List<String> ret = tested.getAllSpaces();
			Assert.assertNotNull(ret);
			Assert.assertEquals(2, ret.size());
			Assert.assertTrue(ret.contains("ORG"));
			Assert.assertTrue(ret.contains("PPP"));
		}

		// case - array in field of JSON response
		{
			IRemoteSystemClient tested = getAllSpaces_createTested("{ \"spaces\" : [ \"ORG\", \"PPP\", 10]}", "spaces");
			List<String> ret = tested.getAllSpaces();
			Assert.assertNotNull(ret);
			Assert.assertEquals(3, ret.size());
			Assert.assertTrue(ret.contains("ORG"));
			Assert.assertTrue(ret.contains("PPP"));
			Assert.assertTrue(ret.contains("10"));
		}

		// case - map in field of JSON response
		{
			IRemoteSystemClient tested = getAllSpaces_createTested("{ \"spaces\" : { \"ORG\" : {}, \"PPP\" : {}}}", "spaces");
			List<String> ret = tested.getAllSpaces();
			Assert.assertNotNull(ret);
			Assert.assertEquals(2, ret.size());
			Assert.assertTrue(ret.contains("ORG"));
			Assert.assertTrue(ret.contains("PPP"));
		}

		// case - evaluation of nested field of JSON response
		{
			IRemoteSystemClient tested = getAllSpaces_createTested(
					"{ \"ret\" : { \"spaces\" : { \"ORG\" : {}, \"PPP\" : {}}}}", "ret.spaces");
			List<String> ret = tested.getAllSpaces();
			Assert.assertNotNull(ret);
			Assert.assertEquals(2, ret.size());
			Assert.assertTrue(ret.contains("ORG"));
			Assert.assertTrue(ret.contains("PPP"));
		}
	}

	private IRemoteSystemClient getAllSpaces_createTested(final String returnJson, String configSpacesResponseField) {
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
		config.put(GetJSONClient.CFG_URL_GET_SPACES, "http://test.org/spaces");
		config.put(GetJSONClient.CFG_GET_SPACES_RESPONSE_FIELD, configSpacesResponseField);
		IRemoteSystemClient tested = new GetJSONClient() {
			@Override
			protected HttpResponseContent performHttpGetCall(String url, Map<String, String> headers) throws Exception,
					HttpCallException {
				Assert.assertEquals("http://test.org/spaces", url);
				return new HttpResponseContent("application/json", returnJson.getBytes("UTF-8"));
			};

		};
		tested.init(mockEsIntegrationComponent(), config, true, null);
		return tested;
	}

	@Test
	public void getChangedDocuments() throws Exception {

		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config
					.put(
							GetJSONClient.CFG_URL_GET_DOCUMENTS,
							"http://totallyrandomdomain.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}&it={indexingType}");
			IRemoteSystemClient tested = createTestedInstance(config, "invalid json",
					"http://totallyrandomdomain.org/documents?docSpace=myspace&docUpdatedAfter=&startAtIndex=0&it=full");
			tested.getChangedDocuments("myspace", 0, true, null);
			Assert.fail("JsonParseException expected");
		} catch (JsonParseException e) {
			// OK
		}

		// case - simple response with direct list, no total
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config
					.put(
							GetJSONClient.CFG_URL_GET_DOCUMENTS,
							"http://totallyrandomdomain.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}&it={indexingType}");
			IRemoteSystemClient tested = createTestedInstance(config, "[{\"key\" : \"a\"},{\"key\" : \"b\"}]",
					"http://totallyrandomdomain.org/documents?docSpace=myspace&docUpdatedAfter=1256&startAtIndex=12&it=inc");
			ChangedDocumentsResults ret = tested.getChangedDocuments("myspace", 12, false, new Date(1256l));
			Assert.assertEquals(2, ret.getDocumentsCount());
			Assert.assertEquals(12, ret.getStartAt());
			Assert.assertEquals(null, ret.getTotal());
			Assert.assertEquals("a", ret.getDocuments().get(0).get("key"));
			Assert.assertEquals("b", ret.getDocuments().get(1).get("key"));
		}

		// case - object response with documents and total in nested fields
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://totallyrandomdomain.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_GET_DOCS_RES_FIELD_DOCUMENTS, "response.items");
			config.put(GetJSONClient.CFG_GET_DOCS_RES_FIELD_TOTALCOUNT, "response.total");

			Map<String,Object> valueForRootResFields = new HashMap<String,Object>(1);
			valueForRootResFields.put("dev", "response.container_info.dev");
			config.put(GetJSONClient.CFG_GET_ROOT_RES_FIELDS_MAPPING, valueForRootResFields);

			IRemoteSystemClient tested = createTestedInstance(config,
					"{\"response\": { \"total\":20 , \"container_info\": { \"dev\":\"false\"},"
					+ "\"items\":[{\"key\" : \"a\"},{\"key\" : \"b\"}]}}",
					"http://totallyrandomdomain.org/documents?docSpace=myspace&docUpdatedAfter=1256&startAtIndex=12");
			ChangedDocumentsResults ret = tested.getChangedDocuments("myspace", 12, false, new Date(1256l));
			Assert.assertEquals(2, ret.getDocumentsCount());
			Assert.assertEquals(12, ret.getStartAt());
			Assert.assertEquals(new Integer(20), ret.getTotal());
			Assert.assertEquals("a", ret.getDocuments().get(0).get("key"));
			Assert.assertEquals("b", ret.getDocuments().get(1).get("key"));
			Assert.assertEquals("false", ret.getDocuments().get(0).get("dev"));
			Assert.assertEquals("false", ret.getDocuments().get(1).get("dev"));
		}

		// case - test that forced pause parameter is correctly parsed and processed
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://averynotexistinghostname.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}");
			config.put(GetJSONClient.CFG_FORCED_INDEXING_PAUSE_FIELD,"backoff");
			config.put(GetJSONClient.CFG_FORCED_INDEXING_PAUSE_FIELD_TIME_UNIT, "SECONDS");
			config.put(GetJSONClient.CFG_GET_DOCS_RES_FIELD_DOCUMENTS, "items");
			Long timeBeforeExecution = System.currentTimeMillis();
			IRemoteSystemClient tested = createTestedInstance(config, "{ \"backoff\": 2, \"items\":[] }",
					"http://averynotexistinghostname.org/documents?docSpace=space&docUpdatedAfter=1000000000000");
			tested.getChangedDocuments("space", 0, true, new Date(1000000000000L));
			tested.getChangedDocuments("space", 0, true, new Date(1000000000000L));
			Assert.assertTrue(System.currentTimeMillis() >= timeBeforeExecution+2000L);

			// now we'll see if delay is properly cleared if the time already passed during processing
			try {
	            Thread.sleep( 2000 );
	        } catch( InterruptedException e ) {
	            //ignore
	        }
			timeBeforeExecution = System.currentTimeMillis();
			tested.getChangedDocuments("space", 0, true, new Date(1000000000000L));
			Assert.assertTrue( System.currentTimeMillis()-timeBeforeExecution < 2000 );
		}

		// case - test that minimal request delay parameter is correctly parsed and processed
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://averynotexistinghostname.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}");
			config.put(GetJSONClient.CFG_MIN_GET_DOCUMENTS_DELAY,"2000");
			config.put(GetJSONClient.CFG_GET_DOCS_RES_FIELD_DOCUMENTS, "items");
			Long timeBeforeExecution = System.currentTimeMillis();
			IRemoteSystemClient tested = createTestedInstance(config, "{ \"items\":[] }",
					"http://averynotexistinghostname.org/documents?docSpace=space&docUpdatedAfter=1000000000000");
			tested.getChangedDocuments("space", 0, true, new Date(1000000000000L));
			Assert.assertTrue(System.currentTimeMillis() >= timeBeforeExecution+2000L);
		}
	}

	private IRemoteSystemClient createTestedInstance(Map<String, Object> config, final String returnJson,
			final String expectadCallUrl) {
		IRemoteSystemClient tested = new GetJSONClient() {
			@Override
			protected HttpResponseContent performHttpCall(String url, Map<String, String> headers, HttpMethodType methodType) throws Exception,
					HttpCallException {
				Assert.assertEquals(expectadCallUrl, url);
				return new HttpResponseContent("application/json", returnJson.getBytes("UTF-8"));
			};

		};
		tested.init(mockEsIntegrationComponent(), config, false, null);
		return tested;
	}

	private IRemoteSystemClient createTestedInstanceWithHttpCallException(Map<String, Object> config,
			final int returnHttpCode) {
		IRemoteSystemClient tested = new GetJSONClient() {
			@Override
			protected HttpResponseContent performHttpGetCall(String url, Map<String, String> headers) throws Exception,
					HttpCallException {
				throw new HttpCallException(url, returnHttpCode, "response content");
			};

		};
		tested.init(mockEsIntegrationComponent(), config, false, null);
		return tested;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getChangedDocumentDetails_urlConfigured() throws Exception {
		// case - test REST not called if URL is not configured
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			IRemoteSystemClient tested = createTestedInstance(config, "invalid json", "not called");
			Assert.assertNull(tested.getChangedDocumentDetails("myspace", "myid", null));
		}

		// case - test bad JSON returned
		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS, "http://test.org/document?docSpace={space}&id={id}");
			IRemoteSystemClient tested = createTestedInstance(config, "invalid json",
					"http://test.org/document?docSpace=myspace&id=myid");
			tested.getChangedDocumentDetails("myspace", "myid", null);
			Assert.fail("JsonParseException expected");
		} catch (JsonParseException e) {
			// OK
		}

		// case - test JSON object returned
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS, "http://test.org/document?docSpace={space}&id={id}");
			IRemoteSystemClient tested = createTestedInstance(config, "{\"item1\":\"val1\"}",
					"http://test.org/document?docSpace=myspace&id=myid");
			Object ret = tested.getChangedDocumentDetails("myspace", "myid", null);
			Assert.assertNotNull(ret);
			Assert.assertTrue(ret instanceof Map);
			Map<String, Object> rm = (Map<String, Object>) ret;
			Assert.assertEquals(1, rm.size());
			Assert.assertEquals("val1", rm.get("item1"));
		}
		// case - test JSON array returned
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS, "http://test.org/document?docSpace={space}&id={id}");
			IRemoteSystemClient tested = createTestedInstance(config, "[{\"item1\":\"val1\"}]",
					"http://test.org/document?docSpace=myspace&id=myid");
			Object ret = tested.getChangedDocumentDetails("myspace", "myid", null);
			Assert.assertNotNull(ret);
			Assert.assertTrue(ret instanceof List);
			List<Map<String, Object>> rm = (List<Map<String, Object>>) ret;
			Assert.assertEquals("val1", rm.get(0).get("item1"));
		}

		// case - HTTP code 404 must throw special exception not to fail indexing completely- issue #11
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS, "http://test.org/document?docSpace={space}&id={id}");
			IRemoteSystemClient tested = createTestedInstanceWithHttpCallException(config, HttpStatus.SC_NOT_FOUND);
			try {
				tested.getChangedDocumentDetails("myspace", "myid", null);
				Assert.fail("RemoteDocumentNotFoundException expected");
			} catch (RemoteDocumentNotFoundException e) {
				// OK
			}
		}

		// case - other HTTP codes must throw RestCallHttpException to fail indexing completely
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS, "http://test.org/document?docSpace={space}&id={id}");
			IRemoteSystemClient tested = createTestedInstanceWithHttpCallException(config, HttpStatus.SC_FORBIDDEN);
			try {
				tested.getChangedDocumentDetails("myspace", "myid", null);
				Assert.fail("RestCallHttpException expected");
			} catch (HttpCallException e) {
				// OK
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getChangedDocumentDetails_urlFieldConfigured() throws Exception {

		// case - no item data available
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS_FIELD, "detailUrlField");
			IRemoteSystemClient tested = createTestedInstance(config, "invalid json", "http://test.org/document/5656525");
			Assert.assertNull(tested.getChangedDocumentDetails("myspace", "myid", null));
		}

		// case - no URL available in item data
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS_FIELD, "detailUrlField");
			IRemoteSystemClient tested = createTestedInstance(config, "invalid json", "http://test.org/document/5656525");
			Map<String, Object> itemData = new HashMap<>();
			Assert.assertNull(tested.getChangedDocumentDetails("myspace", "myid", itemData));
		}

		// case - invalid object available in item data instead of String with URL
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS_FIELD, "detailUrlField");
			IRemoteSystemClient tested = createTestedInstance(config, "invalid json", "http://test.org/document/5656525");
			Map<String, Object> itemData = new HashMap<>();
			itemData.put("detailUrlField", new HashMap<>());
			Assert.assertNull(tested.getChangedDocumentDetails("myspace", "myid", itemData));
		}

		// case - invalid URL format in item data
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS_FIELD, "detailUrlField");
			IRemoteSystemClient tested = createTestedInstance(config, "invalid json", "http://test.org/document/5656525");
			Map<String, Object> itemData = new HashMap<>();
			itemData.put("detailUrlField", "invalid url format");
			Assert.assertNull(tested.getChangedDocumentDetails("myspace", "myid", itemData));
		}

		// case - test bad JSON returned
		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS_FIELD, "detailUrlField");
			IRemoteSystemClient tested = createTestedInstance(config, "invalid json", "http://test.org/document/5656525");
			Map<String, Object> itemData = new HashMap<>();
			itemData.put("detailUrlField", "http://test.org/document/5656525");
			tested.getChangedDocumentDetails("myspace", "myid", itemData);
			Assert.fail("JsonParseException expected");
		} catch (JsonParseException e) {
			// OK
		}

		// case - test JSON object returned, dot notation for field configuration
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS_FIELD, "nested.detailUrlField");
			IRemoteSystemClient tested = createTestedInstance(config, "{\"item1\":\"val1\"}",
					"http://test.org/document/5656525");
			Map<String, Object> itemData = new HashMap<>();
			itemData.put("detailUrlField", "http://test.org/document/5656525");
			Map<String, Object> itemData2 = new HashMap<>();
			itemData2.put("nested", itemData);
			Object ret = tested.getChangedDocumentDetails("myspace", "myid", itemData2);
			Assert.assertNotNull(ret);
			Assert.assertTrue(ret instanceof Map);
			Map<String, Object> rm = (Map<String, Object>) ret;
			Assert.assertEquals(1, rm.size());
			Assert.assertEquals("val1", rm.get("item1"));
		}

		// case - HTTP code 404 must throw special exception not to fail indexing completely- issue #11
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS_FIELD, "detailUrlField");
			IRemoteSystemClient tested = createTestedInstanceWithHttpCallException(config, HttpStatus.SC_NOT_FOUND);
			try {
				Map<String, Object> itemData = new HashMap<>();
				itemData.put("detailUrlField", "http://test.org/document/5656525");
				tested.getChangedDocumentDetails("myspace", "myid", itemData);
				Assert.fail("RemoteDocumentNotFoundException expected");
			} catch (RemoteDocumentNotFoundException e) {
				// OK
			}
		}

		// case - other HTTP codes must throw RestCallHttpException to fail indexing completely
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS_FIELD, "detailUrlField");
			IRemoteSystemClient tested = createTestedInstanceWithHttpCallException(config, HttpStatus.SC_FORBIDDEN);
			try {
				Map<String, Object> itemData = new HashMap<>();
				itemData.put("detailUrlField", "http://test.org/document/5656525");
				tested.getChangedDocumentDetails("myspace", "myid", itemData);
				Assert.fail("RestCallHttpException expected");
			} catch (HttpCallException e) {
				// OK
			}
		}

	}

	@Test
	public void enhanceUrlGetDocumentDetails() throws UnsupportedEncodingException {

		Assert.assertEquals("http://test.org?docSpace=myspace&id=myid",
				GetJSONClient.enhanceUrlGetDocumentDetails("http://test.org?docSpace={space}&id={id}", "myspace", "myid"));

		// param URL encoding test
		Assert.assertEquals("http://test.org?docSpace=myspac%26e&id=my%26id",
				GetJSONClient.enhanceUrlGetDocumentDetails("http://test.org?docSpace={space}&id={id}", "myspac&e", "my&id"));
	}

	@Test
	public void enhanceUrlGetDocuments() throws UnsupportedEncodingException {
		Assert
				.assertEquals(
						"http://test.org?docSpace=myspace&docUpdatedAfter=123456&docUpdatedBefore=123756&startAtIndex=0&it=full",
						GetJSONClient
								.enhanceUrlGetDocuments(
										"http://test.org?docSpace={space}&docUpdatedAfter={updatedAfter}&docUpdatedBefore={updatedBefore}&startAtIndex={startAtIndex}&it={indexingType}",
										"myspace", new Date(123456l), DateTimeUtils.CUSTOM_MILLISEC_EPOCH_DATETIME_FORMAT, null, 300L, 0, true, null));

		// Testing if not providing updatedAfter date format doesn't break the call and uses the correct milisecond-based format as the default.
		Assert
				.assertEquals(
						"http://test.org?docSpace=myspace&docUpdatedAfter=123456&startAtIndex=0&it=full",
						GetJSONClient
								.enhanceUrlGetDocuments(
										"http://test.org?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}&it={indexingType}",
										"myspace", new Date(123456l), null, null, null, 0, true, null));

		Assert
				.assertEquals(
						"http://test.org?docSpace=myspace&docUpdatedAfter=20150404%26Fun&startAtIndex=0&it=full",
						GetJSONClient
								.enhanceUrlGetDocuments(
										"http://test.org?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}&it={indexingType}",
										"myspace", new Date(1428113546000L), "yyyyMMdd'&Fun'", null, null, 0, true, null));

		Assert
				.assertEquals(
						"http://test.org?docSpace=my%26space&docUpdatedAfter=&startAtIndex=125&it=inc",
						GetJSONClient
								.enhanceUrlGetDocuments(
										"http://test.org?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}&it={indexingType}",
										"my&space", null, null, null, null, 125, false, null));

		Assert
				.assertEquals(
						"http://test.org?docSpace=my%26space&docUpdatedAfter=1349108160000&startAtIndex=125&it=inc",
						GetJSONClient
								.enhanceUrlGetDocuments(
										"http://test.org?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}&it={indexingType}",
										"my&space", null, null, 1349108160000L, null, 125, false, null));

		Assert
				.assertEquals(
						"http://test.org?docSpace=my%26space&docUpdatedAfter=1349108160000&startAtIndex=125&it=inc&apikey=password",
						GetJSONClient
								.enhanceUrlGetDocuments(
										"http://test.org?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}&it={indexingType}&apikey={apiKey}",
										"my&space", null, null, 1349108160000L, null, 125, false, "password"));

	}

	protected static IESIntegration mockEsIntegrationComponent() {
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		Mockito.when(esIntegrationMock.createLogger(Mockito.any(Class.class))).thenReturn(
				ESLoggerFactory.getLogger(GetJSONClient.class.getName()));
		return esIntegrationMock;
	}

}

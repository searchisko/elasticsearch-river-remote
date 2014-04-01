/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.jackson.core.JsonParseException;
import org.elasticsearch.common.settings.SettingsException;
import org.jboss.elasticsearch.river.remote.GetJSONClient.RestCallHttpException;
import org.jboss.elasticsearch.river.remote.exception.RemoteDocumentNotFoundException;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link GetJSONClient}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class GetJSONClientTest {

	@Test
	public void init() {

		try {
			Map<String, Object> config = new HashMap<String, Object>();
			GetJSONClient tested = new GetJSONClient();
			tested.init(config, false, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "bad url format");
			GetJSONClient tested = new GetJSONClient();
			tested.init(config, false, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		// case - basic config, no getSpaces required, no authentication
		{
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			tested.init(config, false, null);
			Assert.assertEquals("http://test.org/documents", tested.urlGetDocuments);
			Assert.assertNull(tested.urlGetSpaces);
			Assert.assertNull(tested.getSpacesResField);
			Assert.assertFalse(tested.isAuthConfigured);
			Assert.assertEquals(GetJSONClient.HEADER_ACCEPT_DEFAULT, tested.headerAccept);
		}

		// case - error - getSpaces is required but not configured!
		try {
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			tested.init(config, true, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}
		try {
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_SPACES, "bad url format");
			tested.init(config, true, null);
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
			IPwdLoader pwdLoaderMock = Mockito.mock(IPwdLoader.class);
			tested.init(config, true, pwdLoaderMock);
			Assert.assertEquals("http://test.org/documents", tested.urlGetDocuments);
			Assert.assertEquals("http://test.org/spaces", tested.urlGetSpaces);
			Assert.assertNull(tested.getSpacesResField);
			Assert.assertEquals("app/json", tested.headerAccept);
			Assert.assertTrue(tested.isAuthConfigured);
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
			tested.init(config, true, pwdLoaderMock);
			Assert.assertEquals("http://test.org/documents", tested.urlGetDocuments);
			Assert.assertEquals("http://test.org/spaces", tested.urlGetSpaces);
			Assert.assertNull(tested.getSpacesResField);
			Assert.assertEquals(GetJSONClient.HEADER_ACCEPT_DEFAULT, tested.headerAccept);
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
			tested.init(config, true, null);
			Assert.assertEquals("http://test.org/documents", tested.urlGetDocuments);
			Assert.assertEquals("http://test.org/spaces", tested.urlGetSpaces);
			Assert.assertNull(tested.getSpacesResField);
			Assert.assertEquals(GetJSONClient.HEADER_ACCEPT_DEFAULT, tested.headerAccept);
			Assert.assertFalse(tested.isAuthConfigured);
		}

		// case - error when both detail UIRLa nd detail field are provided
		try {
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENT_DETAILS_FIELD, "detailfield");
			tested.init(config, false, null);
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
			protected byte[] performGetRESTCall(String url) throws Exception {
				Assert.assertEquals("http://test.org/spaces", url);
				return returnJson.getBytes("UTF-8");
			};

		};
		tested.init(config, true, null);
		return tested;
	}

	@Test
	public void getChangedDocuments() throws Exception {

		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			IRemoteSystemClient tested = createTestedInstance(config, "invalid json",
					"http://test.org/documents?docSpace=myspace&docUpdatedAfter=&startAtIndex=0");
			tested.getChangedDocuments("myspace", 0, null);
			Assert.fail("JsonParseException expected");
		} catch (JsonParseException e) {
			// OK
		}

		// case - simple response with direct list, no total
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			IRemoteSystemClient tested = createTestedInstance(config, "[{\"key\" : \"a\"},{\"key\" : \"b\"}]",
					"http://test.org/documents?docSpace=myspace&docUpdatedAfter=1256&startAtIndex=12");
			ChangedDocumentsResults ret = tested.getChangedDocuments("myspace", 12, new Date(1256l));
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
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			config.put(GetJSONClient.CFG_GET_DOCS_RES_FIELD_DOCUMENTS, "response.items");
			config.put(GetJSONClient.CFG_GET_DOCS_RES_FIELD_TOTALCOUNT, "response.total");
			IRemoteSystemClient tested = createTestedInstance(config,
					"{\"response\": { \"total\":20 ,\"items\":[{\"key\" : \"a\"},{\"key\" : \"b\"}]}}",
					"http://test.org/documents?docSpace=myspace&docUpdatedAfter=1256&startAtIndex=12");
			ChangedDocumentsResults ret = tested.getChangedDocuments("myspace", 12, new Date(1256l));
			Assert.assertEquals(2, ret.getDocumentsCount());
			Assert.assertEquals(12, ret.getStartAt());
			Assert.assertEquals(new Integer(20), ret.getTotal());
			Assert.assertEquals("a", ret.getDocuments().get(0).get("key"));
			Assert.assertEquals("b", ret.getDocuments().get(1).get("key"));
		}

	}

	private IRemoteSystemClient createTestedInstance(Map<String, Object> config, final String returnJson,
			final String expectadCallUrl) {
		IRemoteSystemClient tested = new GetJSONClient() {
			@Override
			protected byte[] performGetRESTCall(String url) throws Exception {
				Assert.assertEquals(expectadCallUrl, url);
				return returnJson.getBytes("UTF-8");
			};

		};
		tested.init(config, false, null);
		return tested;
	}

	private IRemoteSystemClient createTestedInstanceWithRestCallHttpException(Map<String, Object> config,
			final int returnHttpCode) {
		IRemoteSystemClient tested = new GetJSONClient() {
			@Override
			protected byte[] performGetRESTCall(String url) throws Exception {
				throw new RestCallHttpException(url, returnHttpCode, "response content");
			};

		};
		tested.init(config, false, null);
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
			IRemoteSystemClient tested = createTestedInstanceWithRestCallHttpException(config, HttpStatus.SC_NOT_FOUND);
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
			IRemoteSystemClient tested = createTestedInstanceWithRestCallHttpException(config, HttpStatus.SC_FORBIDDEN);
			try {
				tested.getChangedDocumentDetails("myspace", "myid", null);
				Assert.fail("RestCallHttpException expected");
			} catch (RestCallHttpException e) {
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
			IRemoteSystemClient tested = createTestedInstanceWithRestCallHttpException(config, HttpStatus.SC_NOT_FOUND);
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
			IRemoteSystemClient tested = createTestedInstanceWithRestCallHttpException(config, HttpStatus.SC_FORBIDDEN);
			try {
				Map<String, Object> itemData = new HashMap<>();
				itemData.put("detailUrlField", "http://test.org/document/5656525");
				tested.getChangedDocumentDetails("myspace", "myid", itemData);
				Assert.fail("RestCallHttpException expected");
			} catch (RestCallHttpException e) {
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
		Assert.assertEquals("http://test.org?docSpace=myspace&docUpdatedAfter=123456&startAtIndex=0", GetJSONClient
				.enhanceUrlGetDocuments(
						"http://test.org?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}", "myspace",
						new Date(123456l), 0));

		Assert.assertEquals("http://test.org?docSpace=my%26space&docUpdatedAfter=&startAtIndex=125", GetJSONClient
				.enhanceUrlGetDocuments(
						"http://test.org?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}", "my&space",
						null, 125));

	}

}

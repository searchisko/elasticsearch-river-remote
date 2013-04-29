/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.common.jackson.core.JsonParseException;
import org.elasticsearch.common.settings.SettingsException;
import org.junit.Test;

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
			tested.init(config, false);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "bad url format");
			GetJSONClient tested = new GetJSONClient();
			tested.init(config, false);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		// case - basic config, no getSpaces required, no authentication
		{
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_SPACES, "http://test.org/spaces");
			config.put(GetJSONClient.CFG_GET_SPACES_RESPONSE_FIELD, "field");
			tested.init(config, false);
			Assert.assertEquals("http://test.org/documents", tested.urlGetDocuments);
			Assert.assertNull(tested.urlGetSpaces);
			Assert.assertNull(tested.getSpacesResField);
			Assert.assertFalse(tested.isAuthConfigured);
		}

		// case - error - getSpaces is required but not configured!
		try {
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			tested.init(config, true);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}
		try {
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_SPACES, "bad url format");
			tested.init(config, true);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		// case - basic config, getSpaces required, authentication
		{
			GetJSONClient tested = new GetJSONClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, "http://test.org/documents");
			config.put(GetJSONClient.CFG_URL_GET_SPACES, "http://test.org/spaces");
			config.put(GetJSONClient.CFG_GET_SPACES_RESPONSE_FIELD, "");
			config.put(GetJSONClient.CFG_USERNAME, "myuser");
			config.put(GetJSONClient.CFG_PASSWORD, "paaswd");
			tested.init(config, true);
			Assert.assertEquals("http://test.org/documents", tested.urlGetDocuments);
			Assert.assertEquals("http://test.org/spaces", tested.urlGetSpaces);
			Assert.assertNull(tested.getSpacesResField);
			Assert.assertTrue(tested.isAuthConfigured);
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
		tested.init(config, true);
		return tested;
	}

	@Test
	public void getChangedDocuments() throws Exception {

		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetJSONClient.CFG_URL_GET_DOCUMENTS,
					"http://test.org/documents?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}");
			IRemoteSystemClient tested = getChangedDocuments_createTested(config, "invalid json",
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
			IRemoteSystemClient tested = getChangedDocuments_createTested(config, "[{\"key\" : \"a\"},{\"key\" : \"b\"}]",
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
			IRemoteSystemClient tested = getChangedDocuments_createTested(config,
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

	private IRemoteSystemClient getChangedDocuments_createTested(Map<String, Object> config, final String returnJson,
			final String expectadCallUrl) {
		IRemoteSystemClient tested = new GetJSONClient() {
			@Override
			protected byte[] performGetRESTCall(String url) throws Exception {
				Assert.assertEquals(expectadCallUrl, url);
				return returnJson.getBytes("UTF-8");
			};

		};
		tested.init(config, false);
		return tested;
	}

	@Test
	public void enhanceUrlGetDocuments() {
		Assert.assertEquals("http://test.org?docSpace=myspace&docUpdatedAfter=123456&startAtIndex=0", GetJSONClient
				.enhanceUrlGetDocuments(
						"http://test.org?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}", "myspace",
						new Date(123456l), 0));

		Assert.assertEquals("http://test.org?docSpace=myspace&docUpdatedAfter=&startAtIndex=125", GetJSONClient
				.enhanceUrlGetDocuments(
						"http://test.org?docSpace={space}&docUpdatedAfter={updatedAfter}&startAtIndex={startAtIndex}", "myspace",
						null, 125));

	}

}

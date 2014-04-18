/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.SettingsException;
import org.jboss.elasticsearch.river.remote.HttpRemoteSystemClientBase.HttpCallException;
import org.jboss.elasticsearch.river.remote.sitemap.SiteMapParserTest;
import org.jboss.elasticsearch.river.remote.sitemap.UnknownFormatException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link GetSitemapHtmlClient}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class GetSitemapHtmlClientTest {

	@Test
	public void init() {

		try {
			Map<String, Object> config = new HashMap<String, Object>();
			GetSitemapHtmlClient tested = new GetSitemapHtmlClient();
			tested.init(config, false, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "bad url format");
			GetSitemapHtmlClient tested = new GetSitemapHtmlClient();
			tested.init(config, false, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		// case - basic config, no authentication
		{
			GetSitemapHtmlClient tested = new GetSitemapHtmlClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/documents");
			tested.init(config, false, null);
			Assert.assertEquals("http://test.org/documents", tested.urlGetSitemap);
			Assert.assertFalse(tested.isAuthConfigured);
		}

		// case - error - bad sitemap url
		try {
			GetSitemapHtmlClient tested = new GetSitemapHtmlClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "aaa");
			tested.init(config, false, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}
		try {
			GetSitemapHtmlClient tested = new GetSitemapHtmlClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "");
			tested.init(config, false, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		// case - basic config, authentication with pwd in config
		{
			GetSitemapHtmlClient tested = new GetSitemapHtmlClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/documents");
			config.put(GetSitemapHtmlClient.CFG_USERNAME, "myuser");
			config.put(GetSitemapHtmlClient.CFG_PASSWORD, "paaswd");
			IPwdLoader pwdLoaderMock = Mockito.mock(IPwdLoader.class);
			tested.init(config, false, pwdLoaderMock);
			Assert.assertEquals("http://test.org/documents", tested.urlGetSitemap);
			Assert.assertTrue(tested.isAuthConfigured);
			Mockito.verifyZeroInteractions(pwdLoaderMock);
		}

		// case - basic config, authentication with pwd from loader
		{
			GetSitemapHtmlClient tested = new GetSitemapHtmlClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/documents");
			config.put(GetSitemapHtmlClient.CFG_USERNAME, "myuser");
			IPwdLoader pwdLoaderMock = Mockito.mock(IPwdLoader.class);
			Mockito.when(pwdLoaderMock.loadPassword("myuser")).thenReturn("paaswd");
			tested.init(config, false, pwdLoaderMock);
			Assert.assertEquals("http://test.org/documents", tested.urlGetSitemap);
			Assert.assertTrue(tested.isAuthConfigured);
			Mockito.verify(pwdLoaderMock).loadPassword("myuser");
		}

		// case - basic config, authentication put pwd not defined
		{
			GetSitemapHtmlClient tested = new GetSitemapHtmlClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/documents");
			config.put(GetSitemapHtmlClient.CFG_USERNAME, "myuser");
			tested.init(config, false, null);
			Assert.assertEquals("http://test.org/documents", tested.urlGetSitemap);
			Assert.assertFalse(tested.isAuthConfigured);
		}

		// case - dynamic spaces loading unsupported
		try {
			GetSitemapHtmlClient tested = new GetSitemapHtmlClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/documents");
			tested.init(config, true, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			Assert
					.assertEquals(
							"Dynamic Spaces obtaining is not supported, use 'remote/spacesIndexed' to configure one space or static list",
							e.getMessage());
		}

	}

	@Test(expected = UnsupportedOperationException.class)
	public void getAllSpaces() throws Exception {
		GetSitemapHtmlClient tested = new GetSitemapHtmlClient();
		tested.getAllSpaces();
	}

	@Test
	public void createIdFromUrl() {
		Assert.assertNull(GetSitemapHtmlClient.createIdFromUrl(null));
		Assert.assertEquals("", GetSitemapHtmlClient.createIdFromUrl(""));
		Assert.assertEquals("http_www_test_org", GetSitemapHtmlClient.createIdFromUrl("http://www.test.org"));
		Assert.assertEquals("https_www_test_org_test_", GetSitemapHtmlClient.createIdFromUrl("https://www.test.org/test/"));
		Assert.assertEquals("https_www_test_org_8080_test_html",
				GetSitemapHtmlClient.createIdFromUrl("https://www.test.org:8080/test.html"));
		Assert.assertEquals("https_www_test_org_8080_test_html_aa_gg_jj_mm",
				GetSitemapHtmlClient.createIdFromUrl("https://www.test.org:8080/test.html?aa=gg&jj=mm"));
	}

	@Test
	public void getChangedDocuments() throws Exception {

		// case - sitemap xml format error
		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
			IRemoteSystemClient tested = createTestedInstance(config, "invalid json", "http://test.org/sitemap.xml");
			tested.getChangedDocuments("myspace", 0, null);
			Assert.fail("UnknownFormatException expected");
		} catch (UnknownFormatException e) {
			// OK
		}

		// case - http error
		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
			IRemoteSystemClient tested = createTestedInstanceWithHttpCallException(config, HttpStatus.SC_NOT_FOUND);
			tested.getChangedDocuments("myspace", 0, null);
			Assert.fail("HttpCallException expected");
		} catch (HttpCallException e) {
			// OK
		}

		// case - sitemap correct
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, SiteMapParserTest.URL_SITEMAP_XML);
			GetSitemapHtmlClient tested = createTestedInstance(config, SiteMapParserTest.SITEMAP_XML_NO_DECLARATIONS,
					SiteMapParserTest.URL_SITEMAP_XML);
			ChangedDocumentsResults chr = tested.getChangedDocuments("myspace", 0, null);
			Assert.assertEquals(5, chr.getDocumentsCount());
			Assert.assertEquals(new Integer(5), chr.getTotal());
			Assert.assertEquals(0, chr.getStartAt());
			assertDoc(chr.getDocuments().get(0), "http://www.example.com/", "2005-01-01T00:00:00.0+0000", 0.8);
			assertDoc(chr.getDocuments().get(1), "http://www.example.com/catalog?item=12&desc=vacation_hawaii", null, 0.5);
			assertDoc(chr.getDocuments().get(2), "http://www.example.com/catalog?item=73&desc=vacation_new_zealand",
					"2004-12-23T00:00:00.0+0000", 0.5);
			assertDoc(chr.getDocuments().get(3), "http://www.example.com/catalog?item=74&desc=vacation_newfoundland",
					"2004-12-23T18:00:15.0+0000", 0.3);
			assertDoc(chr.getDocuments().get(4), "http://www.example.com/catalog?item=83&desc=vacation_usa",
					"2004-11-23T00:00:00.0+0000", 0.5);
		}

		// case - sitemap with incorrect url's (not from base)
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
			GetSitemapHtmlClient tested = createTestedInstance(config, SiteMapParserTest.SITEMAP_XML_NO_DECLARATIONS,
					"http://test.org/sitemap.xml");
			ChangedDocumentsResults chr = tested.getChangedDocuments("myspace", 0, null);
			Assert.assertEquals(0, chr.getDocumentsCount());
			Assert.assertEquals(new Integer(0), chr.getTotal());
			Assert.assertEquals(0, chr.getStartAt());
		}

		// case - sitemap is index so we can't process it
		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, SiteMapParserTest.URL_SITEMAP_XML);
			IRemoteSystemClient tested = createTestedInstance(config, SiteMapParserTest.SITEMAP_XML_INDEX,
					SiteMapParserTest.URL_SITEMAP_XML);
			tested.getChangedDocuments("myspace", 0, null);
			Assert.fail("Exception expected");
		} catch (Exception e) {
			Assert.assertEquals("Sitemap index format is not supported by this river!", e.getMessage());
		}

	}

	private void assertDoc(Map<String, Object> map, String expectedUrl, String expectedDateLastModified,
			Double expectedPriority) {
		Assert.assertEquals(expectedUrl, map.get(GetSitemapHtmlClient.DOC_FIELD_URL));
		Assert.assertEquals(GetSitemapHtmlClient.createIdFromUrl(expectedUrl), map.get(GetSitemapHtmlClient.DOC_FIELD_ID));
		Assert.assertEquals(expectedDateLastModified, map.get(GetSitemapHtmlClient.DOC_FIELD_LAST_MODIFIED));
		Assert.assertEquals(expectedPriority, map.get(GetSitemapHtmlClient.DOC_FIELD_PRIORITY));
	}

	@Test
	public void getChangedDocumentDetails() {
		// TODO Unit test
	}

	private GetSitemapHtmlClient createTestedInstance(Map<String, Object> config, final String returnSitemapData,
			final String expectadCallUrl) {
		GetSitemapHtmlClient tested = new GetSitemapHtmlClient() {
			@Override
			protected byte[] performHttpGetCall(String url, Map<String, String> headers) throws Exception, HttpCallException {
				Assert.assertEquals(expectadCallUrl, url);
				return returnSitemapData.getBytes("UTF-8");
			};

		};
		tested.init(config, false, null);
		return tested;
	}

	private GetSitemapHtmlClient createTestedInstanceWithHttpCallException(Map<String, Object> config,
			final int returnHttpCode) {
		GetSitemapHtmlClient tested = new GetSitemapHtmlClient() {
			@Override
			protected byte[] performHttpGetCall(String url, Map<String, String> headers) throws Exception, HttpCallException {
				throw new HttpCallException(url, returnHttpCode, "response content");
			};

		};
		tested.init(config, false, null);
		return tested;
	}

}

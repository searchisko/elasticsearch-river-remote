/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.elasticsearch.common.settings.SettingsException;
import org.jboss.elasticsearch.river.remote.HttpRemoteSystemClientBase.HttpCallException;
import org.jboss.elasticsearch.river.remote.exception.RemoteDocumentNotFoundException;
import org.jboss.elasticsearch.river.remote.sitemap.SiteMapParserTest;
import org.jboss.elasticsearch.river.remote.sitemap.UnknownFormatException;
import org.jsoup.Jsoup;
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

		// case - bad html mapping configuration
		try {
			GetSitemapHtmlClient tested = new GetSitemapHtmlClient();
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/documents");
			config.put(GetSitemapHtmlClient.CFG_HTML_MAPPING, "no map");
			tested.init(config, false, null);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			Assert.assertEquals("'remote/htmlMapping' configuration section is invalid", e.getMessage());
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

	private static final String CT_XML = "text/xml";
	private static final String CT_HTML = "text/html";

	public static final String SITEMAP_XML_NO_DECLARATIONS_IGNORED_EXTENSIONS = "<urlset>" + "  <url>"
			+ "<loc>http://www.example.com/</loc>" + "<lastmod>2005-01-01</lastmod>" + "<changefreq>monthly</changefreq>"
			+ "<priority>0.8</priority>" + "</url>" + "<url>" + "<loc>http://www.example.com/catalog.zip</loc>"
			+ "<changefreq>weekly</changefreq>" + "</url>" + "<url>" + "<loc>http://www.example.com/catalog.jpeg</loc>"
			+ "<lastmod>2004-12-23</lastmod>" + "<changefreq>weekly</changefreq>" + "</url>" + "<url>"
			+ "<loc>http://www.example.com/catalog.html</loc>" + "<lastmod>2004-12-23T18:00:15+00:00</lastmod>"
			+ "<priority>0.3</priority>" + "</url>" + "<url>" + "<loc>http://www.example.com/catalog.htm</loc>"
			+ "<lastmod>2004-11-23</lastmod>" + "</url>" + "</urlset>";

	@Test
	public void getChangedDocuments() throws Exception {

		// case - sitemap xml format error
		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
			GetSitemapHtmlClient tested = createTestedInstance(config, "invalid sitemap xml", CT_XML,
					"http://test.org/sitemap.xml");
			tested.getChangedDocuments("myspace", 0, null);
			Assert.fail("UnknownFormatException expected");
		} catch (UnknownFormatException e) {
			// OK
		}

		// case - http error
		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
			GetSitemapHtmlClient tested = createTestedInstanceWithHttpCallException(config, new HttpCallException(
					"http://test.org/sitemap.xml", HttpStatus.SC_NOT_FOUND, "response content"));
			tested.getChangedDocuments("myspace", 0, null);
			Assert.fail("HttpCallException expected");
		} catch (HttpCallException e) {
			// OK
		}

		// case - sitemap correct
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, SiteMapParserTest.URL_SITEMAP_XML);
			GetSitemapHtmlClient tested = createTestedInstance(config, SiteMapParserTest.SITEMAP_XML_NO_DECLARATIONS, CT_XML,
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

		// case - sitemap correct with some ignored extensions
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, SiteMapParserTest.URL_SITEMAP_XML);
			GetSitemapHtmlClient tested = createTestedInstance(config, SITEMAP_XML_NO_DECLARATIONS_IGNORED_EXTENSIONS,
					CT_XML, SiteMapParserTest.URL_SITEMAP_XML);
			ChangedDocumentsResults chr = tested.getChangedDocuments("myspace", 0, null);
			Assert.assertEquals(3, chr.getDocumentsCount());
			Assert.assertEquals(new Integer(3), chr.getTotal());
			Assert.assertEquals(0, chr.getStartAt());
			assertDoc(chr.getDocuments().get(0), "http://www.example.com/", "2005-01-01T00:00:00.0+0000", 0.8);
			assertDoc(chr.getDocuments().get(1), "http://www.example.com/catalog.html", "2004-12-23T18:00:15.0+0000", 0.3);
			assertDoc(chr.getDocuments().get(2), "http://www.example.com/catalog.htm", "2004-11-23T00:00:00.0+0000", 0.5);
		}

		// case - sitemap with incorrect url's (not from base)
		{
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
			GetSitemapHtmlClient tested = createTestedInstance(config, SiteMapParserTest.SITEMAP_XML_NO_DECLARATIONS, CT_XML,
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
			GetSitemapHtmlClient tested = createTestedInstance(config, SiteMapParserTest.SITEMAP_XML_INDEX, CT_XML,
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
	public void getChangedDocumentDetails_httpError() throws Exception {
		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
			GetSitemapHtmlClient tested = createTestedInstanceWithHttpCallException(config, new HttpCallException(
					"http://test.org/sitemap.xml", HttpStatus.SC_NOT_FOUND, "response content"));

			Map<String, Object> document = new HashMap<>();
			document.put(GetSitemapHtmlClient.DOC_FIELD_URL, "http://test.org/doc");
			tested.getChangedDocumentDetails("myspace", "myid", document);
			Assert.fail("RemoteDocumentNotFoundException expected");
		} catch (RemoteDocumentNotFoundException e) {
			// OK
		}

		try {
			Map<String, Object> config = new HashMap<String, Object>();
			config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
			GetSitemapHtmlClient tested = createTestedInstanceWithHttpCallException(config, new ClientProtocolException(
					"http protocol error"));

			Map<String, Object> document = new HashMap<>();
			document.put(GetSitemapHtmlClient.DOC_FIELD_URL, "http://test.org/doc");
			tested.getChangedDocumentDetails("myspace", "myid", document);
			Assert.fail("RemoteDocumentNotFoundException expected");
		} catch (RemoteDocumentNotFoundException e) {
			// OK
		}
	}

	@Test(expected = RemoteDocumentNotFoundException.class)
	public void getChangedDocumentDetails_noHtmlFormat() throws Exception {

		Map<String, Object> config = new HashMap<String, Object>();
		config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
		GetSitemapHtmlClient tested = createTestedInstance(config, "dfgsdfg", "application/font", "http://test.org/doc");

		Map<String, Object> document = new HashMap<>();
		document.put(GetSitemapHtmlClient.DOC_FIELD_URL, "http://test.org/doc");

		tested.getChangedDocumentDetails("myspace", "myid", document);
	}

	@Test
	public void getChangedDocumentDetails_noHtmlMappingDefined() throws Exception {

		Map<String, Object> config = new HashMap<String, Object>();
		config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
		GetSitemapHtmlClient tested = createTestedInstance(config, "<body>my html body</body>", CT_HTML,
				"http://test.org/doc");

		Map<String, Object> document = new HashMap<>();
		document.put(GetSitemapHtmlClient.DOC_FIELD_URL, "http://test.org/doc");

		Object o = tested.getChangedDocumentDetails("myspace", "myid", document);
		Assert.assertEquals("<html>\n <head></head>\n <body>\n  my html body\n </body>\n</html>", o);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getChangedDocumentDetails_htmlMappingDefined() throws Exception {

		Map<String, Object> config = new HashMap<String, Object>();
		config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
		Map<String, Object> htmlMapping = new HashMap<>();
		createMappingField(htmlMapping, "field_full_nostripe", null, null);
		createMappingField(htmlMapping, "field_full_stripe", null, true);
		createMappingField(htmlMapping, "field_css_nostripe", ".myclass", false);
		createMappingField(htmlMapping, "field_css_stripe", ".myclass", true);
		createMappingField(htmlMapping, "field_css1_nostripe", ".myclass:eq(1)", false);
		createMappingField(htmlMapping, "field_css1_stripe", ".myclass:eq(1)", true);
		createMappingField(htmlMapping, "field_unknown_css_stripe", ".myclassunknown", true);
		createMappingField(htmlMapping, "field_unknown_css_nostripe", ".myclassunknown", true);
		createMappingField(htmlMapping, "field_unknown_css_attribute", ".myclass", "unknownAtt", true);
		config.put(GetSitemapHtmlClient.CFG_HTML_MAPPING, htmlMapping);

		GetSitemapHtmlClient tested = createTestedInstance(
				config,
				"<html><body>my html body\n<div class='myclass'>my class &amp; content</div>\n<div class='myclass'>my <b>class</b> content 2</div></body><html>",
				CT_HTML, "http://test.org/doc");

		Map<String, Object> document = new HashMap<>();
		document.put(GetSitemapHtmlClient.DOC_FIELD_URL, "http://test.org/doc");

		Map<String, String> o = (Map<String, String>) tested.getChangedDocumentDetails("myspace", "myid", document);

		Assert.assertEquals("<html>\n <head></head>\n <body>\n  my html body \n"
				+ "  <div class=\"myclass\">\n   my class &amp; content\n  </div> \n  <div class=\"myclass\">\n   my \n"
				+ "   <b>class</b> content 2\n  </div>\n </body>\n</html>", o.get("field_full_nostripe"));
		Assert.assertEquals("my html body my class & content my class content 2", o.get("field_full_stripe"));
		Assert.assertEquals("<div class=\"myclass\">\n my class &amp; content\n</div>\n<div class=\"myclass\">\n"
				+ " my \n <b>class</b> content 2\n" + "</div>", o.get("field_css_nostripe"));
		Assert.assertEquals("my class & content my class content 2", o.get("field_css_stripe"));
		Assert.assertEquals("my \n<b>class</b> content 2", o.get("field_css1_nostripe"));
		Assert.assertEquals("my class content 2", o.get("field_css1_stripe"));
		Assert.assertEquals(null, o.get("field_unknown_css_stripe"));
		Assert.assertEquals(null, o.get("field_unknown_css_nostripe"));
		Assert.assertNull(o.get("field_unknown_css_attribute"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getChangedDocumentDetails_htmlMappingDefined_attributes() throws Exception {

		Map<String, Object> config = new HashMap<String, Object>();
		config.put(GetSitemapHtmlClient.CFG_URL_GET_SITEMAP, "http://test.org/sitemap.xml");
		Map<String, Object> htmlMapping = new HashMap<>();
		createMappingField(htmlMapping, "field_css_attribute_one", "meta[name=description]", "content", true);
		createMappingField(htmlMapping, "field_css_attribute_more", ".list", "content", true);
		createMappingField(htmlMapping, "field_unknown_css_attribute", ".myclass", "unknownAtt", true);
		config.put(GetSitemapHtmlClient.CFG_HTML_MAPPING, htmlMapping);

		GetSitemapHtmlClient tested = createTestedInstance(
				config,
				"<html><head><meta name=\"description\" content=\"my &amp; description\"></head><body><span class='list' content='one'/><span class='list' content='two'/><span class='list' content=''/><span class='list' content='three'/></body><html>",
				CT_HTML, "http://test.org/doc");

		Map<String, Object> document = new HashMap<>();
		document.put(GetSitemapHtmlClient.DOC_FIELD_URL, "http://test.org/doc");

		Map<String, String> o = (Map<String, String>) tested.getChangedDocumentDetails("myspace", "myid", document);

		Assert.assertEquals("my & description", o.get("field_css_attribute_one"));
		Assert.assertEquals("one two three", o.get("field_css_attribute_more"));
		Assert.assertNull(o.get("field_unknown_css_attribute"));
	}

	private void createMappingField(Map<String, Object> htmlMapping, String field, Object cssSelector, Boolean stripeHtml) {
		createMappingField(htmlMapping, field, cssSelector, null, stripeHtml);
	}

	private void createMappingField(Map<String, Object> htmlMapping, String field, Object cssSelector,
			String valueAttribute, Boolean stripeHtml) {
		Map<String, Object> hm = new HashMap<String, Object>();
		if (cssSelector != null)
			hm.put(GetSitemapHtmlClient.CFG_HM_CSS_SELECTOR, cssSelector);
		if (stripeHtml != null)
			hm.put(GetSitemapHtmlClient.CFG_HM_STRIP_HTML, stripeHtml);
		if (valueAttribute != null)
			hm.put(GetSitemapHtmlClient.CFG_HM_VALUE_ATTRIBUTE, valueAttribute);
		htmlMapping.put(field, hm);
	}

	@Test
	public void convertNodeToText() {
		Assert.assertEquals("", GetSitemapHtmlClient.convertNodeToText(Jsoup.parse("")));
		Assert.assertEquals("ahoj", GetSitemapHtmlClient.convertNodeToText(Jsoup.parse("ahoj")));
		Assert.assertEquals("ahoj", GetSitemapHtmlClient.convertNodeToText(Jsoup.parse("<p>ahoj</p>")));
		Assert.assertEquals("ahoj home fohe",
				GetSitemapHtmlClient.convertNodeToText(Jsoup.parse("<p>ahoj</p><p>home fohe</p>")));
		Assert.assertEquals("ahoj home fohe my link", GetSitemapHtmlClient.convertNodeToText(Jsoup
				.parse("<p>ahoj</p><p class=\"class\">home fohe</p>\n<div><a href='http://mylink.com'>my link</a></div>")));

	}

	@Test
	public void convertElementsToText() {
		Assert
				.assertEquals(
						"my link",
						GetSitemapHtmlClient
								.convertElementsToText(Jsoup
										.parse(
												"<p>ahoj</p><p class=\"class\">home fohe</p>\n<div class='myclass'><a href='http://mylink.com'>my link</a></div>")
										.select(".myclass")));
		Assert
				.assertEquals(
						"my link my link 2",
						GetSitemapHtmlClient
								.convertElementsToText(Jsoup
										.parse(
												"<p>ahoj</p><p class=\"class\">home fohe</p>\n<div class='myclass'><a href='http://mylink.com'>my link</a></div>\\n<div class='myclass'><a href='http://mylink.com'>my link 2</a></div>")
										.select(".myclass")));

	}

	private GetSitemapHtmlClient createTestedInstance(Map<String, Object> config, final String returnSitemapData,
			final String returnContentType, final String expectadCallUrl) {
		GetSitemapHtmlClient tested = new GetSitemapHtmlClient() {
			@Override
			protected HttpResponseContent performHttpGetCall(String url, Map<String, String> headers) throws Exception,
					HttpCallException {
				Assert.assertEquals(expectadCallUrl, url);
				return new HttpResponseContent(returnContentType, returnSitemapData.getBytes("UTF-8"));
			};

		};
		tested.init(config, false, null);
		return tested;
	}

	private GetSitemapHtmlClient createTestedInstanceWithHttpCallException(Map<String, Object> config,
			final Exception exception) {
		GetSitemapHtmlClient tested = new GetSitemapHtmlClient() {
			@Override
			protected HttpResponseContent performHttpGetCall(String url, Map<String, String> headers) throws Exception {
				throw exception;
			};

		};
		tested.init(config, false, null);
		return tested;
	}

}

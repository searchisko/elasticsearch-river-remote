/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SettingsException;
import org.jboss.elasticsearch.river.remote.exception.RemoteDocumentNotFoundException;
import org.jboss.elasticsearch.river.remote.sitemap.AbstractSiteMap;
import org.jboss.elasticsearch.river.remote.sitemap.SiteMap;
import org.jboss.elasticsearch.river.remote.sitemap.SiteMapParser;
import org.jboss.elasticsearch.river.remote.sitemap.SiteMapURL;

/**
 * Class used to HTTP GET data from <a href="http://www.sitemaps.org">sitemap</a> and then download and process HTML for
 * them.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class GetSitemapHtmlClient extends HttpRemoteSystemClientBase {

	protected static final String CFG_URL_GET_SITEMAP = "urlGetSitemap";

	private static final ESLogger logger = Loggers.getLogger(GetSitemapHtmlClient.class);

	protected static final String DOC_FIELD_ID = "id";
	protected static final String DOC_FIELD_URL = "url";
	protected static final String DOC_FIELD_LAST_MODIFIED = "last_modified";
	protected static final String DOC_FIELD_PRIORITY = "priority";

	protected String urlGetSitemap;

	protected SiteMapParser sitemapParser = new SiteMapParser();

	@Override
	public void init(Map<String, Object> config, boolean spaceListLoadingEnabled, IPwdLoader pwdLoader) {
		urlGetSitemap = getUrlFromConfig(config, CFG_URL_GET_SITEMAP, true);

		if (spaceListLoadingEnabled) {
			throw new SettingsException(
					"Dynamic Spaces obtaining is not supported, use 'remote/spacesIndexed' to configure one space or static list");
		}

		String remoteUsername = initHttpClient(logger, config, pwdLoader, urlGetSitemap);

		logger.info("Configured sitemap.xml HTML client for URL '{}', remote system user '{}'.", urlGetSitemap,
				remoteUsername != null ? remoteUsername : "Anonymous access");
	}

	@Override
	public List<String> getAllSpaces() throws Exception {
		throw new UnsupportedOperationException(
				"Dynamic Spaces obtaining is not supported, use 'remote/spacesIndexed' to configure one space or static list");
	}

	@Override
	public ChangedDocumentsResults getChangedDocuments(String spaceKey, int startAt, Date updatedAfter) throws Exception {
		byte[] responseData = performHttpGetCall(urlGetSitemap, null);

		logger.debug("HTTP GET sitemap response data: {}", new String(responseData));

		List<Map<String, Object>> documents = processSitemap(responseData, urlGetSitemap);

		return new ChangedDocumentsResults(documents, 0, documents.size());
	}

	protected List<Map<String, Object>> processSitemap(byte[] responseData, String url) throws Exception {
		AbstractSiteMap asm = sitemapParser.parseSiteMap(null, responseData, new URL(url));

		if (asm.isIndex()) {
			throw new Exception("Sitemap index format is not supported by this river!");
		}

		SiteMap sm = (SiteMap) asm;

		List<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
		for (SiteMapURL smurl : sm.getSiteMapUrls()) {

			Map<String, Object> document = new HashMap<String, Object>();
			String durl = smurl.getUrl().toExternalForm();
			document.put(DOC_FIELD_ID, createIdFromUrl(durl));
			document.put(DOC_FIELD_URL, durl);
			document.put(DOC_FIELD_LAST_MODIFIED, DateTimeUtils.formatISODateTime(smurl.getLastModified()));
			document.put(DOC_FIELD_PRIORITY, new Double(smurl.getPriority()));
			documents.add(document);
		}
		return documents;
	}

	protected static String createIdFromUrl(String url) {
		if (url == null)
			return null;
		url = url.replace("://", "_");
		url = url.replace(":", "_");
		url = url.replace(".", "_");
		url = url.replace("=", "_");
		url = url.replace("\\", "_");
		url = url.replace("/", "_");
		url = url.replace("?", "_");
		url = url.replace("&", "_");
		url = url.replace("%", "_");
		url = url.replace("*", "_");
		url = url.replace("$", "_");
		url = url.replace("#", "_");
		url = url.replace("@", "_");
		url = url.replace("+", "_");
		url = url.replace("<", "_");
		url = url.replace(">", "_");
		return url;
	}

	@Override
	public Object getChangedDocumentDetails(String spaceKey, String documentId, Map<String, Object> document)
			throws Exception, RemoteDocumentNotFoundException {
		try {
			String url = (String) document.get(DOC_FIELD_URL);
			if (url == null) {
				return null;
			}
			byte[] responseData = performHttpGetCall(url, null);
			// TODO parse and process HTML
			return null;
		} catch (HttpCallException e) {
			if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
				throw new RemoteDocumentNotFoundException(e);
			} else {
				throw e;
			}
		}
	}

}

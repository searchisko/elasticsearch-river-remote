/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.io.ByteArrayInputStream;
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
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.jboss.elasticsearch.river.remote.exception.RemoteDocumentNotFoundException;
import org.jboss.elasticsearch.river.remote.sitemap.AbstractSiteMap;
import org.jboss.elasticsearch.river.remote.sitemap.SiteMap;
import org.jboss.elasticsearch.river.remote.sitemap.SiteMapParser;
import org.jboss.elasticsearch.river.remote.sitemap.SiteMapURL;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeTraversor;
import org.jsoup.select.NodeVisitor;

/**
 * Class used to HTTP GET data from <a href="http://www.sitemaps.org">sitemap</a> and then download and process HTML for
 * them.
 * <p>
 * Document structure returned from {@link #getChangedDocuments(String, int, Date)} contains fields defined in
 * <code>DOC_FIELD_xx</code> constants.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class GetSitemapHtmlClient extends HttpRemoteSystemClientBase {

	protected static final String CFG_HM_STRIP_HTML = "stripHtml";
	protected static final String CFG_HM_CSS_SELECTOR = "cssSelector";
	protected static final String CFG_URL_GET_SITEMAP = "urlGetSitemap";
	protected static final String CFG_HTML_MAPPING = "htmlMapping";

	private static final ESLogger logger = Loggers.getLogger(GetSitemapHtmlClient.class);

	public static final String DOC_FIELD_ID = "id";
	public static final String DOC_FIELD_URL = "url";
	public static final String DOC_FIELD_LAST_MODIFIED = "last_modified";
	public static final String DOC_FIELD_PRIORITY = "priority";

	protected String urlGetSitemap;

	protected Map<String, Map<String, Object>> htmlMapping;

	protected SiteMapParser sitemapParser = new SiteMapParser();

	@SuppressWarnings("unchecked")
	@Override
	public void init(Map<String, Object> config, boolean spaceListLoadingEnabled, IPwdLoader pwdLoader) {
		urlGetSitemap = getUrlFromConfig(config, CFG_URL_GET_SITEMAP, true);

		try {
			htmlMapping = (Map<String, Map<String, Object>>) config.get(CFG_HTML_MAPPING);
		} catch (ClassCastException e) {
			throw new SettingsException("'remote/" + CFG_HTML_MAPPING + "' configuration section is invalid");
		}

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

	/**
	 * Create document id from URL by replacing strange/problematic characters.
	 * 
	 * @param url to crete id from
	 * @return id
	 */
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

			try {
				Document doc = Jsoup.parse(new ByteArrayInputStream(responseData), null, url);

				if (htmlMapping == null) {
					return doc.html();
				} else {
					Map<String, String> ret = new HashMap<>();
					for (String dataField : htmlMapping.keySet()) {
						String value = null;
						Map<String, Object> fieldMappingConfig = htmlMapping.get(dataField);
						String cssSelector = Utils.trimToNull((String) fieldMappingConfig.get(CFG_HM_CSS_SELECTOR));
						boolean stripHtml = XContentMapValues.nodeBooleanValue(fieldMappingConfig.get(CFG_HM_STRIP_HTML), false);
						if (cssSelector != null) {
							Elements e = doc.select(cssSelector);
							if (e != null && !e.isEmpty()) {
								if (stripHtml) {
									value = convertElementsToText(e);
								} else {
									if (e.size() == 1) {
										value = e.html();
									} else {
										value = e.outerHtml();
									}
								}
							}
						} else {
							if (stripHtml) {
								value = convertNodeToText(doc);
							} else {
								value = doc.html();
							}
						}
						ret.put(dataField, value);
					}
					return ret;
				}
			} catch (ClassCastException e) {
				throw new SettingsException("'remote/" + CFG_HTML_MAPPING + "' configuration section is invalid");
			} catch (Exception e) {
				throw new RemoteDocumentNotFoundException("HTML document can't be processed: " + e.getMessage(), e);
			}
		} catch (HttpCallException e) {
			if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
				throw new RemoteDocumentNotFoundException(e);
			} else {
				throw e;
			}
		}
	}

	protected static String convertNodeToText(Node node) {
		if (node == null)
			return "";
		StringBuilder buffer = new StringBuilder();
		new NodeTraversor(new ToTextNodeVisitor(buffer)).traverse(node);
		return buffer.toString().trim();
	}

	protected static String convertElementsToText(Elements elements) {
		if (elements == null || elements.isEmpty())
			return "";
		StringBuilder buffer = new StringBuilder();
		NodeTraversor nt = new NodeTraversor(new ToTextNodeVisitor(buffer));
		for (Element element : elements) {
			nt.traverse(element);
		}
		return buffer.toString().trim();
	}

	private static final class ToTextNodeVisitor implements NodeVisitor {
		final StringBuilder buffer;

		ToTextNodeVisitor(StringBuilder buffer) {
			this.buffer = buffer;
		}

		@Override
		public void head(Node node, int depth) {
			if (node instanceof TextNode) {
				TextNode textNode = (TextNode) node;
				String text = textNode.text().replace('\u00A0', ' ').trim(); // non breaking space
				if (!text.isEmpty()) {
					buffer.append(text);
					if (!text.endsWith(" ")) {
						buffer.append(" ");
					}
				}
			}
		}

		@Override
		public void tail(Node node, int depth) {
		}

	}

}

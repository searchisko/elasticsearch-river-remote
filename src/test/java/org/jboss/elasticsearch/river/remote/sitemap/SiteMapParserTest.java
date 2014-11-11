/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jboss.elasticsearch.river.remote.sitemap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

import org.jboss.elasticsearch.river.remote.DateTimeUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Unit test for {@link SiteMapParser}
 * 
 * @author http://code.google.com/p/crawler-commons
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SiteMapParserTest {

	public static final String SITEMAP_XML_INDEX = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">" + "<sitemap>"
			+ "  <loc>http://www.example.com/sitemap1.xml.gz</loc>" + " <lastmod>2004-10-01T18:23:17+00:00</lastmod>"
			+ " </sitemap>" + "<sitemap>" + "    <loc>http://www.example.com/sitemap2.xml.gz</loc>"
			+ "   <lastmod>2005-01-01</lastmod>" + " </sitemap>" + " </sitemapindex>";

	@Test
	public void parseSiteMap_Index() throws UnknownFormatException, IOException {
		SiteMapParser parser = new SiteMapParser();
		Assert.assertTrue(parser.isStrict());
		String contentType = "text/xml";

		byte[] content = SITEMAP_XML_INDEX.getBytes();
		URL url = new URL(URL_SITEMAP_XML);
		AbstractSiteMap asm = parser.parseSiteMap(contentType, content, url);
		assertEquals(true, asm.isIndex());
		assertEquals(true, asm instanceof SiteMapIndex);
		SiteMapIndex smi = (SiteMapIndex) asm;
		assertEquals(2, smi.getSitemaps().size());
		AbstractSiteMap currentSiteMap = smi.getSitemap(new URL("http://www.example.com/sitemap1.xml.gz"));
		assertNotNull(currentSiteMap);
		assertEquals("http://www.example.com/sitemap1.xml.gz", currentSiteMap.getUrl().toString());
		assertEquals(SiteMap.convertToDate("2004-10-01T18:23:17+00:00"), currentSiteMap.getLastModified());

		currentSiteMap = smi.getSitemap(new URL("http://www.example.com/sitemap2.xml.gz"));
		assertNotNull(currentSiteMap);
		assertEquals("http://www.example.com/sitemap2.xml.gz", currentSiteMap.getUrl().toString());
		assertEquals(SiteMap.convertToDate("2005-01-01"), currentSiteMap.getLastModified());
	}

	public static final String URL_SITEMAP_XML = "http://www.example.com/sitemap.xml";

	public static final String SITEMAP_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">" + "  <url>"
			+ "<loc>http://www.example.com/</loc>" + "<lastmod>2005-01-01</lastmod>" + "<changefreq>monthly</changefreq>"
			+ "<priority>0.8</priority>" + "</url>" + "<url>"
			+ "<loc>http://www.example.com/catalog?item=12&amp;desc=vacation_hawaii</loc>"
			+ "<changefreq>weekly</changefreq>" + "</url>" + "<url>"
			+ "<loc>http://www.example.com/catalog?item=73&amp;desc=vacation_new_zealand</loc>"
			+ "<lastmod>2004-12-23</lastmod>" + "<changefreq>weekly</changefreq>" + "</url>" + "<url>"
			+ "<loc>http://www.example.com/catalog?item=74&amp;desc=vacation_newfoundland</loc>"
			+ "<lastmod>2004-12-23T18:00:15+00:00</lastmod>" + "<priority>0.3</priority>" + "</url>" + "<url>"
			+ "<loc>http://www.example.com/catalog?item=83&amp;desc=vacation_usa</loc>" + "<lastmod>2004-11-23</lastmod>"
			+ "</url>" + "</urlset>";

	@Test
	public void parseSiteMap_XML() throws UnknownFormatException, IOException {
		SiteMapParser parser = new SiteMapParser();
		String contentType = "text/xml";

		byte[] content = SITEMAP_XML.getBytes();
		URL url = new URL(URL_SITEMAP_XML);
		AbstractSiteMap asm = parser.parseSiteMap(contentType, content, url);
		assertEquals(false, asm.isIndex());
		assertEquals(true, asm instanceof SiteMap);
		SiteMap sm = (SiteMap) asm;
		assertEquals(5, sm.getSiteMapUrls().size());
		Assert.assertEquals("http://www.example.com/", sm.getSiteMapUrls().iterator().next().getUrl().toString());
		Assert.assertEquals("2005-01-01T00:00:00.0+0000",
				DateTimeUtils.formatISODateTime(sm.getSiteMapUrls().iterator().next().getLastModified()));
	}

	@Test(expected = UnknownFormatException.class)
	public void parseSiteMap_UnknownXml() throws UnknownFormatException, IOException {
		SiteMapParser parser = new SiteMapParser();
		String contentType = "text/xml";

		byte[] content = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<unknown>" + "</unknown>").getBytes();
		URL url = new URL(URL_SITEMAP_XML);
		parser.parseSiteMap(contentType, content, url);
	}

	@Test
	public void parseSiteMap_XMLGzip() throws UnknownFormatException, IOException {
		SiteMapParser parser = new SiteMapParser();
		String contentType = "application/x-gzip";

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		GZIPOutputStream gos = new GZIPOutputStream(bos);
		gos.write(SITEMAP_XML.getBytes());
		gos.close();
		URL url = new URL(URL_SITEMAP_XML + ".gz");
		AbstractSiteMap asm = parser.parseSiteMap(contentType, bos.toByteArray(), url);
		assertEquals(false, asm.isIndex());
		assertEquals(true, asm instanceof SiteMap);
		SiteMap sm = (SiteMap) asm;
		assertEquals(5, sm.getSiteMapUrls().size());
		Assert.assertEquals("http://www.example.com/", sm.getSiteMapUrls().iterator().next().getUrl().toString());
		Assert.assertEquals("2005-01-01T00:00:00.0+0000",
				DateTimeUtils.formatISODateTime(sm.getSiteMapUrls().iterator().next().getLastModified()));
	}

	public static final String SITEMAP_XML_NO_DECLARATIONS = "<urlset>" + "  <url>"
			+ "<loc>http://www.example.com/</loc>" + "<lastmod>2005-01-01</lastmod>" + "<changefreq>monthly</changefreq>"
			+ "<priority>0.8</priority>" + "</url>" + "<url>"
			+ "<loc>http://www.example.com/catalog?item=12&amp;desc=vacation_hawaii</loc>"
			+ "<changefreq>weekly</changefreq>" + "</url>" + "<url>"
			+ "<loc>http://www.example.com/catalog?item=73&amp;desc=vacation_new_zealand</loc>"
			+ "<lastmod>2004-12-23</lastmod>" + "<changefreq>weekly</changefreq>" + "</url>" + "<url>"
			+ "<loc>http://www.example.com/catalog?item=74&amp;desc=vacation_newfoundland</loc>"
			+ "<lastmod>2004-12-23T18:00:15+00:00</lastmod>" + "<priority>0.3</priority>" + "</url>" + "<url>"
			+ "<loc>http://www.example.com/catalog?item=83&amp;desc=vacation_usa</loc>" + "<lastmod>2004-11-23</lastmod>"
			+ "</url>" + "</urlset>";

	@Test(expected = UnknownFormatException.class)
	public void parseSiteMap_UnknownFormat() throws UnknownFormatException, IOException {
		SiteMapParser parser = new SiteMapParser();
		String contentType = "text/unklnown";

		byte[] content = SITEMAP_XML_NO_DECLARATIONS.getBytes();
		URL url = new URL(URL_SITEMAP_XML + ".aaa");
		parser.parseSiteMap(contentType, content, url);
	}

	@Test
	public void parseSiteMap_XMLNoDeclaration() throws UnknownFormatException, IOException {
		SiteMapParser parser = new SiteMapParser();
		String contentType = "text/xml";

		byte[] content = SITEMAP_XML_NO_DECLARATIONS.getBytes();
		URL url = new URL(URL_SITEMAP_XML);
		AbstractSiteMap asm = parser.parseSiteMap(contentType, content, url);
		assertEquals(false, asm.isIndex());
		assertEquals(true, asm instanceof SiteMap);
		SiteMap sm = (SiteMap) asm;
		assertEquals(5, sm.getSiteMapUrls().size());
	}

	@Test
	public void parseSiteMap_XMLNoDeclarationNoContentype() throws UnknownFormatException, IOException {
		SiteMapParser parser = new SiteMapParser();
		byte[] content = SITEMAP_XML_NO_DECLARATIONS.getBytes();

		URL url = new URL(URL_SITEMAP_XML);
		AbstractSiteMap asm = parser.parseSiteMap(null, content, url);
		assertEquals(false, asm.isIndex());
		assertEquals(true, asm instanceof SiteMap);
		SiteMap sm = (SiteMap) asm;
		assertEquals(5, sm.getSiteMapUrls().size());
	}

	@Test
	public void parseSiteMap_TXT() throws UnknownFormatException, IOException {
		SiteMapParser parser = new SiteMapParser();
		String contentType = "text/plain";

		String scontent = "http://www.example.com/catalog?item=1\nhttp://www.example.com/catalog?item=11";

		byte[] content = scontent.getBytes();
		URL url = new URL("http://www.example.com/sitemap.txt");
		AbstractSiteMap asm = parser.parseSiteMap(contentType, content, url);
		assertEquals(false, asm.isIndex());
		assertEquals(true, asm instanceof SiteMap);
		SiteMap sm = (SiteMap) asm;
		assertEquals(2, sm.getSiteMapUrls().size());
	}

	@Test(expected = UnknownFormatException.class)
	public void parseSiteMap_BrokenXml() throws IOException, UnknownFormatException {
		// This Sitemap contains badly formatted XML and can't be read
		SiteMapParser parser = new SiteMapParser();
		String contentType = "text/xml";

		String scontent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><urlset "
				+ "xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\"><url>"
				+ "<!-- This file is not a valid XML file --></url><url><loc>"
				+ "http://cs.harding.edu/fmccown/sitemaps/something.html</loc>"
				+ "</url><!-- missing opening url tag --></url></urlset>";
		byte[] content = scontent.getBytes();
		URL url = new URL("http://www.example.com/sitemapindex.xml");

		parser.parseSiteMap(contentType, content, url);
	}

	@Test
	public void parseSiteMap_lenientParser() throws UnknownFormatException, IOException {
		SiteMapParser parser = new SiteMapParser();
		String contentType = "text/xml";

		String scontent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
				+ "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">" + "  <url>"
				+ "<loc>http://www.example.com/</loc>" + " </url>" + "</urlset>";

		// no lenient parsing means URL is not there as it is not from range
		byte[] content = scontent.getBytes();
		URL url = new URL("http://www.example.com/subsection/sitemap.xml");
		AbstractSiteMap asm = parser.parseSiteMap(contentType, content, url);
		assertEquals(false, asm.isIndex());
		assertEquals(true, asm instanceof SiteMap);
		SiteMap sm = (SiteMap) asm;
		assertEquals(0, sm.getSiteMapUrls().size());

		// Now try again with lenient parsing. We should get one invalid URL
		parser = new SiteMapParser(false);
		Assert.assertFalse(parser.isStrict());
		asm = parser.parseSiteMap(contentType, content, url);
		assertEquals(false, asm.isIndex());
		assertEquals(true, asm instanceof SiteMap);
		sm = (SiteMap) asm;
		assertEquals(1, sm.getSiteMapUrls().size());
		assertFalse(sm.getSiteMapUrls().iterator().next().isValid());

		// no lenient parsing means URL is not there as it is from another domain
		parser = new SiteMapParser();
		asm = parser.parseSiteMap(contentType, content, new URL("http://www.example.org/sitemap.xml"));
		assertEquals(false, asm.isIndex());
		assertEquals(true, asm instanceof SiteMap);
		sm = (SiteMap) asm;
		assertEquals(0, sm.getSiteMapUrls().size());

		parser = new SiteMapParser(false);
		asm = parser.parseSiteMap(contentType, content, new URL("http://www.example.org/sitemap.xml"));
		assertEquals(false, asm.isIndex());
		assertEquals(true, asm instanceof SiteMap);
		sm = (SiteMap) asm;
		assertEquals(1, sm.getSiteMapUrls().size());
		assertFalse(sm.getSiteMapUrls().iterator().next().isValid());

	}

	public static final String URL_SITEMAP_ATOM = "http://www.example.com/sitemap.xml";

	public static final String SITEMAP_ATOM = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<feed>"
			+ "<modified>2005-01-01</modified>" + "<entry><link href=\"http://www.example.com/\"/></entry>"
			+ "<entry><link href=\"http://www.example.com/catalog?item=12&amp;desc=vacation_hawaii\"/></entry>"
			+ "<entry><link href=\"http://www.example.com/catalog?item=73&amp;desc=vacation_new_zealand\"/></entry>"
			+ "<entry><link href=\"http://www.example.com/catalog?item=74&amp;desc=vacation_newfoundland\"/></entry>"
			+ "<entry><link href=\"http://www.example.org/catalog?item=74&amp;desc=vacation_newfoundland\"/></entry>"
			+ "<entry><link href=\"\"/></entry>"
			+ "<entry><link href=\"http://www.example.com/catalog?item=83&amp;desc=vacation_usa\"/></entry>" + "</feed>";

	@Test
	public void parseSiteMap_Atom() throws UnknownFormatException, IOException {
		SiteMapParser parser = new SiteMapParser();
		String contentType = "application/atom+xml";

		byte[] content = SITEMAP_ATOM.getBytes();
		URL url = new URL(URL_SITEMAP_ATOM);
		AbstractSiteMap asm = parser.parseSiteMap(contentType, content, url);
		assertEquals(false, asm.isIndex());
		assertEquals(true, asm instanceof SiteMap);
		SiteMap sm = (SiteMap) asm;
		assertEquals(5, sm.getSiteMapUrls().size());
		Assert.assertEquals("http://www.example.com/", sm.getSiteMapUrls().iterator().next().getUrl().toString());
		Assert.assertEquals("2005-01-01T00:00:00.0+0000",
				DateTimeUtils.formatISODateTime(sm.getSiteMapUrls().iterator().next().getLastModified()));

	}

	public static final String URL_SITEMAP_RSS = "http://www.example.com/sitemap.xml";

	public static final String SITEMAP_RSS = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "<rss version=\"2.0\"> <channel>" + "<pubDate>Tue, 10 Jun 2003 04:00:00 GMT</pubDate>"
			+ "<item><link>http://www.example.com/</link></item>"
			+ "<item><link>http://www.example.com/catalog?item=12&amp;desc=vacation_hawaii</link></item>"
			+ "<item><link>http://www.example.com/catalog?item=73&amp;desc=vacation_new_zealand</link></item>"
			+ "<item><link>http://www.example.com/catalog?item=74&amp;desc=vacation_newfoundland</link></item>"
			+ "<item><link>http://www.example.org/catalog?item=74&amp;desc=vacation_newfoundland</link></item>"
			+ "<item><link></link></item>"
			+ "<item><link>http://www.example.com/catalog?item=83&amp;desc=vacation_usa</link></item>" + "</channel></rss>";

	@Test
	public void parseSiteMap_Rss() throws UnknownFormatException, IOException {
		SiteMapParser parser = new SiteMapParser();
		String contentType = "application/rss+xml";

		byte[] content = SITEMAP_RSS.getBytes();
		URL url = new URL(URL_SITEMAP_RSS);
		AbstractSiteMap asm = parser.parseSiteMap(contentType, content, url);
		assertEquals(false, asm.isIndex());
		assertEquals(true, asm instanceof SiteMap);
		SiteMap sm = (SiteMap) asm;
		assertEquals(5, sm.getSiteMapUrls().size());
		Assert.assertEquals("http://www.example.com/", sm.getSiteMapUrls().iterator().next().getUrl().toString());
		Assert.assertEquals("2003-06-10T04:00:00.0+0000",
				DateTimeUtils.formatISODateTime(sm.getSiteMapUrls().iterator().next().getLastModified()));

	}

	@Test
	public void urlIsLegal() {
		Assert.assertTrue(SiteMapParser.urlIsLegal("http://aaa.cz", "http://aaa.cz"));
		Assert.assertTrue(SiteMapParser.urlIsLegal("http://aaa.cz", "http://aaa.cz/aok/rtr.html"));
		Assert.assertTrue(SiteMapParser.urlIsLegal("http://aaA.cz", "http://aaa.CZ/aok/rtR.html"));

		Assert.assertFalse(SiteMapParser.urlIsLegal("http://aaa.com", "http://aaa.cz/aok/rtr.html"));
		Assert.assertFalse(SiteMapParser.urlIsLegal("http://aaa.cz/oo", "http://aaa.cz/aok/rtr.html"));

	}

}

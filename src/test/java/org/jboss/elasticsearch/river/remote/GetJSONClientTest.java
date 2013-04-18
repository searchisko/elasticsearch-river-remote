/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import junit.framework.Assert;

import org.apache.http.NameValuePair;
import org.elasticsearch.common.settings.SettingsException;
import org.junit.Test;

/**
 * Unit test for {@link GetJSONClient}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class GetJSONClientTest {

	/**
	 * URL used for Client constructor in unit tests.
	 */
	protected static final String TEST_URL = "https://issues.jboss.org";

	/**
	 * Date formatter used to prepare {@link Date} instances for tests
	 */
	protected SimpleDateFormat JQL_TEST_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	protected TimeZone JQL_TEST_TIMEZONE = TimeZone.getTimeZone("GMT");
	{
		JQL_TEST_DATE_FORMAT.setTimeZone(JQL_TEST_TIMEZONE);
	}

	/**
	 * Main method used to run integration tests with real JIRA call.
	 * 
	 * @param args not used
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		IRemoteSystemClient tested = new GetJSONClient("https://issues.jboss.org", null, null, 5000);

		// List<String> projects = tested.getAllJIRAProjects();
		// System.out.println(projects);

		ChangedDocumentsResults ret = tested.getChangedDocuments("ORG", 0,
				DateTimeUtils.parseISODateTime("2013-02-04T01:00:00Z"));
		System.out.println("total: " + ret.getTotal());
		System.out.println(ret);
	}

	@Test
	public void constructor() {
		try {
			new GetJSONClient(null, null, null, 5000);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}
		try {
			new GetJSONClient("  ", null, null, 5000);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}
		try {
			new GetJSONClient("nonsenseUrl", null, null, 5000);
			Assert.fail("SettingsException not thrown");
		} catch (SettingsException e) {
			// OK
		}

		GetJSONClient tested = new GetJSONClient("http://issues.jboss.org", null, null, 5000);
		Assert.assertEquals(GetJSONClient.prepareAPIURLFromBaseURL("http://issues.jboss.org"), tested.jiraRestAPIUrlBase);
		tested = new GetJSONClient(TEST_URL, null, null, 5000);
		Assert.assertEquals(GetJSONClient.prepareAPIURLFromBaseURL(TEST_URL), tested.jiraRestAPIUrlBase);
		Assert.assertFalse(tested.isAuthConfigured);

		tested = new GetJSONClient(TEST_URL, "", "pwd", 5000);
		Assert.assertFalse(tested.isAuthConfigured);

		tested = new GetJSONClient(TEST_URL, "uname", "pwd", 5000);
		Assert.assertTrue(tested.isAuthConfigured);
	}

	@Test
	public void getAllJIRAProjects() throws Exception {

		IRemoteSystemClient tested = new GetJSONClient(TEST_URL, null, null, 5000) {
			@Override
			protected byte[] performJIRAGetRESTCall(String restOperation, List<NameValuePair> params) throws Exception {
				Assert.assertEquals("project", restOperation);
				Assert.assertNull(params);
				return ("[{\"key\": \"ORG\", \"name\": \"ORG project\"},{\"key\": \"PPP\"}]").getBytes("UTF-8");
			};

		};

		List<String> ret = tested.getAllSpaces();
		Assert.assertNotNull(ret);
		Assert.assertEquals(2, ret.size());
		Assert.assertTrue(ret.contains("ORG"));
		Assert.assertTrue(ret.contains("PPP"));
	}

	@Test
	public void getJIRAChangedIssues() throws Exception {
		final Date ua = new Date();

		IRemoteSystemClient tested = new GetJSONClient(TEST_URL, null, null, 5000) {
			@Override
			protected byte[] performJIRAChangedIssuesREST(String projectKey, int startAt, Date updatedAfter,
					Date updatedBefore) throws Exception {
				Assert.assertEquals("ORG", projectKey);
				Assert.assertEquals(ua, updatedAfter);
				Assert.assertEquals(10, startAt);
				return "{\"startAt\": 5, \"maxResults\" : 10, \"total\" : 50, \"issues\" : [{\"key\" : \"ORG-45\"}]}"
						.getBytes("UTF-8");
			};
		};

		ChangedDocumentsResults ret = tested.getChangedDocuments("ORG", 10, ua);
		Assert.assertEquals(5, ret.getStartAt());
		Assert.assertEquals(10, ret.getMaxResults());
		Assert.assertEquals(50, ret.getTotal());
		Assert.assertNotNull(ret.getDocuments());
		Assert.assertEquals(1, ret.getDocumentsCount());
	}

	@Test
	public void performJIRAChangedIssuesREST() throws Exception {
		final Date ua = new Date();
		final Date ub = new Date();

		GetJSONClient tested = new GetJSONClient(TEST_URL, null, null, 5000) {
			@Override
			protected byte[] performJIRAGetRESTCall(String restOperation, List<NameValuePair> params) throws Exception {
				Assert.assertEquals("search", restOperation);
				Assert.assertNotNull(params);
				String mr = "-1";
				String fields = "";
				String startAt = "";
				for (NameValuePair param : params) {
					if (param.getName().equals("maxResults")) {
						mr = param.getValue();
					} else if (param.getName().equals("jql")) {
						Assert.assertEquals("JQL string", param.getValue());
					} else if (param.getName().equals("fields")) {
						fields = param.getValue();
					} else if (param.getName().equals("startAt")) {
						startAt = param.getValue();
					}
				}

				if ("-1".equals(mr)) {
					Assert.assertEquals(3, params.size());
				} else if ("10".equals(mr)) {
					Assert.assertEquals(4, params.size());
				} else if ("20".equals(mr)) {
					Assert.assertEquals(3, params.size());
				}

				return ("{\"maxResults\": " + mr + ", \"startAt\": " + startAt + ", \"fields\" : \"" + fields + "\" }")
						.getBytes("UTF-8");
			};

			@Override
			protected String prepareJIRAChangedIssuesJQL(String projectKey, Date updatedAfter, Date updatedBefore) {
				Assert.assertEquals("ORG", projectKey);
				Assert.assertEquals(ua, updatedAfter);
				Assert.assertEquals(ub, updatedBefore);
				return "JQL string";
			}
		};
		IDocumentIndexStructureBuilder jiraIssueIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		tested.setIndexStructureBuilder(jiraIssueIndexStructureBuilderMock);
		when(jiraIssueIndexStructureBuilderMock.getRequiredRemoteCallFields()).thenReturn(
				"key,status,issuetype,created,updated,reporter,assignee,summary,description");

		// case - no maxResults parameter defined
		byte[] ret = tested.performJIRAChangedIssuesREST("ORG", 10, ua, ub);
		Assert
				.assertEquals(
						"{\"maxResults\": -1, \"startAt\": 10, \"fields\" : \"key,status,issuetype,created,updated,reporter,assignee,summary,description\" }",
						new String(ret, "UTF-8"));

		// case - maxResults parameter defined
		tested.listJIRAIssuesMax = 10;
		ret = tested.performJIRAChangedIssuesREST("ORG", 20, ua, ub);
		Assert
				.assertEquals(
						"{\"maxResults\": 10, \"startAt\": 20, \"fields\" : \"key,status,issuetype,created,updated,reporter,assignee,summary,description\" }",
						new String(ret, "UTF-8"));

		// case - no fields defined
		reset(jiraIssueIndexStructureBuilderMock);
		tested.listJIRAIssuesMax = 20;
		ret = tested.performJIRAChangedIssuesREST("ORG", 30, ua, ub);
		Assert.assertEquals("{\"maxResults\": 20, \"startAt\": 30, \"fields\" : \"\" }", new String(ret, "UTF-8"));

	}

	@Test
	public void prepareAPIURLFromBaseURL() {
		Assert.assertNull(GetJSONClient.prepareAPIURLFromBaseURL(null));
		Assert.assertNull(GetJSONClient.prepareAPIURLFromBaseURL(""));
		Assert.assertNull(GetJSONClient.prepareAPIURLFromBaseURL("  "));
		Assert.assertEquals("http://issues.jboss.org/rest/api/2/",
				GetJSONClient.prepareAPIURLFromBaseURL("http://issues.jboss.org"));
		Assert.assertEquals("https://issues.jboss.org/rest/api/2/",
				GetJSONClient.prepareAPIURLFromBaseURL("https://issues.jboss.org/"));
	}

}

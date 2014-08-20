/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.testtools;

import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.jboss.elasticsearch.river.remote.DateTimeUtils;
import org.jboss.elasticsearch.river.remote.DocumentWithCommentsIndexStructureBuilder;
import org.jboss.elasticsearch.river.remote.RemoteRiver;

import static org.mockito.Mockito.mock;

/**
 * Class for ElasticSearch integration tests against some running ES cluster. This is not Unit test but helper for tests
 * during development!
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public abstract class ElasticSearchIntegrationTest {

	public static void main(String[] args) throws MalformedURLException {

		TransportClient client = new TransportClient();

		try {
			client.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
			Map<String, Object> settings = new HashMap<String, Object>();
			Map<String, Object> jiraSettings = new HashMap<String, Object>();
			settings.put("jira", jiraSettings);
			jiraSettings.put("urlBase", "http://issues-stg.jboss.org");
			Settings gs = mock(Settings.class);
			RiverSettings rs = new RiverSettings(gs, settings);

			RemoteRiver jr = new RemoteRiver(new RiverName("rt", "my_jira_river"), rs, client);
			DocumentWithCommentsIndexStructureBuilder structureBuilder = new DocumentWithCommentsIndexStructureBuilder(
					"my_jira_river", "my_jira_index", "jira_issue", null, true);

			String spaceKey = "ORG";
			// Date date = new Date();
			Date date = DateTimeUtils.parseISODateTime("2012-08-30T16:25:51");

			SearchRequestBuilder srb = jr.prepareESScrollSearchRequestBuilder(structureBuilder
					.getDocumentSearchIndexName(spaceKey));

			structureBuilder.buildSearchForIndexedDocumentsNotUpdatedAfter(srb, spaceKey, date);

			System.out.println(srb);

			SearchResponse response = jr.executeESSearchRequest(srb);
			response = jr.executeESScrollSearchNextRequest(response);

			System.out.println(response);

		} finally {
			client.close();
		}
	}

}

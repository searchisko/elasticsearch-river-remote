/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.jboss.elasticsearch.river.remote.testtools.ProjectInfoMatcher;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Unit test for {@link SpaceByLastUpdateTimestampIndexer}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceIndexerTest {

	/**
	 * Main method used to run integration tests with real remote call.
	 * 
	 * @param args not used
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		IRemoteSystemClient remoteClient = new GetJSONClient("https://issues.jboss.org", null, null, 7000);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder jiraIssueIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);

		SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer("ORG", false, remoteClient, esIntegrationMock,
				jiraIssueIndexStructureBuilderMock);
		tested.run();
	}

	@Test
	public void constructor() {
		IRemoteSystemClient remoteClient = new GetJSONClient("https://issues.jboss.org", null, null, 7000);
		IDocumentIndexStructureBuilder documentIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer("ORG", true, remoteClient, null, documentIndexStructureBuilderMock);
		Assert.assertEquals("ORG", tested.spaceKey);
		Assert.assertTrue(tested.indexingInfo.fullUpdate);
		Assert.assertEquals(remoteClient, tested.remoteSystemClient);
		Assert.assertEquals(documentIndexStructureBuilderMock, tested.documentIndexStructureBuilder);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void processUpdate_Basic() throws Exception {

		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder documentIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer("ORG", false, remoteClientMock, esIntegrationMock,
				documentIndexStructureBuilderMock);

		List<Map<String, Object>> issues = new ArrayList<Map<String, Object>>();

		// test case with empty result list from remote system search method
		// test case of 'last update date' reading from store and passing to the remote system search method
		{
			Date mockDateAfter = new Date();
			when(esIntegrationMock.readDatetimeValue("ORG", SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE))
					.thenReturn(mockDateAfter);
			when(remoteClientMock.getChangedDocuments("ORG", 0, mockDateAfter)).thenReturn(
					new ChangedDocumentsResults(issues, 0, 50, 0));

			tested.processUpdate();
			Assert.assertEquals(0, tested.getIndexingInfo().documentsUpdated);
			Assert.assertFalse(tested.getIndexingInfo().fullUpdate);
			verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 0, mockDateAfter);
			verify(esIntegrationMock, times(1)).readDatetimeValue(Mockito.any(String.class), Mockito.any(String.class));
			verify(esIntegrationMock, times(0)).prepareESBulkRequestBuilder();
			verify(esIntegrationMock, times(0)).storeDatetimeValue(Mockito.any(String.class), Mockito.any(String.class),
					Mockito.any(Date.class), Mockito.any(BulkRequestBuilder.class));
			verify(esIntegrationMock, times(0)).executeESBulkRequest(Mockito.any(BulkRequestBuilder.class));
			verify(esIntegrationMock, Mockito.atLeastOnce()).isClosed();
			Mockito.verifyNoMoreInteractions(remoteClientMock);
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

		// test case with one "page" of results from remote system search method
		// test case with 'last update date' storing
		{
			reset(esIntegrationMock);
			reset(remoteClientMock);
			reset(documentIndexStructureBuilderMock);
			when(esIntegrationMock.readDatetimeValue("ORG", SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE))
					.thenReturn(null);
			addDocumentMock(issues, "ORG-45", "2012-08-14T08:00:00.000-0400");
			addDocumentMock(issues, "ORG-46", "2012-08-14T08:01:00.000-0400");
			addDocumentMock(issues, "ORG-47", "2012-08-14T08:02:10.000-0400");
			configureStructureBuilderMockDefaults(documentIndexStructureBuilderMock);
			when(remoteClientMock.getChangedDocuments("ORG", 0, null)).thenReturn(
					new ChangedDocumentsResults(issues, 0, 50, 3));
			BulkRequestBuilder brb = new BulkRequestBuilder(null);
			when(esIntegrationMock.prepareESBulkRequestBuilder()).thenReturn(brb);

			tested.processUpdate();
			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertTrue(tested.indexingInfo.fullUpdate);
			verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 0, null);
			verify(esIntegrationMock, times(1)).readDatetimeValue(Mockito.any(String.class), Mockito.any(String.class));
			verify(esIntegrationMock, times(1)).prepareESBulkRequestBuilder();
			verify(documentIndexStructureBuilderMock, times(3)).indexDocument(Mockito.eq(brb), Mockito.eq("ORG"),
					Mockito.any(Map.class));
			verify(esIntegrationMock, times(1)).storeDatetimeValue(Mockito.eq("ORG"),
					Mockito.eq(SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE),
					Mockito.eq(DateTimeUtils.parseISODateTime("2012-08-14T08:02:10.000-0400")), eq(brb));
			verify(esIntegrationMock, times(1)).executeESBulkRequest(eq(brb));
			verify(esIntegrationMock, Mockito.atLeastOnce()).isClosed();
			Mockito.verifyNoMoreInteractions(remoteClientMock);
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

	}

	@SuppressWarnings("unchecked")
	@Test
	public void processUpdate_NoLastIsuueIndexedAgain() throws Exception {

		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder jiraIssueIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer("ORG", false, remoteClientMock, esIntegrationMock,
				jiraIssueIndexStructureBuilderMock);

		List<Map<String, Object>> issues = new ArrayList<Map<String, Object>>();

		// test case with list from remote system search method containing only one issue with same update time as 'last
		// update date'
		Date mockDateAfter = DateTimeUtils.parseISODateTime("2012-08-14T08:00:20.000-0400");
		when(esIntegrationMock.readDatetimeValue("ORG", SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE))
				.thenReturn(mockDateAfter);
		addDocumentMock(issues, "ORG-45", "2012-08-14T08:00:20.000-0400");
		configureStructureBuilderMockDefaults(jiraIssueIndexStructureBuilderMock);
		when(remoteClientMock.getChangedDocuments("ORG", 0, mockDateAfter)).thenReturn(
				new ChangedDocumentsResults(issues, 0, 50, 1));
		BulkRequestBuilder brb = new BulkRequestBuilder(null);
		when(esIntegrationMock.prepareESBulkRequestBuilder()).thenReturn(brb);

		tested.processUpdate();
		Assert.assertEquals(1, tested.indexingInfo.documentsUpdated);
		Assert.assertFalse(tested.indexingInfo.fullUpdate);
		verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 0, mockDateAfter);
		verify(esIntegrationMock, times(1)).readDatetimeValue(Mockito.any(String.class), Mockito.any(String.class));
		verify(esIntegrationMock, times(1)).prepareESBulkRequestBuilder();
		verify(jiraIssueIndexStructureBuilderMock, times(1)).indexDocument(Mockito.eq(brb), Mockito.eq("ORG"),
				Mockito.any(Map.class));
		verify(esIntegrationMock, times(1)).storeDatetimeValue(Mockito.eq("ORG"),
				Mockito.eq(SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE),
				Mockito.eq(DateTimeUtils.parseISODateTime("2012-08-14T08:00:20.000-0400")), eq(brb));
		verify(esIntegrationMock, times(1)).executeESBulkRequest(eq(brb));
		// one more timestamp store with time incremented by one second not to index last updated document next time again!
		verify(esIntegrationMock, times(1)).storeDatetimeValue(Mockito.eq("ORG"),
				Mockito.eq(SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE),
				Mockito.eq(DateTimeUtils.parseISODateTime("2012-08-14T08:00:21.000-0400")),
				((BulkRequestBuilder) Mockito.isNull()));
		verify(esIntegrationMock, Mockito.atLeastOnce()).isClosed();
		Mockito.verifyNoMoreInteractions(remoteClientMock);
		Mockito.verifyNoMoreInteractions(esIntegrationMock);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void processUpdate_PagedByDate() throws Exception {

		// test case with more than one "page" of results from remote system search method with different updated dates
		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder jiraIssueIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer("ORG", false, remoteClientMock, esIntegrationMock,
				jiraIssueIndexStructureBuilderMock);

		List<Map<String, Object>> issues = new ArrayList<Map<String, Object>>();
		addDocumentMock(issues, "ORG-45", "2012-08-14T08:00:10.000-0400");
		addDocumentMock(issues, "ORG-46", "2012-08-14T08:01:10.000-0400");
		addDocumentMock(issues, "ORG-47", "2012-08-14T08:02:20.000-0400");
		Date after2 = DateTimeUtils.parseISODateTime("2012-08-14T08:02:20.000-0400");
		List<Map<String, Object>> issues2 = new ArrayList<Map<String, Object>>();
		addDocumentMock(issues2, "ORG-481", "2012-08-14T08:03:10.000-0400");
		addDocumentMock(issues2, "ORG-49", "2012-08-14T08:04:10.000-0400");
		addDocumentMock(issues2, "ORG-154", "2012-08-14T08:05:20.000-0400");
		Date after3 = ISODateTimeFormat.dateTimeParser().parseDateTime("2012-08-14T08:05:20.000-0400").toDate();
		List<Map<String, Object>> issues3 = new ArrayList<Map<String, Object>>();
		addDocumentMock(issues3, "ORG-4", "2012-08-14T08:06:10.000-0400");
		addDocumentMock(issues3, "ORG-91", "2012-08-14T08:07:20.000-0400");
		when(esIntegrationMock.readDatetimeValue("ORG", SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE))
				.thenReturn(null);
		when(remoteClientMock.getChangedDocuments("ORG", 0, null)).thenReturn(new ChangedDocumentsResults(issues, 0, 3, 8));
		when(remoteClientMock.getChangedDocuments("ORG", 0, after2)).thenReturn(
				new ChangedDocumentsResults(issues2, 0, 3, 5));
		when(remoteClientMock.getChangedDocuments("ORG", 0, after3)).thenReturn(
				new ChangedDocumentsResults(issues3, 0, 3, 2));
		when(esIntegrationMock.prepareESBulkRequestBuilder()).thenReturn(new BulkRequestBuilder(null));
		configureStructureBuilderMockDefaults(jiraIssueIndexStructureBuilderMock);

		tested.processUpdate();
		Assert.assertEquals(8, tested.indexingInfo.documentsUpdated);
		Assert.assertTrue(tested.indexingInfo.fullUpdate);
		verify(esIntegrationMock, times(1)).readDatetimeValue(Mockito.any(String.class), Mockito.any(String.class));
		verify(esIntegrationMock, times(3)).prepareESBulkRequestBuilder();
		verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 0, null);
		verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 0, after2);
		verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 0, after3);
		verify(jiraIssueIndexStructureBuilderMock, times(8)).indexDocument(Mockito.any(BulkRequestBuilder.class),
				Mockito.eq("ORG"), Mockito.any(Map.class));
		verify(esIntegrationMock, times(3)).storeDatetimeValue(Mockito.any(String.class), Mockito.any(String.class),
				Mockito.any(Date.class), Mockito.any(BulkRequestBuilder.class));
		verify(esIntegrationMock, times(1)).storeDatetimeValue(Mockito.eq("ORG"),
				Mockito.eq(SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE),
				Mockito.eq(ISODateTimeFormat.dateTimeParser().parseDateTime("2012-08-14T08:02:20.000-0400").toDate()),
				Mockito.any(BulkRequestBuilder.class));
		verify(esIntegrationMock, times(1)).storeDatetimeValue(Mockito.eq("ORG"),
				Mockito.eq(SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE),
				Mockito.eq(ISODateTimeFormat.dateTimeParser().parseDateTime("2012-08-14T08:05:20.000-0400").toDate()),
				Mockito.any(BulkRequestBuilder.class));
		verify(esIntegrationMock, times(1)).storeDatetimeValue(Mockito.eq("ORG"),
				Mockito.eq(SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE),
				Mockito.eq(ISODateTimeFormat.dateTimeParser().parseDateTime("2012-08-14T08:07:20.000-0400").toDate()),
				Mockito.any(BulkRequestBuilder.class));
		verify(esIntegrationMock, times(3)).executeESBulkRequest(Mockito.any(BulkRequestBuilder.class));
		verify(esIntegrationMock, Mockito.atLeastOnce()).isClosed();
		Mockito.verifyNoMoreInteractions(remoteClientMock);
		Mockito.verifyNoMoreInteractions(esIntegrationMock);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void processUpdate_PagedByStartAt() throws Exception {

		// test case with more than one "page" of results from remote system search method with same updated dates so
		// pagination in
		// remote system is used. "same updated dates" means on millis precise basis!!!
		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder documentIssueIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer("ORG", false, remoteClientMock, esIntegrationMock,
				documentIssueIndexStructureBuilderMock);

		List<Map<String, Object>> issues = new ArrayList<Map<String, Object>>();
		addDocumentMock(issues, "ORG-45", "2012-08-14T08:00:00.000-0400");
		addDocumentMock(issues, "ORG-46", "2012-08-14T08:00:00.000-0400");
		addDocumentMock(issues, "ORG-47", "2012-08-14T08:00:00.000-0400");
		List<Map<String, Object>> issues2 = new ArrayList<Map<String, Object>>();
		addDocumentMock(issues2, "ORG-481", "2012-08-14T08:00:00.000-0400");
		addDocumentMock(issues2, "ORG-49", "2012-08-14T08:00:00.000-0400");
		addDocumentMock(issues2, "ORG-154", "2012-08-14T08:00:00.000-0400");
		List<Map<String, Object>> issues3 = new ArrayList<Map<String, Object>>();
		addDocumentMock(issues3, "ORG-4", "2012-08-14T08:00:00.000-0400");
		addDocumentMock(issues3, "ORG-91", "2012-08-14T08:00:00.000-0400");
		when(esIntegrationMock.readDatetimeValue("ORG", SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE))
				.thenReturn(null);
		when(remoteClientMock.getChangedDocuments("ORG", 0, null)).thenReturn(new ChangedDocumentsResults(issues, 0, 3, 8));
		when(remoteClientMock.getChangedDocuments("ORG", 3, null))
				.thenReturn(new ChangedDocumentsResults(issues2, 3, 3, 8));
		when(remoteClientMock.getChangedDocuments("ORG", 6, null))
				.thenReturn(new ChangedDocumentsResults(issues3, 6, 3, 8));
		when(esIntegrationMock.prepareESBulkRequestBuilder()).thenReturn(new BulkRequestBuilder(null));
		configureStructureBuilderMockDefaults(documentIssueIndexStructureBuilderMock);

		tested.processUpdate();
		Assert.assertEquals(8, tested.indexingInfo.documentsUpdated);
		verify(esIntegrationMock, times(1)).readDatetimeValue(Mockito.any(String.class), Mockito.any(String.class));
		verify(esIntegrationMock, times(3)).prepareESBulkRequestBuilder();
		verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 0, null);
		verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 3, null);
		verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 6, null);
		verify(documentIssueIndexStructureBuilderMock, times(8)).indexDocument(Mockito.any(BulkRequestBuilder.class),
				Mockito.eq("ORG"), Mockito.any(Map.class));
		verify(esIntegrationMock, times(3)).storeDatetimeValue(Mockito.any(String.class), Mockito.any(String.class),
				Mockito.any(Date.class), Mockito.any(BulkRequestBuilder.class));
		verify(esIntegrationMock, times(3)).storeDatetimeValue(Mockito.eq("ORG"),
				Mockito.eq(SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE),
				Mockito.eq(DateTimeUtils.parseISODateTime("2012-08-14T08:00:00.000-0400")),
				Mockito.any(BulkRequestBuilder.class));
		verify(esIntegrationMock, times(3)).executeESBulkRequest(Mockito.any(BulkRequestBuilder.class));
		verify(esIntegrationMock, Mockito.atLeastOnce()).isClosed();
		Mockito.verifyNoMoreInteractions(remoteClientMock);
		Mockito.verifyNoMoreInteractions(esIntegrationMock);

	}

	@Test
	public void run() throws Exception {
		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder documentIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer("ORG", false, remoteClientMock, esIntegrationMock,
				documentIndexStructureBuilderMock);

		List<Map<String, Object>> issues = new ArrayList<Map<String, Object>>();
		Date lastUpdatedDate = DateTimeUtils.parseISODateTime("2012-08-14T07:00:00.000-0400");

		addDocumentMock(issues, "ORG-45", "2012-08-14T08:00:00.000-0400");
		addDocumentMock(issues, "ORG-46", "2012-08-14T08:00:00.000-0400");
		addDocumentMock(issues, "ORG-47", "2012-08-14T08:00:00.000-0400");

		// test case with indexing finished OK
		{
			when(esIntegrationMock.readDatetimeValue("ORG", SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE))
					.thenReturn(lastUpdatedDate);
			when(remoteClientMock.getChangedDocuments("ORG", 0, lastUpdatedDate)).thenReturn(
					new ChangedDocumentsResults(issues, 0, 50, 3));
			when(esIntegrationMock.prepareESBulkRequestBuilder()).thenReturn(new BulkRequestBuilder(null));
			configureStructureBuilderMockDefaults(documentIndexStructureBuilderMock);

			tested.run();
			verify(esIntegrationMock, times(1)).reportIndexingFinished(
					Mockito.argThat(new ProjectInfoMatcher("ORG", false, true, 3, 0, null)));
		}

		// test case with indexing finished with error, but some documents was indexed from first page
		{
			reset(esIntegrationMock);
			reset(remoteClientMock);
			when(esIntegrationMock.readDatetimeValue("ORG", SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE))
					.thenReturn(null);
			when(remoteClientMock.getChangedDocuments("ORG", 0, null)).thenReturn(
					new ChangedDocumentsResults(issues, 0, 50, 4));
			when(remoteClientMock.getChangedDocuments("ORG", 3, null)).thenThrow(new Exception("Remote call error"));
			when(esIntegrationMock.prepareESBulkRequestBuilder()).thenReturn(new BulkRequestBuilder(null));

			tested.run();
			verify(esIntegrationMock, times(1)).reportIndexingFinished(
					Mockito.argThat(new ProjectInfoMatcher("ORG", true, false, 3, 0, "Remote call error")));
		}

		// case - run documents delete on full update!!!
		{
			reset(esIntegrationMock);
			reset(remoteClientMock);

			tested = new SpaceByLastUpdateTimestampIndexer("ORG", true, remoteClientMock, esIntegrationMock, documentIndexStructureBuilderMock);
			// prepare update part

			Date mockDate = new Date();
			when(esIntegrationMock.readDatetimeValue("ORG", SpaceByLastUpdateTimestampIndexer.STORE_PROPERTYNAME_LAST_INDEXED_ISSUE_UPDATE_DATE))
					.thenReturn(mockDate);
			// updatedAfter is null here (even some is returned from previous when) because we run full update!
			when(remoteClientMock.getChangedDocuments("ORG", 0, null)).thenReturn(
					new ChangedDocumentsResults(issues, 0, 50, 3));
			when(esIntegrationMock.prepareESBulkRequestBuilder()).thenReturn(new BulkRequestBuilder(null));

			// prepare delete part
			when(esIntegrationMock.prepareESScrollSearchRequestBuilder(Mockito.anyString())).thenReturn(
					new SearchRequestBuilder(null));
			SearchResponse sr1 = prepareSearchResponse("scrlid1", new InternalSearchHit(1, "ORG-12", "", null, null));
			when(esIntegrationMock.executeESSearchRequest(Mockito.any(SearchRequestBuilder.class))).thenReturn(sr1);
			SearchResponse sr2 = prepareSearchResponse("scrlid1", new InternalSearchHit(1, "ORG-12", "", null, null));
			when(esIntegrationMock.executeESScrollSearchNextRequest(sr1)).thenReturn(sr2);
			when(esIntegrationMock.executeESScrollSearchNextRequest(sr2)).thenReturn(prepareSearchResponse("scrlid3"));

			when(
					documentIndexStructureBuilderMock.deleteESDocument(Mockito.any(BulkRequestBuilder.class),
							Mockito.any(SearchHit.class))).thenReturn(true);

			tested.run();
			// verify updatedAfter is null in this call, not value read from store, because we run full update here!
			verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 0, null);
			verify(remoteClientMock, times(0)).getChangedDocuments("ORG", 0, mockDate);

			verify(esIntegrationMock, times(1)).reportIndexingFinished(
					Mockito.argThat(new ProjectInfoMatcher("ORG", true, true, 3, 1, null)));
		}

	}

	/**
	 * @param documentIndexStructureBuilderMock
	 */
	@SuppressWarnings("unchecked")
	private void configureStructureBuilderMockDefaults(IDocumentIndexStructureBuilder documentIndexStructureBuilderMock) {
		when(documentIndexStructureBuilderMock.extractDocumentId(Mockito.anyMap())).thenAnswer(new Answer<String>() {
			public String answer(InvocationOnMock invocation) throws Throwable {
				return (String) ((Map<String, Object>) invocation.getArguments()[0]).get("key");
			}
		});
		when(documentIndexStructureBuilderMock.extractDocumentUpdated(Mockito.anyMap())).thenAnswer(new Answer<Date>() {
			public Date answer(InvocationOnMock invocation) throws Throwable {
				return DateTimeUtils.parseISODateTime((String) ((Map<String, Object>) invocation.getArguments()[0])
						.get("updated"));
			}
		});

	}

	@Test
	public void processDelete() throws Exception {
		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder documentIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer("ORG", true, remoteClientMock, esIntegrationMock,
				documentIndexStructureBuilderMock);

		try {
			tested.processDelete(null);
			Assert.fail("IllegalArgumentException must be thrown");
		} catch (IllegalArgumentException e) {
			// OK
		}

		// case - nothing performed if no full update mode
		{
			tested.indexingInfo.fullUpdate = false;
			tested.processDelete(new Date());
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Mockito.verifyZeroInteractions(remoteClientMock);
			Mockito.verifyZeroInteractions(esIntegrationMock);
			Mockito.verifyZeroInteractions(documentIndexStructureBuilderMock);
		}

		// case - no issues for delete found
		{
			reset(remoteClientMock);
			reset(esIntegrationMock);
			reset(documentIndexStructureBuilderMock);

			tested.indexingInfo.fullUpdate = true;
			String testIndexName = "test_index";
			Date boundDate = DateTimeUtils.parseISODateTime("2012-08-14T07:00:00.000-0400");

			when(documentIndexStructureBuilderMock.getIssuesSearchIndexName("ORG")).thenReturn(testIndexName);
			SearchRequestBuilder srbmock = new SearchRequestBuilder(null);
			when(esIntegrationMock.prepareESScrollSearchRequestBuilder(testIndexName)).thenReturn(srbmock);
			when(esIntegrationMock.executeESSearchRequest(srbmock)).thenReturn(prepareSearchResponse("scrlid3"));

			tested.processDelete(boundDate);

			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			verify(documentIndexStructureBuilderMock).getIssuesSearchIndexName("ORG");
			verify(esIntegrationMock).refreshSearchIndex(testIndexName);
			verify(documentIndexStructureBuilderMock)
					.buildSearchForIndexedDocumentsNotUpdatedAfter(srbmock, "ORG", boundDate);
			verify(esIntegrationMock).prepareESScrollSearchRequestBuilder(testIndexName);
			verify(esIntegrationMock).executeESSearchRequest(srbmock);

			Mockito.verifyNoMoreInteractions(remoteClientMock);
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
			Mockito.verifyNoMoreInteractions(documentIndexStructureBuilderMock);
		}

		// case - perform delete for some issues
		{
			reset(remoteClientMock);
			reset(esIntegrationMock);
			reset(documentIndexStructureBuilderMock);

			tested.indexingInfo.fullUpdate = true;
			String testIndexName = "test_index";
			Date boundDate = DateTimeUtils.parseISODateTime("2012-08-14T07:00:00.000-0400");

			when(documentIndexStructureBuilderMock.getIssuesSearchIndexName("ORG")).thenReturn(testIndexName);
			SearchRequestBuilder srbmock = new SearchRequestBuilder(null);
			when(esIntegrationMock.prepareESScrollSearchRequestBuilder(testIndexName)).thenReturn(srbmock);

			SearchResponse sr = prepareSearchResponse("scrlid0", new InternalSearchHit(1, "ORG-12", "", null, null));
			when(esIntegrationMock.executeESSearchRequest(srbmock)).thenReturn(sr);

			BulkRequestBuilder brbmock = new BulkRequestBuilder(null);
			when(esIntegrationMock.prepareESBulkRequestBuilder()).thenReturn(brbmock);

			InternalSearchHit hit1_1 = new InternalSearchHit(1, "ORG-12", "", null, null);
			InternalSearchHit hit1_2 = new InternalSearchHit(2, "ORG-124", "", null, null);
			SearchResponse sr1 = prepareSearchResponse("scrlid1", hit1_1, hit1_2);
			when(esIntegrationMock.executeESScrollSearchNextRequest(sr)).thenReturn(sr1);

			InternalSearchHit hit2_1 = new InternalSearchHit(1, "ORG-22", "", null, null);
			InternalSearchHit hit2_2 = new InternalSearchHit(2, "ORG-224", "", null, null);
			InternalSearchHit hit2_3 = new InternalSearchHit(3, "ORG-2243", "", null, null);
			SearchResponse sr2 = prepareSearchResponse("scrlid2", hit2_1, hit2_2, hit2_3);
			when(esIntegrationMock.executeESScrollSearchNextRequest(sr1)).thenReturn(sr2);

			when(esIntegrationMock.executeESScrollSearchNextRequest(sr2)).thenReturn(prepareSearchResponse("scrlid3"));

			when(documentIndexStructureBuilderMock.deleteESDocument(Mockito.eq(brbmock), Mockito.any(SearchHit.class)))
					.thenReturn(true);

			tested.processDelete(boundDate);

			Assert.assertEquals(5, tested.indexingInfo.documentsDeleted);
			verify(documentIndexStructureBuilderMock).getIssuesSearchIndexName("ORG");
			verify(esIntegrationMock).refreshSearchIndex(testIndexName);
			verify(documentIndexStructureBuilderMock)
					.buildSearchForIndexedDocumentsNotUpdatedAfter(srbmock, "ORG", boundDate);
			verify(esIntegrationMock).prepareESScrollSearchRequestBuilder(testIndexName);
			verify(esIntegrationMock).executeESSearchRequest(srbmock);
			verify(esIntegrationMock).prepareESBulkRequestBuilder();
			verify(esIntegrationMock, times(3)).isClosed();
			verify(esIntegrationMock, times(3)).executeESScrollSearchNextRequest(Mockito.any(SearchResponse.class));
			verify(documentIndexStructureBuilderMock).deleteESDocument(brbmock, hit1_1);
			verify(documentIndexStructureBuilderMock).deleteESDocument(brbmock, hit1_2);
			verify(documentIndexStructureBuilderMock).deleteESDocument(brbmock, hit2_1);
			verify(documentIndexStructureBuilderMock).deleteESDocument(brbmock, hit2_2);
			verify(documentIndexStructureBuilderMock).deleteESDocument(brbmock, hit2_3);
			verify(esIntegrationMock).executeESBulkRequest(brbmock);

			Mockito.verifyNoMoreInteractions(remoteClientMock);
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
			Mockito.verifyNoMoreInteractions(documentIndexStructureBuilderMock);
		}
	}

	private SearchResponse prepareSearchResponse(String scrollId, InternalSearchHit... hits) {
		InternalSearchHits hitsi = new InternalSearchHits(hits, hits.length, 10f);
		InternalSearchResponse sr1i = new InternalSearchResponse(hitsi, null, false);
		SearchResponse sr1 = new SearchResponse(sr1i, scrollId, 1, 1, 100, null);
		return sr1;
	}

	/**
	 * Add document info structure into list of documents. Used to build mock {@link ChangedDocumentsResults} instances.
	 * 
	 * @param documents list to add document into
	 * @param key of document
	 * @param updated field of document with format: 2009-03-23T08:38:52.000-0400
	 */
	protected void addDocumentMock(List<Map<String, Object>> documents, String key, String updated) {
		Map<String, Object> issue = new HashMap<String, Object>();
		documents.add(issue);
		issue.put("key", key);
		issue.put("updated", updated);
	}

}

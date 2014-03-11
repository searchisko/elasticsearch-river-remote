/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.SearchHit;
import org.jboss.elasticsearch.river.remote.testtools.ESRealClientTestBase;
import org.jboss.elasticsearch.river.remote.testtools.TestUtils;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * jUnit test for {@link SpaceByLastUpdateTimestampIndexer} which tests search index update processes against embedded
 * inmemory elastic search node.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceByLastUpdateTimestampIndexer_IntegrationTest extends ESRealClientTestBase {

	private static final String CFG_RIVER_NAME = "remote_river";
	private static final String CFG_INDEX_NAME = CFG_RIVER_NAME;
	private static final String CFG_TYPE_DOCUMENT = "jira_issue";
	private static final String CFG_TYPE_COMMENT = "jira_issue_comment";

	private static final String CFG_TYPE_ACTIVITY = "jira_river_indexupdate";
	private static final String CFG_INDEX_NAME_ACTIVITY = "activity_index";

	private static final String PROJECT_KEY = "ORG";

	@Test
	public void incrementalUpdateCommentsEMBEDDED() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;
			remoteRiverMock.activityLogIndexName = CFG_INDEX_NAME_ACTIVITY;
			remoteRiverMock.activityLogTypeName = CFG_TYPE_ACTIVITY;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.EMBEDDED;

			initIndexStructures(client, structureBuilder.commentIndexingMode);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer(PROJECT_KEY, false,
					remoteClientMock, remoteRiverMock, structureBuilder);
			Date lastIssueUpdatedDate = DateTimeUtils.parseISODateTime("2012-09-06T02:26:53.000-0400");
			tested.storeLastDocumentUpdatedDate(null, PROJECT_KEY, lastIssueUpdatedDate);
			when(remoteClientMock.getChangedDocuments(PROJECT_KEY, 0, lastIssueUpdatedDate)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(PROJECT_KEY), Mockito.eq(0),
							Mockito.eq(DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400")))).thenReturn(
					new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(false, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate2 = DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate2, tested.readLastDocumentUpdatedDate(PROJECT_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun1, "ORG-1501", "ORG-1513", "ORG-1514");
			assertNumDocumentsInIndex(client, CFG_INDEX_NAME_ACTIVITY, CFG_TYPE_ACTIVITY, 1);

			// run 2 to update one document
			Thread.sleep(100);
			Date dateStartRun2 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(PROJECT_KEY, false, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(PROJECT_KEY, 0, lastIssueUpdatedDate2)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501-updated"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(PROJECT_KEY), Mockito.eq(0),
							Mockito.eq(DateTimeUtils.parseISODateTime("2012-09-06T03:28:22.000-0400")))).thenReturn(
					new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(1, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(false, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate3 = DateTimeUtils.parseISODateTime("2012-09-06T03:28:21.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate3, tested.readLastDocumentUpdatedDate(PROJECT_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1501");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1513", "ORG-1514");

			assertNumDocumentsInIndex(client, CFG_INDEX_NAME_ACTIVITY, CFG_TYPE_ACTIVITY, 2);

		} finally {
			finalizeESClientForUnitTest();
		}
	}

	private DocumentWithCommentsIndexStructureBuilder prepareStructureBuilder() {
		DocumentWithCommentsIndexStructureBuilder structureBuilder = new DocumentWithCommentsIndexStructureBuilder(
				CFG_RIVER_NAME, CFG_INDEX_NAME, CFG_TYPE_DOCUMENT,
				DocumentWithCommentsIndexStructureBuilderTest.createSettingsWithMandatoryFilled());
		structureBuilder.remoteDataFieldForDocumentId = "key";
		structureBuilder.remoteDataFieldForUpdated = "fields.updated";
		structureBuilder.remoteDataFieldForComments = "fields.comment.comments";
		structureBuilder.remoteDataFieldForCommentId = "id";
		structureBuilder.indexFieldForComments = "comments";
		structureBuilder.commentTypeName = CFG_TYPE_COMMENT;
		structureBuilder.commentFieldsConfig = new HashMap<String, Map<String, String>>();
		return structureBuilder;
	}

	@Test
	public void incrementalUpdateCommentsCHILD() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.CHILD;
			initIndexStructures(client, structureBuilder.commentIndexingMode);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer(PROJECT_KEY, false,
					remoteClientMock, remoteRiverMock, structureBuilder);
			Date lastIssueUpdatedDate = DateTimeUtils.parseISODateTime("2012-09-06T02:26:53.000-0400");
			tested.storeLastDocumentUpdatedDate(null, PROJECT_KEY, lastIssueUpdatedDate);
			when(remoteClientMock.getChangedDocuments(PROJECT_KEY, 0, lastIssueUpdatedDate)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(PROJECT_KEY), Mockito.eq(0),
							Mockito.eq(DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400")))).thenReturn(
					new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(false, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate2 = DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate2, tested.readLastDocumentUpdatedDate(PROJECT_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714252", "12716241");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun1, "ORG-1501", "ORG-1513", "ORG-1514");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1513");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1514", "12716241");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1501", "12714153", "12714252");

			// run 2 to update one document
			Thread.sleep(100);
			Date dateStartRun2 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(PROJECT_KEY, false, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(PROJECT_KEY, 0, lastIssueUpdatedDate2)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501-updated"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(PROJECT_KEY), Mockito.eq(0),
							Mockito.eq(DateTimeUtils.parseISODateTime("2012-09-06T03:28:22.000-0400")))).thenReturn(
					new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(1, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(false, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate3 = DateTimeUtils.parseISODateTime("2012-09-06T03:28:21.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate3, tested.readLastDocumentUpdatedDate(PROJECT_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714252", "12716241", "12714253");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1501");
			// note comment "12714252" was removed and "12714253" was added in "ORG-1501-updated", but incremental update do
			// not remove comments from index
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_COMMENT, dateStartRun2, "12714153", "12714253");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1513", "ORG-1514");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_COMMENT, dateStartRun2, "12716241", "12714252");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1513");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1514", "12716241");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1501", "12714153", "12714252", "12714253");

		} finally {
			finalizeESClientForUnitTest();
		}

	}

	@Test
	public void fullUpdateCommentsEMBEDDED() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.EMBEDDED;
			initIndexStructures(client, structureBuilder.commentIndexingMode);
			initDocumentsForProjectAAA(remoteRiverMock, structureBuilder);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer(PROJECT_KEY, true,
					remoteClientMock, remoteRiverMock, structureBuilder);
			Date lastIssueUpdatedDate = DateTimeUtils.parseISODateTime("2012-09-06T02:26:53.000-0400");
			tested.storeLastDocumentUpdatedDate(null, PROJECT_KEY, lastIssueUpdatedDate);
			when(remoteClientMock.getChangedDocuments(PROJECT_KEY, 0, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));
			when(remoteClientMock.getChangedDocuments(Mockito.eq(PROJECT_KEY), Mockito.eq(0), (Date) Mockito.notNull()))
					.thenReturn(new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate2 = DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate2, tested.readLastDocumentUpdatedDate(PROJECT_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514", "AAA-1", "AAA-2");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun1, "ORG-1501", "ORG-1513", "ORG-1514");

			// run 2 to update one document
			Thread.sleep(100);
			Date dateStartRun2 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(PROJECT_KEY, true, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(PROJECT_KEY, 0, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1513", "ORG-1501-updated"));
			when(remoteClientMock.getChangedDocuments(Mockito.eq(PROJECT_KEY), Mockito.eq(0), (Date) Mockito.notNull()))
					.thenReturn(new ChangedDocumentsResults(null, 0, null));
			tested.run();

			Assert.assertEquals(2, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(1, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate3 = DateTimeUtils.parseISODateTime("2012-09-06T03:28:21.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate3, tested.readLastDocumentUpdatedDate(PROJECT_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "AAA-1", "AAA-2");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1501", "ORG-1513");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "AAA-1", "AAA-2");

		} finally {
			finalizeESClientForUnitTest();
		}
	}

	@Test
	public void fullUpdateCommentsCHILD() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.CHILD;
			initIndexStructures(client, structureBuilder.commentIndexingMode);
			initDocumentsForProjectAAA(remoteRiverMock, structureBuilder);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer(PROJECT_KEY, true,
					remoteClientMock, remoteRiverMock, structureBuilder);
			Date lastIssueUpdatedDate = DateTimeUtils.parseISODateTime("2012-09-06T02:26:53.000-0400");
			tested.storeLastDocumentUpdatedDate(null, PROJECT_KEY, lastIssueUpdatedDate);
			when(remoteClientMock.getChangedDocuments(PROJECT_KEY, 0, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));
			when(remoteClientMock.getChangedDocuments(Mockito.eq(PROJECT_KEY), Mockito.eq(0), (Date) Mockito.notNull()))
					.thenReturn(new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate2 = DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate2, tested.readLastDocumentUpdatedDate(PROJECT_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514", "AAA-1", "AAA-2");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714252", "12716241");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun1, "ORG-1501", "ORG-1513", "ORG-1514");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1513");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1514", "12716241");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1501", "12714153", "12714252");

			// run 2 to update one document
			Thread.sleep(100);
			Date dateStartRun2 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(PROJECT_KEY, true, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(PROJECT_KEY, 0, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1513", "ORG-1501-updated"));
			when(remoteClientMock.getChangedDocuments(Mockito.eq(PROJECT_KEY), Mockito.eq(0), (Date) Mockito.notNull()))
					.thenReturn(new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(2, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(1, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(2, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate3 = DateTimeUtils.parseISODateTime("2012-09-06T03:28:21.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate3, tested.readLastDocumentUpdatedDate(PROJECT_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "AAA-1", "AAA-2");
			// note comment "12714252" was removed and "12714253" was added in "ORG-1501-updated"
			// comment 12716241 removed due "ORG-1514" remove
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714253");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1501", "ORG-1513");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_COMMENT, dateStartRun2, "12714153", "12714253");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "AAA-1", "AAA-2");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_COMMENT, dateStartRun2);
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1513");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1501", "12714153", "12714253");

		} finally {
			finalizeESClientForUnitTest();
		}

	}

	@Test
	public void fullUpdateCommentsSTANDALONE() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.STANDALONE;
			initIndexStructures(client, structureBuilder.commentIndexingMode);
			initDocumentsForProjectAAA(remoteRiverMock, structureBuilder);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer(PROJECT_KEY, true,
					remoteClientMock, remoteRiverMock, structureBuilder);
			Date lastIssueUpdatedDate = DateTimeUtils.parseISODateTime("2012-09-06T02:26:53.000-0400");
			tested.storeLastDocumentUpdatedDate(null, PROJECT_KEY, lastIssueUpdatedDate);
			when(remoteClientMock.getChangedDocuments(PROJECT_KEY, 0, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));
			when(remoteClientMock.getChangedDocuments(Mockito.eq(PROJECT_KEY), Mockito.eq(0), (Date) Mockito.notNull()))
					.thenReturn(new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate2 = DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate2, tested.readLastDocumentUpdatedDate(PROJECT_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514", "AAA-1", "AAA-2");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714252", "12716241");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun1, "ORG-1501", "ORG-1513", "ORG-1514");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1513");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1514");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1501");

			// run 2 to update one document
			Thread.sleep(100);
			Date dateStartRun2 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(PROJECT_KEY, true, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(PROJECT_KEY, 0, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1513", "ORG-1501-updated"));
			when(remoteClientMock.getChangedDocuments(Mockito.eq(PROJECT_KEY), Mockito.eq(0), (Date) Mockito.notNull()))
					.thenReturn(new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(2, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(1, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(2, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate3 = DateTimeUtils.parseISODateTime("2012-09-06T03:28:21.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate3, tested.readLastDocumentUpdatedDate(PROJECT_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "AAA-1", "AAA-2");
			// note comment "12714252" was removed and "12714253" was added in "ORG-1501-updated"
			// comment 12716241 removed due "ORG-1514" remove
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714253");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1501", "ORG-1513");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_COMMENT, dateStartRun2, "12714153", "12714253");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "AAA-1", "AAA-2");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_COMMENT, dateStartRun2);
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1513");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1501");

		} finally {
			finalizeESClientForUnitTest();
		}

	}

	/**
	 * Assert documents with given id's exists in {@value #CFG_INDEX_NAME} search index and no any other exists here.
	 * 
	 * @param client to be used
	 * @param documentType type of document to check
	 * @param documentIds list of document id's to check
	 * 
	 */
	protected void assertDocumentsInIndex(Client client, String documentType, String... documentIds) {

		SearchRequestBuilder srb = client.prepareSearch(CFG_INDEX_NAME).setTypes(documentType)
				.setQuery(QueryBuilders.matchAllQuery());

		assertImplSearchResults(client, srb, documentIds);
	}

	/**
	 * Assert child documents with given id's exists in {@value #CFG_INDEX_NAME} search index for given parent, and no any
	 * other exists here.
	 * 
	 * @param client to be used
	 * @param documentType type of document to check
	 * @param parentDocumentId id of parent to check childs for
	 * @param childDocumentIds list of document id's to check
	 * 
	 */
	protected void assertChildDocumentsInIndex(Client client, String documentType, String parentDocumentId,
			String... childDocumentIds) {

		SearchRequestBuilder srb = client.prepareSearch(CFG_INDEX_NAME).setTypes(documentType)
				.setQuery(QueryBuilders.matchAllQuery());
		srb.setPostFilter(FilterBuilders.termFilter("_parent", parentDocumentId));

		assertImplSearchResults(client, srb, childDocumentIds);
	}

	private void assertImplSearchResults(Client client, SearchRequestBuilder srb, String... documentIds) {
		client.admin().indices().prepareRefresh(CFG_INDEX_NAME).execute().actionGet();

		SearchResponse resp = srb.execute().actionGet();
		List<String> expected = Arrays.asList(documentIds);
		Assert.assertEquals("Documents number is wrong", expected.size(), resp.getHits().getTotalHits());
		for (SearchHit hit : resp.getHits().getHits()) {
			Assert.assertTrue("Document list can't contain document with id " + hit.id(), expected.contains(hit.id()));
		}
	}

	/**
	 * Assert number of documents of given type in search index.
	 * 
	 * @param client to be used
	 * @param indexName name of index to check documents in
	 * @param documentType type of document to check
	 * @param expectedNum expected number of documents
	 * 
	 */
	protected void assertNumDocumentsInIndex(Client client, String indexName, String documentType, int expectedNum) {

		client.admin().indices().prepareRefresh(indexName).execute().actionGet();

		SearchRequestBuilder srb = client.prepareSearch(indexName).setTypes(documentType)
				.setQuery(QueryBuilders.matchAllQuery());

		SearchResponse resp = srb.execute().actionGet();
		Assert.assertEquals("Documents number is wrong", expectedNum, resp.getHits().getTotalHits());
	}

	/**
	 * Assert documents with given id's exists in {@value #CFG_INDEX_NAME} search index and was NOT updated after given
	 * bound date (so was updated before).
	 * 
	 * @param client to be used
	 * @param documentType type of document to check
	 * @param boundDate bound date for check
	 * @param documentIds list of document id's to check
	 * 
	 */
	protected void assertDocumentsUpdatedBeforeDate(Client client, String documentType, Date boundDate,
			String... documentIds) {
		assertImplDocumentsUpdatedDate(client, documentType, boundDate, true, documentIds);
	}

	/**
	 * Assert documents with given id's exists in {@value #CFG_INDEX_NAME} search index and was updated after given bound
	 * date.
	 * 
	 * @param client to be used
	 * @param documentType type of document to check
	 * @param boundDate bound date for check
	 * @param documentIds list of document id's to check
	 * 
	 */
	protected void assertDocumentsUpdatedAfterDate(Client client, String documentType, Date boundDate,
			String... documentIds) {
		assertImplDocumentsUpdatedDate(client, documentType, boundDate, false, documentIds);
	}

	private void assertImplDocumentsUpdatedDate(Client client, String documentType, Date boundDate, boolean beforeDate,
			String... documentIds) {
		FilterBuilder filterTime = null;
		if (beforeDate) {
			filterTime = FilterBuilders.rangeFilter("_timestamp").lt(boundDate);
		} else {
			filterTime = FilterBuilders.rangeFilter("_timestamp").gte(boundDate);
		}
		SearchRequestBuilder srb = client.prepareSearch(CFG_INDEX_NAME).setTypes(documentType)
				.setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filterTime);

		assertImplSearchResults(client, srb, documentIds);
	}

	protected ChangedDocumentsResults prepareChangedDocumentsCallResults(String... issueKeys) throws IOException {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		if (issueKeys != null) {
			for (String key : issueKeys) {
				list.add(TestUtils.readDocumentJsonDataFromClasspathFile(key));
			}
		}
		return new ChangedDocumentsResults(list, 0, null);
	}

	protected RemoteRiver initRiverInstanceForTest(Client client) throws Exception {
		Map<String, Object> settings = new HashMap<String, Object>();
		Settings gs = mock(Settings.class);
		RiverSettings rs = new RiverSettings(gs, settings);
		RemoteRiver tested = new RemoteRiver(new RiverName("remote", CFG_RIVER_NAME), rs);
		tested.client = client;
		IRemoteSystemClient jClientMock = mock(IRemoteSystemClient.class);
		tested.remoteSystemClient = jClientMock;
		// simulate started river
		tested.closed = false;
		return tested;
	}

	protected void initIndexStructures(Client client, CommentIndexingMode commentMode) throws Exception {
		indexCreate(CFG_INDEX_NAME);
		indexCreate(CFG_INDEX_NAME_ACTIVITY);
		client.admin().indices().preparePutMapping(CFG_INDEX_NAME).setType(CFG_TYPE_DOCUMENT)
				.setSource(TestUtils.readStringFromClasspathFile("/mappings/jira_issue.json")).execute().actionGet();
		if (commentMode.isExtraDocumentIndexed()) {
			String commentMappingFilePath = "/mappings/jira_issue_comment.json";
			if (commentMode == CommentIndexingMode.CHILD)
				commentMappingFilePath = "/mappings/jira_issue_comment-child.json";

			client.admin().indices().preparePutMapping(CFG_INDEX_NAME).setType(CFG_TYPE_COMMENT)
					.setSource(TestUtils.readStringFromClasspathFile(commentMappingFilePath)).execute().actionGet();
		}
	}

	/**
	 * Adds two issues from <code>AAA</code> space into search index for tests - keys <code>AAA-1</code> and
	 * <code>AAA-2</code>
	 * 
	 * @param remoteRiverMock to be used
	 * @param structureBuilder to be used
	 * @throws Exception
	 */
	protected void initDocumentsForProjectAAA(RemoteRiver remoteRiverMock,
			DocumentWithCommentsIndexStructureBuilder structureBuilder) throws Exception {
		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer("AAA", true, remoteClientMock,
				remoteRiverMock, structureBuilder);
		ChangedDocumentsResults changedIssues = prepareChangedDocumentsCallResults("AAA-1", "AAA-2");
		when(remoteClientMock.getChangedDocuments("AAA", 0, null)).thenReturn(changedIssues);
		when(remoteClientMock.getChangedDocuments(Mockito.eq("AAA"), Mockito.eq(0), (Date) Mockito.notNull())).thenReturn(
				new ChangedDocumentsResults(null, 0, null));
		tested.run();
		remoteRiverMock.refreshSearchIndex(CFG_INDEX_NAME);
	}

}

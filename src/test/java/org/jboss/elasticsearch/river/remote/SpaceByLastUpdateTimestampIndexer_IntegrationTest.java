/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.Date;

import junit.framework.Assert;

import org.elasticsearch.client.Client;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

/**
 * Unit test for {@link SpaceByLastUpdateTimestampIndexer} which tests search index update processes against embedded
 * inmemmory elastic search node.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceByLastUpdateTimestampIndexer_IntegrationTest extends SpaceIndexer_IntegrationTestBase {

	@Test
	public void incrementalUpdateCommentsEMBEDDED() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client, SpaceIndexingMode.UPDATE_TIMESTAMP);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;
			remoteRiverMock.activityLogIndexName = CFG_INDEX_NAME_ACTIVITY;
			remoteRiverMock.activityLogTypeName = CFG_TYPE_ACTIVITY;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.EMBEDDED;

			initIndexStructures(client, structureBuilder.commentIndexingMode);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, false,
					remoteClientMock, remoteRiverMock, structureBuilder);
			Date lastIssueUpdatedDate = DateTimeUtils.parseISODateTime("2012-09-06T02:26:53.000-0400");
			tested.storeLastDocumentUpdatedDate(null, SPACE_KEY, lastIssueUpdatedDate);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, false, lastIssueUpdatedDate)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(false),
							Mockito.eq(DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400")))).thenReturn(
					new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(0, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(false, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate2 = DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate2, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun1, "ORG-1501", "ORG-1513", "ORG-1514");
			assertNumDocumentsInIndex(client, CFG_INDEX_NAME_ACTIVITY, CFG_TYPE_ACTIVITY, 1);

			// run 2 to update one document
			Thread.sleep(100);
			Date dateStartRun2 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, false, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, false, lastIssueUpdatedDate2)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501-updated"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(false),
							Mockito.eq(DateTimeUtils.parseISODateTime("2012-09-06T03:28:22.000-0400")))).thenReturn(
					new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(1, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(0, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(false, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate3 = DateTimeUtils.parseISODateTime("2012-09-06T03:28:21.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate3, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1501");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1513", "ORG-1514");

			assertNumDocumentsInIndex(client, CFG_INDEX_NAME_ACTIVITY, CFG_TYPE_ACTIVITY, 2);

			// run 3 to delete one document
			Thread.sleep(100);
			Date dateStartRun3 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, false, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, false, lastIssueUpdatedDate3)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1513-deleted"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(false),
							Mockito.eq(DateTimeUtils.parseISODateTime("2012-09-06T06:52:06.000-0400")))).thenReturn(
					new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(0, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(1, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(0, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(false, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate4 = DateTimeUtils.parseISODateTime("2012-09-06T06:52:05.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate4, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1514");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun3);
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun3, "ORG-1501", "ORG-1514");

			assertNumDocumentsInIndex(client, CFG_INDEX_NAME_ACTIVITY, CFG_TYPE_ACTIVITY, 3);

		} finally {
			finalizeESClientForUnitTest();
		}
	}

	@Test
	public void incrementalUpdateCommentsCHILD() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client, SpaceIndexingMode.UPDATE_TIMESTAMP);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.CHILD;
			initIndexStructures(client, structureBuilder.commentIndexingMode);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, false,
					remoteClientMock, remoteRiverMock, structureBuilder);
			Date lastIssueUpdatedDate = DateTimeUtils.parseISODateTime("2012-09-06T02:26:53.000-0400");
			tested.storeLastDocumentUpdatedDate(null, SPACE_KEY, lastIssueUpdatedDate);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, false, lastIssueUpdatedDate)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(false),
							Mockito.eq(DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400")))).thenReturn(
					new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(false, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate2 = DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate2, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714252", "12716241", "12716289");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun1, "ORG-1501", "ORG-1513", "ORG-1514");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1513", "12716289");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1514", "12716241");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1501", "12714153", "12714252");

			// run 2 to update one document
			Thread.sleep(100);
			Date dateStartRun2 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, false, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, false, lastIssueUpdatedDate2)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501-updated"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(false),
							Mockito.eq(DateTimeUtils.parseISODateTime("2012-09-06T03:28:22.000-0400")))).thenReturn(
					new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(1, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(false, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate3 = DateTimeUtils.parseISODateTime("2012-09-06T03:28:21.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate3, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714252", "12716241", "12714253", "12716289");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1501");
			// note comment "12714252" was removed and "12714253" was added in "ORG-1501-updated", but incremental update do
			// not remove comments from index
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_COMMENT, dateStartRun2, "12714153", "12714253");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1513", "ORG-1514");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_COMMENT, dateStartRun2, "12716241", "12714252", "12716289");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1513", "12716289");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1514", "12716241");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1501", "12714153", "12714252", "12714253");

			// run 3 to delete one document
			Thread.sleep(100);
			Date dateStartRun3 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, false, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, false, lastIssueUpdatedDate3)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1513-deleted"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(false),
							Mockito.eq(DateTimeUtils.parseISODateTime("2012-09-06T06:52:06.000-0400")))).thenReturn(
					new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(0, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(1, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(1, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(false, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate4 = DateTimeUtils.parseISODateTime("2012-09-06T06:52:05.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate4, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1514");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714252", "12716241", "12714253");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun3);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_COMMENT, dateStartRun3);
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun3, "ORG-1501", "ORG-1514");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_COMMENT, dateStartRun3, "12714153", "12714252", "12716241",
					"12714253");

		} finally {
			finalizeESClientForUnitTest();
		}

	}

	@Test
	public void fullUpdateCommentsEMBEDDED() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client, SpaceIndexingMode.UPDATE_TIMESTAMP);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.EMBEDDED;
			initIndexStructures(client, structureBuilder.commentIndexingMode);
			initDocumentsForProjectAAA(remoteRiverMock, structureBuilder);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, true,
					remoteClientMock, remoteRiverMock, structureBuilder);
			Date lastIssueUpdatedDate = DateTimeUtils.parseISODateTime("2012-09-06T02:26:53.000-0400");
			tested.storeLastDocumentUpdatedDate(null, SPACE_KEY, lastIssueUpdatedDate);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(true),
							(Date) Mockito.notNull())).thenReturn(new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate2 = DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate2, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514", "AAA-1", "AAA-2");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun1, "ORG-1501", "ORG-1513", "ORG-1514");

			// run 2 to update one document and delete one flagged document and one disappeared document
			Thread.sleep(100);
			Date dateStartRun2 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, true, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1513-deleted", "ORG-1501-updated"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(true),
							(Date) Mockito.notNull())).thenReturn(new ChangedDocumentsResults(null, 0, null));
			tested.run();

			Assert.assertEquals(1, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(2, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(0, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate3 = DateTimeUtils.parseISODateTime("2012-09-06T03:28:21.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate3, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "AAA-1", "AAA-2");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1501");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "AAA-1", "AAA-2");

		} finally {
			finalizeESClientForUnitTest();
		}
	}

	@Test
	public void fullUpdateCommentsCHILD() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client, SpaceIndexingMode.UPDATE_TIMESTAMP);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.CHILD;
			initIndexStructures(client, structureBuilder.commentIndexingMode);
			initDocumentsForProjectAAA(remoteRiverMock, structureBuilder);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, true,
					remoteClientMock, remoteRiverMock, structureBuilder);
			Date lastIssueUpdatedDate = DateTimeUtils.parseISODateTime("2012-09-06T02:26:53.000-0400");
			tested.storeLastDocumentUpdatedDate(null, SPACE_KEY, lastIssueUpdatedDate);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(true),
							(Date) Mockito.notNull())).thenReturn(new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate2 = DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate2, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514", "AAA-1", "AAA-2");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714252", "12716241", "12716289");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun1, "ORG-1501", "ORG-1513", "ORG-1514");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1513", "12716289");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1514", "12716241");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1501", "12714153", "12714252");

			// run 2 to update one document, delete one disappeared and one flagged
			Thread.sleep(100);
			Date dateStartRun2 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, true, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1513-deleted", "ORG-1501-updated"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(true),
							(Date) Mockito.notNull())).thenReturn(new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(1, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(2, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(3, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate3 = DateTimeUtils.parseISODateTime("2012-09-06T03:28:21.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate3, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "AAA-1", "AAA-2");
			// note comment "12714252" was removed and "12714253" was added in "ORG-1501-updated"
			// comment 12716241 removed due "ORG-1514" remove
			// comment "12716289" removed due "ORG-1513" remove
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714253");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1501");
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

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client, SpaceIndexingMode.UPDATE_TIMESTAMP);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.STANDALONE;
			initIndexStructures(client, structureBuilder.commentIndexingMode);
			initDocumentsForProjectAAA(remoteRiverMock, structureBuilder);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceByLastUpdateTimestampIndexer tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, true,
					remoteClientMock, remoteRiverMock, structureBuilder);
			Date lastIssueUpdatedDate = DateTimeUtils.parseISODateTime("2012-09-06T02:26:53.000-0400");
			tested.storeLastDocumentUpdatedDate(null, SPACE_KEY, lastIssueUpdatedDate);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(true),
							(Date) Mockito.notNull())).thenReturn(new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate2 = DateTimeUtils.parseISODateTime("2012-09-06T03:27:25.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate2, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514", "AAA-1", "AAA-2");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714252", "12716241", "12716289");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun1, "ORG-1501", "ORG-1513", "ORG-1514");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1513");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1514");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1501");

			// run 2 to update one document, delete one flagged and one disappeared doc (with some comments)
			Thread.sleep(100);
			Date dateStartRun2 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, true, remoteClientMock, remoteRiverMock,
					structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1513-deleted", "ORG-1501-updated"));
			when(
					remoteClientMock.getChangedDocuments(Mockito.eq(SPACE_KEY), Mockito.eq(0), Mockito.eq(true),
							(Date) Mockito.notNull())).thenReturn(new ChangedDocumentsResults(null, 0, null));

			tested.run();

			Assert.assertEquals(1, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(2, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(3, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			Date lastIssueUpdatedDate3 = DateTimeUtils.parseISODateTime("2012-09-06T03:28:21.000-0400");
			Assert.assertEquals(lastIssueUpdatedDate3, tested.readLastDocumentUpdatedDate(SPACE_KEY));

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "AAA-1", "AAA-2");
			// note comment "12714252" was removed and "12714253" was added in "ORG-1501-updated"
			// comment 12716241 removed due "ORG-1514" remove
			// comment "12716289" removed due "ORG-1513" remove
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT, "12714153", "12714253");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1501");
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_COMMENT, dateStartRun2, "12714153", "12714253");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "AAA-1", "AAA-2");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_COMMENT, dateStartRun2);
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1513");
			assertChildDocumentsInIndex(client, CFG_TYPE_COMMENT, "ORG-1501");

		} finally {
			finalizeESClientForUnitTest();
		}

	}

}

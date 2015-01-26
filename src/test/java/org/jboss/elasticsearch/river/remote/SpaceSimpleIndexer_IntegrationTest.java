/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.Date;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.client.Client;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

/**
 * Unit test for {@link SpaceSimpleIndexer} which tests search index update processes against embedded inmemmory elastic
 * search node.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceSimpleIndexer_IntegrationTest extends SpaceIndexer_IntegrationTestBase {

	@SuppressWarnings("unchecked")
	@Test
	public void fullUpdateCommentsEMBEDDED() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client, SpaceIndexingMode.SIMPLE);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.EMBEDDED;
			initIndexStructures(client, structureBuilder.commentIndexingMode);
			initDocumentsForProjectAAA(remoteRiverMock, structureBuilder);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceSimpleIndexer tested = new SpaceSimpleIndexer(SPACE_KEY, remoteClientMock, remoteRiverMock, structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));

			tested.run();

			Mockito.verify(remoteClientMock).getChangedDocuments(SPACE_KEY, 0, true, null);
			Mockito.verify(remoteClientMock, Mockito.times(3)).getChangedDocumentDetails(Mockito.eq(SPACE_KEY),
					(String) Mockito.notNull(), (Map<String, Object>) Mockito.notNull());
			Mockito.verifyNoMoreInteractions(remoteClientMock);

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "ORG-1513", "ORG-1514", "AAA-1", "AAA-2");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun1, "ORG-1501", "ORG-1513", "ORG-1514");

			// run 2 to update one document and delete one flagged document and one disappeared document
			Thread.sleep(100);
			Date dateStartRun2 = new Date();
			Mockito.reset(remoteClientMock);
			tested = new SpaceSimpleIndexer(SPACE_KEY, remoteClientMock, remoteRiverMock, structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1513-deleted", "ORG-1501-updated"));

			tested.run();

			Mockito.verify(remoteClientMock).getChangedDocuments(SPACE_KEY, 0, true, null);
			Mockito.verify(remoteClientMock, Mockito.times(2)).getChangedDocumentDetails(Mockito.eq(SPACE_KEY),
					(String) Mockito.notNull(), (Map<String, Object>) Mockito.notNull());
			Mockito.verifyNoMoreInteractions(remoteClientMock);

			Assert.assertEquals(1, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(2, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(0, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

			assertDocumentsInIndex(client, CFG_TYPE_DOCUMENT, "ORG-1501", "AAA-1", "AAA-2");
			assertDocumentsInIndex(client, CFG_TYPE_COMMENT);
			assertDocumentsUpdatedAfterDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "ORG-1501");
			assertDocumentsUpdatedBeforeDate(client, CFG_TYPE_DOCUMENT, dateStartRun2, "AAA-1", "AAA-2");

		} finally {
			finalizeESClientForUnitTest();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void fullUpdateCommentsCHILD() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client, SpaceIndexingMode.SIMPLE);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.CHILD;
			initIndexStructures(client, structureBuilder.commentIndexingMode);
			initDocumentsForProjectAAA(remoteRiverMock, structureBuilder);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceSimpleIndexer tested = new SpaceSimpleIndexer(SPACE_KEY, remoteClientMock, remoteRiverMock, structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));

			tested.run();

			Mockito.verify(remoteClientMock).getChangedDocuments(SPACE_KEY, 0, true, null);
			Mockito.verify(remoteClientMock, Mockito.times(3)).getChangedDocumentDetails(Mockito.eq(SPACE_KEY),
					(String) Mockito.notNull(), (Map<String, Object>) Mockito.notNull());
			Mockito.verifyNoMoreInteractions(remoteClientMock);

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

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
			tested = new SpaceSimpleIndexer(SPACE_KEY, remoteClientMock, remoteRiverMock, structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1513-deleted", "ORG-1501-updated"));

			tested.run();

			Mockito.verify(remoteClientMock).getChangedDocuments(SPACE_KEY, 0, true, null);
			Mockito.verify(remoteClientMock, Mockito.times(2)).getChangedDocumentDetails(Mockito.eq(SPACE_KEY),
					(String) Mockito.notNull(), (Map<String, Object>) Mockito.notNull());
			Mockito.verifyNoMoreInteractions(remoteClientMock);

			Assert.assertEquals(1, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(2, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(3, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

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

	@SuppressWarnings("unchecked")
	@Test
	public void fullUpdateCommentsSTANDALONE() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver remoteRiverMock = initRiverInstanceForTest(client, SpaceIndexingMode.SIMPLE);
			IRemoteSystemClient remoteClientMock = remoteRiverMock.remoteSystemClient;

			DocumentWithCommentsIndexStructureBuilder structureBuilder = prepareStructureBuilder();
			structureBuilder.commentIndexingMode = CommentIndexingMode.STANDALONE;
			initIndexStructures(client, structureBuilder.commentIndexingMode);
			initDocumentsForProjectAAA(remoteRiverMock, structureBuilder);

			// run 1 to insert documents
			Date dateStartRun1 = new Date();
			SpaceSimpleIndexer tested = new SpaceSimpleIndexer(SPACE_KEY, remoteClientMock, remoteRiverMock, structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1501", "ORG-1513", "ORG-1514"));

			tested.run();

			Mockito.verify(remoteClientMock).getChangedDocuments(SPACE_KEY, 0, true, null);
			Mockito.verify(remoteClientMock, Mockito.times(3)).getChangedDocumentDetails(Mockito.eq(SPACE_KEY),
					(String) Mockito.notNull(), (Map<String, Object>) Mockito.notNull());
			Mockito.verifyNoMoreInteractions(remoteClientMock);

			Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(0, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

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
			tested = new SpaceSimpleIndexer(SPACE_KEY, remoteClientMock, remoteRiverMock, structureBuilder);
			when(remoteClientMock.getChangedDocuments(SPACE_KEY, 0, true, null)).thenReturn(
					prepareChangedDocumentsCallResults("ORG-1513-deleted", "ORG-1501-updated"));

			tested.run();

			Mockito.verify(remoteClientMock).getChangedDocuments(SPACE_KEY, 0, true, null);
			Mockito.verify(remoteClientMock, Mockito.times(2)).getChangedDocumentDetails(Mockito.eq(SPACE_KEY),
					(String) Mockito.notNull(), (Map<String, Object>) Mockito.notNull());
			Mockito.verifyNoMoreInteractions(remoteClientMock);

			Assert.assertEquals(1, tested.indexingInfo.documentsUpdated);
			Assert.assertEquals(2, tested.indexingInfo.documentsDeleted);
			Assert.assertEquals(3, tested.indexingInfo.commentsDeleted);
			Assert.assertEquals(true, tested.indexingInfo.fullUpdate);
			Assert.assertNotNull(tested.startTime);

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

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
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.SearchHit;
import org.jboss.elasticsearch.river.remote.testtools.ESRealClientTestBase;
import org.jboss.elasticsearch.river.remote.testtools.TestUtils;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class with helpers for unit tests of Space indexers which test search index update processes against embedded
 * inmemmory elastic search node.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceIndexer_IntegrationTestBase extends ESRealClientTestBase {

	protected static final String CFG_RIVER_NAME = "remote_river";
	protected static final String CFG_INDEX_NAME = CFG_RIVER_NAME;
	protected static final String CFG_TYPE_DOCUMENT = "jira_issue";
	protected static final String CFG_TYPE_COMMENT = "jira_issue_comment";

	protected static final String CFG_TYPE_ACTIVITY = "jira_river_indexupdate";
	protected static final String CFG_INDEX_NAME_ACTIVITY = "activity_index";

	protected static final String SPACE_KEY = "ORG";

	protected DocumentWithCommentsIndexStructureBuilder prepareStructureBuilder() {
		DocumentWithCommentsIndexStructureBuilder structureBuilder = new DocumentWithCommentsIndexStructureBuilder(
				mockEsIntegrationComponent(), CFG_INDEX_NAME, CFG_TYPE_DOCUMENT,
				DocumentWithCommentsIndexStructureBuilderTest.createSettingsWithMandatoryFilled(), true);
		structureBuilder.remoteDataFieldForDocumentId = "key";
		structureBuilder.remoteDataFieldForUpdated = "fields.updated";
		structureBuilder.remoteDataFieldForDeleted = "fields.deleted";
		structureBuilder.remoteDataValueForDeleted = "true";
		structureBuilder.remoteDataFieldForComments = "fields.comment.comments";
		structureBuilder.remoteDataFieldForCommentId = "id";
		structureBuilder.indexFieldForComments = "comments";
		structureBuilder.commentTypeName = CFG_TYPE_COMMENT;
		structureBuilder.commentFieldsConfig = new HashMap<String, Map<String, String>>();
		return structureBuilder;
	}

	protected IESIntegration mockEsIntegrationComponent() {
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		Mockito.when(esIntegrationMock.createLogger(Mockito.any(Class.class))).thenReturn(
				ESLoggerFactory.getLogger(SpaceIndexerCoordinator.class.getName()));
		RiverName riverName = new RiverName("remote", CFG_RIVER_NAME);
		Mockito.when(esIntegrationMock.riverName()).thenReturn(riverName);
		return esIntegrationMock;
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

	protected void assertImplSearchResults(Client client, SearchRequestBuilder srb, String... documentIds) {
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

	protected void assertImplDocumentsUpdatedDate(Client client, String documentType, Date boundDate, boolean beforeDate,
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
		return prepareChangedDocumentsCallResults(false, issueKeys);
	}

	protected ChangedDocumentsResults prepareChangedDocumentsCallResults(boolean setTotal, String... issueKeys)
			throws IOException {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		if (issueKeys != null) {
			for (String key : issueKeys) {
				list.add(TestUtils.readDocumentJsonDataFromClasspathFile(key));
			}
		}
		return new ChangedDocumentsResults(list, 0, setTotal ? issueKeys.length : null);
	}

	protected RemoteRiver initRiverInstanceForTest(Client client, SpaceIndexingMode spaceIndexingMode) throws Exception {
		Map<String, Object> settings = new HashMap<String, Object>();
		Settings gs = mock(Settings.class);
		RiverSettings rs = new RiverSettings(gs, settings);
		RemoteRiver tested = new RemoteRiver(new RiverName("remote", CFG_RIVER_NAME), rs);
		tested.client = client;
		tested.spaceIndexingMode = spaceIndexingMode;
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

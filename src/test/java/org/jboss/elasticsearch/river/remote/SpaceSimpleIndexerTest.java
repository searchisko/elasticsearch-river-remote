/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.jboss.elasticsearch.river.remote.exception.RemoteDocumentNotFoundException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.eq;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link SpaceSimpleIndexer}
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceSimpleIndexerTest {

	@Test
	public void init() {
		IRemoteSystemClient remoteClient = new GetJSONClient();
		IDocumentIndexStructureBuilder documentIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpaceSimpleIndexer tested = new SpaceSimpleIndexer("ORG", remoteClient, null, documentIndexStructureBuilderMock);
		Assert.assertEquals("ORG", tested.spaceKey);
		Assert.assertTrue(tested.indexingInfo.fullUpdate);
		Assert.assertEquals(remoteClient, tested.remoteSystemClient);
		Assert.assertEquals(documentIndexStructureBuilderMock, tested.documentIndexStructureBuilder);
		Assert.assertNotNull(tested.logger);
	}

	@Test
	public void processUpdate_emptyList() throws Exception {

		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder documentIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpaceSimpleIndexer tested = new SpaceSimpleIndexer("ORG", remoteClientMock, esIntegrationMock,
				documentIndexStructureBuilderMock);

		List<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();

		// test case with empty result list from remote system search method
		when(remoteClientMock.getChangedDocuments("ORG", 0, null)).thenReturn(new ChangedDocumentsResults(docs, 0, 0));

		tested.processUpdate();
		Assert.assertEquals(0, tested.getIndexingInfo().documentsUpdated);
		Assert.assertEquals(0, tested.indexingInfo.documentsWithError);
		Assert.assertTrue(tested.getIndexingInfo().fullUpdate);
		verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 0, null);
		verify(esIntegrationMock, times(0)).prepareESBulkRequestBuilder();
		verify(esIntegrationMock, times(0)).executeESBulkRequest(Mockito.any(BulkRequestBuilder.class));
		Mockito.verifyNoMoreInteractions(remoteClientMock);
		Mockito.verifyNoMoreInteractions(esIntegrationMock);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void processUpdate_list() throws Exception {

		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder documentIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpaceSimpleIndexer tested = new SpaceSimpleIndexer("ORG", remoteClientMock, esIntegrationMock,
				documentIndexStructureBuilderMock);

		Client client = Mockito.mock(Client.class);

		List<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();

		reset(esIntegrationMock, remoteClientMock, documentIndexStructureBuilderMock);
		// test that bulks are handled correctly
		tested.bulkSize = 2;

		Map<String, Object> doc1 = addDocumentMock(docs, "ORG-45");
		Map<String, Object> doc2 = addDocumentMock(docs, "ORG-46");
		Map<String, Object> doc3 = addDocumentMock(docs, "ORG-47");
		Map<String, Object> doc4 = addDocumentMock(docs, "ORG-48");
		// test skipping of bad read of remote document detail - issue #11
		when(
				remoteClientMock.getChangedDocumentDetails(Mockito.eq("ORG"), Mockito.eq("ORG-46"),
						(Map<String, Object>) Mockito.notNull())).thenThrow(new RemoteDocumentNotFoundException());
		configureStructureBuilderMockDefaults(documentIndexStructureBuilderMock);
		when(remoteClientMock.getChangedDocuments("ORG", 0, null)).thenReturn(new ChangedDocumentsResults(docs, 0, 3));
		BulkRequestBuilder brb = new BulkRequestBuilder(client);
		when(esIntegrationMock.prepareESBulkRequestBuilder()).thenReturn(brb);

		tested.processUpdate();
		Assert.assertEquals(3, tested.indexingInfo.documentsUpdated);
		Assert.assertEquals(1, tested.indexingInfo.documentsWithError);
		Assert.assertTrue(tested.indexingInfo.fullUpdate);
		verify(remoteClientMock, times(1)).getChangedDocuments("ORG", 0, null);
		verify(esIntegrationMock, times(2)).prepareESBulkRequestBuilder();
		verify(documentIndexStructureBuilderMock, times(3)).indexDocument(Mockito.eq(brb), Mockito.eq("ORG"),
				Mockito.any(Map.class));
		verify(esIntegrationMock, times(2)).executeESBulkRequest(eq(brb));
		verify(esIntegrationMock, Mockito.atLeastOnce()).isClosed();

		verify(remoteClientMock).getChangedDocumentDetails("ORG", "ORG-45", doc1);
		verify(remoteClientMock).getChangedDocumentDetails("ORG", "ORG-46", doc2);
		verify(remoteClientMock).getChangedDocumentDetails("ORG", "ORG-47", doc3);
		verify(remoteClientMock).getChangedDocumentDetails("ORG", "ORG-48", doc4);
		Mockito.verifyNoMoreInteractions(remoteClientMock);
		Mockito.verifyNoMoreInteractions(esIntegrationMock);
	}

	/**
	 * Add document info structure into list of documents. Used to build mock {@link ChangedDocumentsResults} instances.
	 * 
	 * @param documents list to add document into
	 * @param key of document
	 * @return document Map structure
	 */
	public static Map<String, Object> addDocumentMock(List<Map<String, Object>> documents, String key) {
		Map<String, Object> doc = new HashMap<String, Object>();
		documents.add(doc);
		doc.put("key", key);
		return doc;
	}

	/**
	 * @param documentIndexStructureBuilderMock
	 */
	@SuppressWarnings("unchecked")
	protected static void configureStructureBuilderMockDefaults(
			IDocumentIndexStructureBuilder documentIndexStructureBuilderMock) {
		when(documentIndexStructureBuilderMock.extractDocumentId(Mockito.anyMap())).thenAnswer(new Answer<String>() {
			public String answer(InvocationOnMock invocation) throws Throwable {
				return (String) ((Map<String, Object>) invocation.getArguments()[0]).get("key");
			}
		});

	}

}

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.ArrayList;
import java.util.Date;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link SpacePaginatingIndexer}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpacePaginatingIndexerTest {

	@Test
	public void init() {
		IRemoteSystemClient remoteClient = new GetJSONClient();
		IDocumentIndexStructureBuilder documentIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpacePaginatingIndexer tested = new SpacePaginatingIndexer("ORG", remoteClient, null,
				documentIndexStructureBuilderMock);
		Assert.assertEquals("ORG", tested.spaceKey);
		Assert.assertNotNull(tested.indexingInfo);
		Assert.assertTrue(tested.indexingInfo.fullUpdate);
		Assert.assertEquals(remoteClient, tested.remoteSystemClient);
		Assert.assertEquals(documentIndexStructureBuilderMock, tested.documentIndexStructureBuilder);
		Assert.assertNotNull(tested.logger);
	}

	@Test
	public void processUpdate_emptyList() throws Exception {
		SpacePaginatingIndexer tested = getTested();
		configureStructureBuilderMockDefaults(tested.documentIndexStructureBuilder);

		List<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();

		when(tested.remoteSystemClient.getChangedDocuments("ORG", 0, null)).thenReturn(
				new ChangedDocumentsResults(docs, 0, null));

		tested.processUpdate();
		Assert.assertEquals(0, tested.getIndexingInfo().documentsUpdated);
		Assert.assertEquals(0, tested.indexingInfo.documentsWithError);
		Assert.assertTrue(tested.getIndexingInfo().fullUpdate);
		verify(tested.remoteSystemClient, times(1)).getChangedDocuments("ORG", 0, null);
		verify(tested.esIntegrationComponent, times(0)).prepareESBulkRequestBuilder();
		verify(tested.esIntegrationComponent, times(0)).executeESBulkRequest(Mockito.any(BulkRequestBuilder.class));
		verify(tested.esIntegrationComponent, Mockito.atLeastOnce()).isClosed();
		Mockito.verifyNoMoreInteractions(tested.remoteSystemClient);
		Mockito.verifyNoMoreInteractions(tested.esIntegrationComponent);
		Mockito.verifyNoMoreInteractions(tested.documentIndexStructureBuilder);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void processUpdate_onePage_noTotal() throws Exception {
		SpacePaginatingIndexer tested = getTested();
		configureStructureBuilderMockDefaults(tested.documentIndexStructureBuilder);

		List<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();
		addDocumentMock(docs, "AA1");
		addDocumentMock(docs, "AA2");
		addDocumentMock(docs, "AA3");

		when(tested.remoteSystemClient.getChangedDocuments("ORG", 0, null)).thenReturn(
				new ChangedDocumentsResults(docs, 0, null));
		when(tested.remoteSystemClient.getChangedDocuments("ORG", docs.size(), null)).thenReturn(
				new ChangedDocumentsResults(null, docs.size(), null));

		Client client = Mockito.mock(Client.class);
		BulkRequestBuilder brb = new BulkRequestBuilder(client);
		when(tested.esIntegrationComponent.prepareESBulkRequestBuilder()).thenReturn(brb);

		// test skipping of bad read of remote document detail - issue #11
		when(
				tested.remoteSystemClient.getChangedDocumentDetails(Mockito.eq("ORG"), Mockito.eq("AA2"),
						(Map<String, Object>) Mockito.notNull())).thenThrow(new RemoteDocumentNotFoundException());

		tested.processUpdate();
		Assert.assertEquals(2, tested.getIndexingInfo().documentsUpdated);
		Assert.assertEquals(1, tested.indexingInfo.documentsWithError);
		Assert.assertTrue(tested.getIndexingInfo().fullUpdate);
		// next is called twice because no total info is provided
		verify(tested.remoteSystemClient, times(2)).getChangedDocuments(Mockito.eq("ORG"), Mockito.anyInt(),
				Mockito.eq((Date) null));
		verify(tested.remoteSystemClient, times(3)).getChangedDocumentDetails(Mockito.eq("ORG"), Mockito.anyString(),
				Mockito.anyMap());
		verify(tested.documentIndexStructureBuilder, times(3)).extractDocumentId(Mockito.anyMap());
		verify(tested.documentIndexStructureBuilder, times(2)).indexDocument(Mockito.eq(brb), Mockito.eq("ORG"),
				Mockito.any(Map.class));

		verify(tested.esIntegrationComponent, times(1)).prepareESBulkRequestBuilder();
		verify(tested.esIntegrationComponent, times(1)).executeESBulkRequest(Mockito.any(BulkRequestBuilder.class));
		verify(tested.esIntegrationComponent, Mockito.atLeastOnce()).isClosed();
		Mockito.verifyNoMoreInteractions(tested.remoteSystemClient);
		Mockito.verifyNoMoreInteractions(tested.esIntegrationComponent);
		Mockito.verifyNoMoreInteractions(tested.documentIndexStructureBuilder);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void processUpdate_onePage_withTotal() throws Exception {
		SpacePaginatingIndexer tested = getTested();
		configureStructureBuilderMockDefaults(tested.documentIndexStructureBuilder);

		List<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();
		addDocumentMock(docs, "AA1");
		addDocumentMock(docs, "AA2");
		addDocumentMock(docs, "AA3");

		when(tested.remoteSystemClient.getChangedDocuments("ORG", 0, null)).thenReturn(
				new ChangedDocumentsResults(docs, 0, 3));

		Client client = Mockito.mock(Client.class);
		BulkRequestBuilder brb = new BulkRequestBuilder(client);
		when(tested.esIntegrationComponent.prepareESBulkRequestBuilder()).thenReturn(brb);

		// test skipping of bad read of remote document detail - issue #11
		when(
				tested.remoteSystemClient.getChangedDocumentDetails(Mockito.eq("ORG"), Mockito.eq("AA2"),
						(Map<String, Object>) Mockito.notNull())).thenThrow(new RemoteDocumentNotFoundException());

		tested.processUpdate();
		Assert.assertEquals(2, tested.getIndexingInfo().documentsUpdated);
		Assert.assertEquals(1, tested.indexingInfo.documentsWithError);
		Assert.assertTrue(tested.getIndexingInfo().fullUpdate);
		// next is called only once thanks to total info provided
		verify(tested.remoteSystemClient, times(1)).getChangedDocuments(Mockito.eq("ORG"), Mockito.anyInt(),
				Mockito.eq((Date) null));
		verify(tested.remoteSystemClient, times(3)).getChangedDocumentDetails(Mockito.eq("ORG"), Mockito.anyString(),
				Mockito.anyMap());
		verify(tested.documentIndexStructureBuilder, times(3)).extractDocumentId(Mockito.anyMap());
		verify(tested.documentIndexStructureBuilder, times(2)).indexDocument(Mockito.eq(brb), Mockito.eq("ORG"),
				Mockito.any(Map.class));

		verify(tested.esIntegrationComponent, times(1)).prepareESBulkRequestBuilder();
		verify(tested.esIntegrationComponent, times(1)).executeESBulkRequest(Mockito.any(BulkRequestBuilder.class));
		verify(tested.esIntegrationComponent, Mockito.atLeastOnce()).isClosed();
		Mockito.verifyNoMoreInteractions(tested.remoteSystemClient);
		Mockito.verifyNoMoreInteractions(tested.esIntegrationComponent);
		Mockito.verifyNoMoreInteractions(tested.documentIndexStructureBuilder);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void processUpdate_morePages_noTotal() throws Exception {
		SpacePaginatingIndexer tested = getTested();
		configureStructureBuilderMockDefaults(tested.documentIndexStructureBuilder);

		List<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();
		addDocumentMock(docs, "AA1");
		addDocumentMock(docs, "AA2");
		addDocumentMock(docs, "AA3");

		List<Map<String, Object>> docs2 = new ArrayList<Map<String, Object>>();
		addDocumentMock(docs2, "AA4");
		addDocumentMock(docs2, "AA5");

		when(tested.remoteSystemClient.getChangedDocuments("ORG", 0, null)).thenReturn(
				new ChangedDocumentsResults(docs, 0, null));
		when(tested.remoteSystemClient.getChangedDocuments("ORG", docs.size(), null)).thenReturn(
				new ChangedDocumentsResults(docs2, docs.size(), null));
		when(tested.remoteSystemClient.getChangedDocuments("ORG", docs.size() + docs2.size(), null)).thenReturn(
				new ChangedDocumentsResults(null, docs.size() + docs2.size(), null));

		Client client = Mockito.mock(Client.class);
		BulkRequestBuilder brb = new BulkRequestBuilder(client);
		when(tested.esIntegrationComponent.prepareESBulkRequestBuilder()).thenReturn(brb);

		// test skipping of bad read of remote document detail - issue #11
		when(
				tested.remoteSystemClient.getChangedDocumentDetails(Mockito.eq("ORG"), Mockito.eq("AA2"),
						(Map<String, Object>) Mockito.notNull())).thenThrow(new RemoteDocumentNotFoundException());

		tested.processUpdate();
		Assert.assertEquals(4, tested.getIndexingInfo().documentsUpdated);
		Assert.assertEquals(1, tested.indexingInfo.documentsWithError);
		Assert.assertTrue(tested.getIndexingInfo().fullUpdate);
		verify(tested.remoteSystemClient, times(3)).getChangedDocuments(Mockito.eq("ORG"), Mockito.anyInt(),
				Mockito.eq((Date) null));
		verify(tested.remoteSystemClient, times(5)).getChangedDocumentDetails(Mockito.eq("ORG"), Mockito.anyString(),
				Mockito.anyMap());
		verify(tested.documentIndexStructureBuilder, times(5)).extractDocumentId(Mockito.anyMap());
		verify(tested.documentIndexStructureBuilder, times(4)).indexDocument(Mockito.eq(brb), Mockito.eq("ORG"),
				Mockito.any(Map.class));

		verify(tested.esIntegrationComponent, times(2)).prepareESBulkRequestBuilder();
		verify(tested.esIntegrationComponent, times(2)).executeESBulkRequest(Mockito.any(BulkRequestBuilder.class));
		verify(tested.esIntegrationComponent, Mockito.atLeastOnce()).isClosed();
		Mockito.verifyNoMoreInteractions(tested.remoteSystemClient);
		Mockito.verifyNoMoreInteractions(tested.esIntegrationComponent);
		Mockito.verifyNoMoreInteractions(tested.documentIndexStructureBuilder);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void processUpdate_morePages_withTotal() throws Exception {
		SpacePaginatingIndexer tested = getTested();
		configureStructureBuilderMockDefaults(tested.documentIndexStructureBuilder);

		List<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();
		addDocumentMock(docs, "AA1");
		addDocumentMock(docs, "AA2");
		addDocumentMock(docs, "AA3");

		List<Map<String, Object>> docs2 = new ArrayList<Map<String, Object>>();
		addDocumentMock(docs2, "AA4");
		addDocumentMock(docs2, "AA5");

		when(tested.remoteSystemClient.getChangedDocuments("ORG", 0, null)).thenReturn(
				new ChangedDocumentsResults(docs, 0, 5));
		when(tested.remoteSystemClient.getChangedDocuments("ORG", docs.size(), null)).thenReturn(
				new ChangedDocumentsResults(docs2, docs.size(), 5));

		Client client = Mockito.mock(Client.class);
		BulkRequestBuilder brb = new BulkRequestBuilder(client);
		when(tested.esIntegrationComponent.prepareESBulkRequestBuilder()).thenReturn(brb);

		// test skipping of bad read of remote document detail - issue #11
		when(
				tested.remoteSystemClient.getChangedDocumentDetails(Mockito.eq("ORG"), Mockito.eq("AA2"),
						(Map<String, Object>) Mockito.notNull())).thenThrow(new RemoteDocumentNotFoundException());

		tested.processUpdate();
		Assert.assertEquals(4, tested.getIndexingInfo().documentsUpdated);
		Assert.assertEquals(1, tested.indexingInfo.documentsWithError);
		Assert.assertTrue(tested.getIndexingInfo().fullUpdate);
		verify(tested.remoteSystemClient, times(2)).getChangedDocuments(Mockito.eq("ORG"), Mockito.anyInt(),
				Mockito.eq((Date) null));
		verify(tested.remoteSystemClient, times(5)).getChangedDocumentDetails(Mockito.eq("ORG"), Mockito.anyString(),
				Mockito.anyMap());
		verify(tested.documentIndexStructureBuilder, times(5)).extractDocumentId(Mockito.anyMap());
		verify(tested.documentIndexStructureBuilder, times(4)).indexDocument(Mockito.eq(brb), Mockito.eq("ORG"),
				Mockito.any(Map.class));

		verify(tested.esIntegrationComponent, times(2)).prepareESBulkRequestBuilder();
		verify(tested.esIntegrationComponent, times(2)).executeESBulkRequest(Mockito.any(BulkRequestBuilder.class));
		verify(tested.esIntegrationComponent, Mockito.atLeastOnce()).isClosed();
		Mockito.verifyNoMoreInteractions(tested.remoteSystemClient);
		Mockito.verifyNoMoreInteractions(tested.esIntegrationComponent);
		Mockito.verifyNoMoreInteractions(tested.documentIndexStructureBuilder);
	}

	protected SpacePaginatingIndexer getTested() {
		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder documentIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		SpacePaginatingIndexer tested = new SpacePaginatingIndexer("ORG", remoteClientMock, esIntegrationMock,
				documentIndexStructureBuilderMock);
		return tested;
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

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.logging.ESLogger;
import org.jboss.elasticsearch.river.remote.exception.RemoteDocumentNotFoundException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

/**
 * Unit test for {@link SpaceIndexerBase}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceIndexerBaseTest {

	private static final String DOC_ID = "docid";
	private static final String SPACE = "space";

	@Test
	public void constructor() {
		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder documentIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);

		try {
			new TestIndexer(null, remoteClientMock, esIntegrationMock, documentIndexStructureBuilderMock);
			Assert.fail("IllegalArgumentException expected");
		} catch (IllegalArgumentException e) {
			// OK
		}

		TestIndexer tested = new TestIndexer(SPACE, remoteClientMock, esIntegrationMock, documentIndexStructureBuilderMock);
		Assert.assertEquals(SPACE, tested.spaceKey);
		Assert.assertEquals(remoteClientMock, tested.remoteSystemClient);
		Assert.assertEquals(documentIndexStructureBuilderMock, tested.documentIndexStructureBuilder);
		Assert.assertEquals(esIntegrationMock, tested.esIntegrationComponent);

	}

	@Test
	public void getDocumentIdChecked() {

		TestIndexer tested = getTested();

		// document id found
		{
			Map<String, Object> document = new HashMap<String, Object>();
			Mockito.when(tested.documentIndexStructureBuilder.extractDocumentId(document)).thenReturn("aa");
			Assert.assertEquals("aa", tested.getDocumentIdChecked(document));
		}

		// document id is null
		{
			Map<String, Object> document = new HashMap<String, Object>();
			Mockito.when(tested.documentIndexStructureBuilder.extractDocumentId(document)).thenReturn(null);
			try {
				tested.getDocumentIdChecked(document);
				Assert.fail("IllegalArgumentException expected");
			} catch (IllegalArgumentException e) {

			}
		}

		// document id is empty string
		{
			Map<String, Object> document = new HashMap<String, Object>();
			Mockito.when(tested.documentIndexStructureBuilder.extractDocumentId(document)).thenReturn("  ");
			try {
				tested.getDocumentIdChecked(document);
				Assert.fail("IllegalArgumentException expected");
			} catch (IllegalArgumentException e) {

			}
		}

	}

	@Test
	public void getDocumentDetail() throws Exception {
		TestIndexer tested = getTested();

		// case - remote document OK
		{
			Mockito.reset(tested.remoteSystemClient);
			Map<String, Object> detail = new HashMap<String, Object>();
			Map<String, Object> document = new HashMap<String, Object>();
			Mockito.when(tested.remoteSystemClient.getChangedDocumentDetails(SPACE, DOC_ID, document)).thenReturn(detail);

			Assert.assertTrue(tested.getDocumentDetail(DOC_ID, document));

			Assert.assertEquals(detail, document.get(SpaceIndexerBase.KEY_DETAIL));
		}

		// case - remote document empty
		{
			Mockito.reset(tested.remoteSystemClient);
			Map<String, Object> document = new HashMap<String, Object>();
			Mockito.when(tested.remoteSystemClient.getChangedDocumentDetails(SPACE, DOC_ID, document)).thenReturn(null);

			Assert.assertTrue(tested.getDocumentDetail(DOC_ID, document));

			Assert.assertEquals(null, document.get(SpaceIndexerBase.KEY_DETAIL));
		}

		// case - remote document RemoteDocumentNotFoundException
		{
			Mockito.reset(tested.remoteSystemClient, tested.indexingInfo);
			Map<String, Object> document = new HashMap<String, Object>();
			Mockito.when(tested.remoteSystemClient.getChangedDocumentDetails(SPACE, DOC_ID, document)).thenThrow(
					new RemoteDocumentNotFoundException("msg"));

			Assert.assertFalse(tested.getDocumentDetail(DOC_ID, document));
			Assert.assertEquals(null, document.get(SpaceIndexerBase.KEY_DETAIL));
			Assert.assertEquals(1, tested.indexingInfo.documentsWithError);
			Mockito.verify(tested.indexingInfo).addErrorMessage(Mockito.anyString());
		}

		// case - other exception
		{
			Mockito.reset(tested.remoteSystemClient, tested.indexingInfo);
			tested.indexingInfo.documentsWithError = 0;
			Map<String, Object> document = new HashMap<String, Object>();
			Exception e = new Exception();
			Mockito.when(tested.remoteSystemClient.getChangedDocumentDetails(SPACE, DOC_ID, document)).thenThrow(e);

			try {
				tested.getDocumentDetail(DOC_ID, document);
				Assert.fail("Exception expected");
			} catch (Exception ex) {
				Assert.assertEquals(null, document.get(SpaceIndexerBase.KEY_DETAIL));
				Assert.assertEquals(0, tested.indexingInfo.documentsWithError);
				Mockito.verify(tested.indexingInfo, Mockito.times(0)).addErrorMessage(Mockito.anyString());
				Assert.assertEquals(e, ex);
			}
		}

	}

	/**
	 * @return
	 */
	protected TestIndexer getTested() {
		IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		IDocumentIndexStructureBuilder documentIndexStructureBuilderMock = mock(IDocumentIndexStructureBuilder.class);
		TestIndexer tested = new TestIndexer(SPACE, remoteClientMock, esIntegrationMock, documentIndexStructureBuilderMock);
		tested.indexingInfo = Mockito.mock(SpaceIndexingInfo.class);
		tested.logger = Mockito.mock(ESLogger.class);
		return tested;
	}

	private static final class TestIndexer extends SpaceIndexerBase {

		public TestIndexer(String spaceKey, IRemoteSystemClient remoteSystemClient, IESIntegration esIntegrationComponent,
				IDocumentIndexStructureBuilder documentIndexStructureBuilder) {
			super(spaceKey, remoteSystemClient, esIntegrationComponent, documentIndexStructureBuilder);
		}

		@Override
		protected void processUpdate() throws Exception {
		}

	}

}

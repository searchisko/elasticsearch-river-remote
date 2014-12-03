/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.SettingsException;
import org.jboss.elasticsearch.river.remote.testtools.MockThread;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link SpaceIndexerCoordinator}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceIndexerCoordinatorTest {

	private static final String SPACE_KEY = "ORG";

	@Test
	public void prepareSpaceIndexer() {
		IESIntegration esIntegrationMock = mockEsIntegrationComponent();
		IRemoteSystemClient remoteSystemClientMock = Mockito.mock(IRemoteSystemClient.class);
		IDocumentIndexStructureBuilder documentIndexStructureBuilder = Mockito.mock(IDocumentIndexStructureBuilder.class);
		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(remoteSystemClientMock, esIntegrationMock,
				documentIndexStructureBuilder, 10, 2, -1, null, SpaceIndexingMode.SIMPLE);

		{
			SpaceIndexerBase indexer = tested.prepareSpaceIndexer(SPACE_KEY, true);
			Assert.assertTrue(indexer instanceof SpaceSimpleIndexer);
			Assert.assertEquals(esIntegrationMock, indexer.esIntegrationComponent);
			Assert.assertEquals(documentIndexStructureBuilder, indexer.documentIndexStructureBuilder);
			Assert.assertEquals(remoteSystemClientMock, indexer.remoteSystemClient);
			Assert.assertEquals(SPACE_KEY, indexer.spaceKey);
		}

		{
			tested.spaceIndexingMode = SpaceIndexingMode.PAGINATION;
			SpaceIndexerBase indexer = tested.prepareSpaceIndexer(SPACE_KEY, true);
			Assert.assertTrue(indexer instanceof SpacePaginatingIndexer);
			Assert.assertEquals(esIntegrationMock, indexer.esIntegrationComponent);
			Assert.assertEquals(documentIndexStructureBuilder, indexer.documentIndexStructureBuilder);
			Assert.assertEquals(remoteSystemClientMock, indexer.remoteSystemClient);
			Assert.assertEquals(SPACE_KEY, indexer.spaceKey);
		}

		{
			tested.spaceIndexingMode = SpaceIndexingMode.UPDATE_TIMESTAMP;
			SpaceIndexerBase indexer = tested.prepareSpaceIndexer(SPACE_KEY, true);
			Assert.assertTrue(indexer instanceof SpaceByLastUpdateTimestampIndexer);
			Assert.assertEquals(esIntegrationMock, indexer.esIntegrationComponent);
			Assert.assertEquals(documentIndexStructureBuilder, indexer.documentIndexStructureBuilder);
			Assert.assertEquals(remoteSystemClientMock, indexer.remoteSystemClient);
			Assert.assertEquals(SPACE_KEY, indexer.spaceKey);
			Assert.assertEquals(true, indexer.indexingInfo.fullUpdate);
		}

		{
			tested.spaceIndexingMode = SpaceIndexingMode.UPDATE_TIMESTAMP;
			SpaceIndexerBase indexer = tested.prepareSpaceIndexer(SPACE_KEY, false);
			Assert.assertTrue(indexer instanceof SpaceByLastUpdateTimestampIndexer);
			Assert.assertEquals(esIntegrationMock, indexer.esIntegrationComponent);
			Assert.assertEquals(documentIndexStructureBuilder, indexer.documentIndexStructureBuilder);
			Assert.assertEquals(remoteSystemClientMock, indexer.remoteSystemClient);
			Assert.assertEquals(SPACE_KEY, indexer.spaceKey);
			Assert.assertEquals(false, indexer.indexingInfo.fullUpdate);
		}

		try {
			tested.spaceIndexingMode = null;
			tested.prepareSpaceIndexer(SPACE_KEY, true);
			Assert.fail("SettingsException expected");
		} catch (SettingsException e) {
			// OK
		}

	}

	@Test
	public void spaceIndexUpdateNecessary() throws Exception {
		int indexUpdatePeriod = 60 * 1000;

		IESIntegration esIntegrationMock = mockEsIntegrationComponent();
		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, indexUpdatePeriod, 2,
				-1, null, SpaceIndexingMode.SIMPLE);

		// case - update necessary - no date of last update stored
		{
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(null);
			Assert.assertTrue(tested.spaceIndexUpdateNecessary(SPACE_KEY));
		}

		// case - update necessary - date of last update stored and is older than index update period
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexUpdatePeriod - 100));
			Assert.assertTrue(tested.spaceIndexUpdateNecessary(SPACE_KEY));
		}

		// case - no update necessary - date of last update stored and is newer than index update period
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexUpdatePeriod + 1000));
			Assert.assertFalse(tested.spaceIndexUpdateNecessary(SPACE_KEY));
		}

		// case - update necessary - date of last update stored and is newer than index update period, but full reindex is
		// forced now, so we have to start it ASAP
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexUpdatePeriod + 1000));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - 1000));
			Assert.assertTrue(tested.spaceIndexUpdateNecessary(SPACE_KEY));
		}

		// case - update necessary - #49 - date of last run is newer than index update period, but cron expression for
		// full update is satisfied
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexUpdatePeriod + 100));
			tested.indexFullUpdateCronExpression = new CronExpression("0 0/1 * * * ?");
			Assert.assertTrue(tested.spaceIndexUpdateNecessary(SPACE_KEY));
		}

		// case - update not necessary - #49 - date of last run is newer than index update period and cron expression for
		// full update is not satisfied
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexUpdatePeriod + 1000));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - 100));
			tested.indexFullUpdateCronExpression = new CronExpression("0 0/1 * * * ?");
			Assert.assertFalse(tested.spaceIndexUpdateNecessary(SPACE_KEY));
		}

		// case - update not necessary - date of last update stored but index update period is 0
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexUpdatePeriod - 100));
			tested.indexUpdatePeriod = 0;
			tested.indexFullUpdateCronExpression = null;
			Assert.assertFalse(tested.spaceIndexUpdateNecessary(SPACE_KEY));
		}

	}

	@Test
	public void spaceIndexFullUpdateNecessary() throws Exception {
		int indexFullUpdatePeriod = 60 * 1000;

		IESIntegration esIntegrationMock = mockEsIntegrationComponent();
		// simpleGetDocument is on true to check tested operation is not affected by this (as it is used in indexer later)
		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, 1000, 2, -1, null,
				SpaceIndexingMode.SIMPLE);
		Assert.assertEquals(SpaceIndexingMode.SIMPLE, tested.spaceIndexingMode);
		tested.setIndexFullUpdatePeriod(0);

		// case - full update disabled, no force
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			Assert.assertFalse(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
			verify(esIntegrationMock).readDatetimeValue(SPACE_KEY,
					SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE);
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

		tested.setIndexFullUpdatePeriod(indexFullUpdatePeriod);
		// case - full update necessary - no date of last full update stored, no force
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			Assert.assertTrue(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
			verify(esIntegrationMock).readDatetimeValue(SPACE_KEY,
					SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE);
			verify(esIntegrationMock).readDatetimeValue(SPACE_KEY,
					SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE);

			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

		// case - full update necessary - date of last full update stored and is older than index full update period, no
		// force
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexFullUpdatePeriod - 100));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			Assert.assertTrue(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
			verify(esIntegrationMock).readDatetimeValue(SPACE_KEY,
					SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE);
			verify(esIntegrationMock).readDatetimeValue(SPACE_KEY,
					SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE);
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

		// case - no full update necessary - date of last full update stored and is newer than index full update period, no
		// force
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexFullUpdatePeriod + 1000));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(null);

			Assert.assertFalse(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
			verify(esIntegrationMock).readDatetimeValue(SPACE_KEY,
					SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE);
			verify(esIntegrationMock).readDatetimeValue(SPACE_KEY,
					SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE);

			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}
	}

	@Test
	public void spaceIndexFullUpdateNecessary_cron() throws Exception {
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, 1000, 2, -1, null,
				SpaceIndexingMode.SIMPLE);

		// case - full update necessary - because no full update performed yet
		{
			tested.indexFullUpdateCronExpression = new CronExpression("0 0 0/1 * * ?");
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			Assert.assertTrue(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
		}

		// case - full update necessary - full update performed but cron is satisfied now
		{
			tested.indexFullUpdateCronExpression = new CronExpression("0 0 0/1 * * ?");
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - (61 * 60 * 1000L)));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			Assert.assertTrue(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
		}

		// case - full update not necessary - full update performed and cron is not satisfied now
		{
			tested.indexFullUpdateCronExpression = new CronExpression("0 0 0/1 * * ?");
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(new Date());
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			Assert.assertFalse(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
		}

		// case - full update necessary - full update performed and cron is not satisfied now but update is forced
		{
			tested.indexFullUpdateCronExpression = new CronExpression("0 0 0/1 * * ?");
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - (1000L)));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(new Date());
			Assert.assertTrue(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
		}

	}

	@Test
	public void spaceIndexFullUpdateNecessary_forced() throws Exception {
		int indexFullUpdatePeriod = 60 * 1000;

		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, 1000, 2, -1, null,
				SpaceIndexingMode.SIMPLE);
		tested.setIndexFullUpdatePeriod(0);

		// case - full update disabled, but forced
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(new Date());
			Assert.assertTrue(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
			verify(esIntegrationMock).readDatetimeValue(SPACE_KEY,
					SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE);
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

		tested.setIndexFullUpdatePeriod(indexFullUpdatePeriod);
		// case - full update necessary - no date of last full update stored, but forced
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(new Date());
			Assert.assertTrue(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
			verify(esIntegrationMock).readDatetimeValue(SPACE_KEY,
					SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE);

			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

		// case - full update necessary - date of last full update stored and is older than index full update period, but
		// forced
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexFullUpdatePeriod - 100));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(new Date());
			Assert.assertTrue(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
			verify(esIntegrationMock).readDatetimeValue(SPACE_KEY,
					SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE);
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

		// case - no full update necessary - date of last full update stored and is newer than index full update period, but
		// forced
		{
			reset(esIntegrationMock);
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexFullUpdatePeriod + 1000));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE)).thenReturn(new Date());

			Assert.assertTrue(tested.spaceIndexFullUpdateNecessary(SPACE_KEY));
			verify(esIntegrationMock).readDatetimeValue(SPACE_KEY,
					SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE);

			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}
	}

	@Test
	public void fillSpaceKeysToIndexQueue() throws Exception {
		int indexUpdatePeriod = 60 * 1000;
		IESIntegration esIntegrationMock = mockEsIntegrationComponent();
		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, indexUpdatePeriod, 2,
				-1, null, SpaceIndexingMode.SIMPLE);
		Assert.assertTrue(tested.spaceKeysToIndexQueue.isEmpty());

		// case - no any space available (both null or empty list)
		{
			when(esIntegrationMock.getAllIndexedSpaceKeys()).thenReturn(null);
			tested.fillSpaceKeysToIndexQueue();
			Assert.assertTrue(tested.spaceKeysToIndexQueue.isEmpty());
			verify(esIntegrationMock, times(0)).readDatetimeValue(Mockito.any(String.class), Mockito.anyString());
			reset(esIntegrationMock);
			when(esIntegrationMock.getAllIndexedSpaceKeys()).thenReturn(new ArrayList<String>());
			tested.fillSpaceKeysToIndexQueue();
			Assert.assertTrue(tested.spaceKeysToIndexQueue.isEmpty());
			verify(esIntegrationMock, times(0)).readDatetimeValue(Mockito.any(String.class), Mockito.anyString());
		}

		// case - some spaces available
		{
			reset(esIntegrationMock);
			when(esIntegrationMock.getAllIndexedSpaceKeys()).thenReturn(Utils.parseCsvString("ORG,AAA,BBB,CCC,DDD"));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(null);
			when(
					esIntegrationMock.readDatetimeValue("AAA",
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexUpdatePeriod - 100));
			when(
					esIntegrationMock.readDatetimeValue("BBB",
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexUpdatePeriod + 1000));
			when(
					esIntegrationMock.readDatetimeValue("CCC",
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexUpdatePeriod - 100));
			when(
					esIntegrationMock.readDatetimeValue("DDD",
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(
					new Date(System.currentTimeMillis() - indexUpdatePeriod - 100));

			tested.fillSpaceKeysToIndexQueue();
			Assert.assertFalse(tested.spaceKeysToIndexQueue.isEmpty());
			Assert.assertEquals(4, tested.spaceKeysToIndexQueue.size());
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains(SPACE_KEY));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("AAA"));
			Assert.assertFalse(tested.spaceKeysToIndexQueue.contains("BBB"));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("CCC"));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("DDD"));
		}

		// case - some space available for index update, but in processing already, so do not schedule it for processing
		// now
		{
			esIntegrationMock = mockEsIntegrationComponent();
			tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, indexUpdatePeriod, 2, -1, null,
					SpaceIndexingMode.SIMPLE);
			tested.spaceIndexerThreads.put(SPACE_KEY, new Thread());
			when(
					esIntegrationMock.readDatetimeValue(Mockito.eq(Mockito.anyString()),
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(null);

			when(esIntegrationMock.getAllIndexedSpaceKeys()).thenReturn(Utils.parseCsvString("ORG,AAA,BBB,CCC,DDD"));
			tested.fillSpaceKeysToIndexQueue();
			Assert.assertFalse(tested.spaceKeysToIndexQueue.isEmpty());
			Assert.assertEquals(4, tested.spaceKeysToIndexQueue.size());
			Assert.assertFalse(tested.spaceKeysToIndexQueue.contains(SPACE_KEY));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("AAA"));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("BBB"));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("CCC"));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("DDD"));
		}

		// case - some space available for index update, but in queue already, so do not schedule it for processing now
		{
			esIntegrationMock = mockEsIntegrationComponent();
			tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, indexUpdatePeriod, 2, -1, null,
					SpaceIndexingMode.SIMPLE);
			tested.spaceKeysToIndexQueue.add(SPACE_KEY);
			when(
					esIntegrationMock.readDatetimeValue(Mockito.eq(Mockito.anyString()),
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(null);

			when(esIntegrationMock.getAllIndexedSpaceKeys()).thenReturn(Utils.parseCsvString("ORG,AAA,BBB,CCC,DDD"));
			tested.fillSpaceKeysToIndexQueue();
			Assert.assertFalse(tested.spaceKeysToIndexQueue.isEmpty());
			Assert.assertEquals(5, tested.spaceKeysToIndexQueue.size());
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains(SPACE_KEY));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("AAA"));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("BBB"));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("CCC"));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("DDD"));
		}

		// case - exception when interrupted from ES server
		{
			esIntegrationMock = mockEsIntegrationComponent();
			tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, indexUpdatePeriod, 2, -1, null,
					SpaceIndexingMode.SIMPLE);
			tested.spaceKeysToIndexQueue.add(SPACE_KEY);
			when(esIntegrationMock.getAllIndexedSpaceKeys()).thenReturn(Utils.parseCsvString("ORG,AAA,BBB,CCC,DDD"));
			when(esIntegrationMock.isClosed()).thenReturn(true);
			try {
				tested.fillSpaceKeysToIndexQueue();
				Assert.fail("No InterruptedException thrown");
			} catch (InterruptedException e) {
				// OK
			}
		}
	}

	@Test
	public void startIndexers() throws Exception {

		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, 100000, 2, -1, null,
				SpaceIndexingMode.SIMPLE);
		Assert.assertTrue(tested.spaceKeysToIndexQueue.isEmpty());

		// case - nothing to start
		{
			tested.startIndexers();
			Assert.assertTrue(tested.spaceIndexerThreads.isEmpty());
			Assert.assertTrue(tested.spaceIndexers.isEmpty());
			verify(esIntegrationMock, times(0)).acquireIndexingThread(Mockito.any(String.class), Mockito.any(Runnable.class));
		}

		// case - all indexer slots full, do not start new ones
		{
			reset(esIntegrationMock);
			tested.spaceKeysToIndexQueue.addAll(Utils.parseCsvString("ORG,AAA,BBB,CCC,DDD"));
			tested.spaceIndexerThreads.put("JJ", new Thread());
			tested.spaceIndexerThreads.put("II", new Thread());
			tested.startIndexers();
			Assert.assertEquals(2, tested.spaceIndexerThreads.size());
			Assert.assertEquals(5, tested.spaceKeysToIndexQueue.size());
			verify(esIntegrationMock, times(0)).acquireIndexingThread(Mockito.any(String.class), Mockito.any(Runnable.class));
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

		// case - one indexer slot empty, start new one
		{
			reset(esIntegrationMock);
			tested.spaceIndexerThreads.clear();
			tested.spaceIndexerThreads.put("II", new Thread());
			tested.spaceIndexers.clear();
			tested.spaceIndexers.put("II", new SpaceByLastUpdateTimestampIndexer("II", true, null, esIntegrationMock, null));
			tested.spaceKeysToIndexQueue.clear();
			tested.spaceKeysToIndexQueue.addAll(Utils.parseCsvString("ORG,AAA,BBB,CCC,DDD"));
			when(esIntegrationMock.acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"), Mockito.any(Runnable.class)))
					.thenReturn(new MockThread());
			tested.startIndexers();
			Assert.assertEquals(2, tested.spaceIndexerThreads.size());
			Assert.assertTrue(tested.spaceIndexerThreads.containsKey(SPACE_KEY));
			Assert.assertEquals(2, tested.spaceIndexers.size());
			Assert.assertTrue(tested.spaceIndexers.containsKey(SPACE_KEY));
			Assert.assertTrue(((MockThread) tested.spaceIndexerThreads.get(SPACE_KEY)).wasStarted);
			Assert.assertEquals(4, tested.spaceKeysToIndexQueue.size());
			Assert.assertFalse(tested.spaceKeysToIndexQueue.contains(SPACE_KEY));
			verify(esIntegrationMock, times(1)).acquireIndexingThread(Mockito.any(String.class), Mockito.any(Runnable.class));
			verify(esIntegrationMock, times(1)).acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"),
					Mockito.any(Runnable.class));
			verify(esIntegrationMock, times(1)).storeDatetimeValue(Mockito.eq(SPACE_KEY),
					Mockito.eq(SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE), Mockito.any(Date.class),
					Mockito.eq((BulkRequestBuilder) null));
		}

		// case - two slots empty and more space available, start two indexers
		{
			reset(esIntegrationMock);
			tested.spaceIndexerThreads.clear();
			tested.spaceIndexers.clear();
			tested.spaceKeysToIndexQueue.clear();
			tested.spaceKeysToIndexQueue.addAll(Utils.parseCsvString("ORG,AAA,BBB,CCC,DDD"));
			when(esIntegrationMock.acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"), Mockito.any(Runnable.class)))
					.thenReturn(new MockThread());
			when(esIntegrationMock.acquireIndexingThread(Mockito.eq("remote_river_indexer_AAA"), Mockito.any(Runnable.class)))
					.thenReturn(new MockThread());
			tested.startIndexers();
			Assert.assertEquals(2, tested.spaceIndexerThreads.size());
			Assert.assertTrue(tested.spaceIndexerThreads.containsKey(SPACE_KEY));
			Assert.assertTrue(tested.spaceIndexerThreads.containsKey("AAA"));
			Assert.assertTrue(((MockThread) tested.spaceIndexerThreads.get(SPACE_KEY)).wasStarted);
			Assert.assertTrue(((MockThread) tested.spaceIndexerThreads.get("AAA")).wasStarted);
			Assert.assertEquals(2, tested.spaceIndexers.size());
			Assert.assertTrue(tested.spaceIndexers.containsKey(SPACE_KEY));
			Assert.assertTrue(tested.spaceIndexers.containsKey("AAA"));

			Assert.assertEquals(3, tested.spaceKeysToIndexQueue.size());
			Assert.assertFalse(tested.spaceKeysToIndexQueue.contains(SPACE_KEY));
			Assert.assertFalse(tested.spaceKeysToIndexQueue.contains("AAA"));
			verify(esIntegrationMock, times(2)).acquireIndexingThread(Mockito.any(String.class), Mockito.any(Runnable.class));
			verify(esIntegrationMock, times(1)).acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"),
					Mockito.any(Runnable.class));
			verify(esIntegrationMock, times(1)).acquireIndexingThread(Mockito.eq("remote_river_indexer_AAA"),
					Mockito.any(Runnable.class));
			verify(esIntegrationMock, times(1)).storeDatetimeValue(Mockito.eq(SPACE_KEY),
					Mockito.eq(SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE), Mockito.any(Date.class),
					Mockito.eq((BulkRequestBuilder) null));
			verify(esIntegrationMock, times(1)).storeDatetimeValue(Mockito.eq("AAA"),
					Mockito.eq(SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE), Mockito.any(Date.class),
					Mockito.eq((BulkRequestBuilder) null));
		}

		// case - two slots empty but only one space available, start it
		{
			reset(esIntegrationMock);
			tested.spaceIndexerThreads.clear();
			tested.spaceIndexers.clear();
			tested.spaceKeysToIndexQueue.clear();
			tested.spaceKeysToIndexQueue.addAll(Utils.parseCsvString(SPACE_KEY));
			when(esIntegrationMock.acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"), Mockito.any(Runnable.class)))
					.thenReturn(new MockThread());
			tested.startIndexers();
			Assert.assertEquals(1, tested.spaceIndexerThreads.size());
			Assert.assertTrue(tested.spaceIndexerThreads.containsKey(SPACE_KEY));
			Assert.assertEquals(1, tested.spaceIndexers.size());
			Assert.assertTrue(tested.spaceIndexers.containsKey(SPACE_KEY));
			Assert.assertTrue(((MockThread) tested.spaceIndexerThreads.get(SPACE_KEY)).wasStarted);
			Assert.assertTrue(tested.spaceKeysToIndexQueue.isEmpty());
			verify(esIntegrationMock, times(1)).acquireIndexingThread(Mockito.any(String.class), Mockito.any(Runnable.class));
			verify(esIntegrationMock, times(1)).acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"),
					Mockito.any(Runnable.class));
			verify(esIntegrationMock, times(1)).storeDatetimeValue(Mockito.eq(SPACE_KEY),
					Mockito.eq(SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE), Mockito.any(Date.class),
					Mockito.eq((BulkRequestBuilder) null));
		}

		// case - exception when interrupted from ES server
		{
			reset(esIntegrationMock);
			tested.spaceIndexerThreads.clear();
			tested.spaceIndexers.clear();
			tested.spaceKeysToIndexQueue.clear();
			tested.spaceKeysToIndexQueue.addAll(Utils.parseCsvString(SPACE_KEY));
			when(esIntegrationMock.isClosed()).thenReturn(true);
			try {
				tested.startIndexers();
				Assert.fail("No InterruptedException thrown");
			} catch (InterruptedException e) {
				// OK
			}
		}
	}

	@Test
	public void startIndexers_reserveIndexingThreadSlotForIncremental() throws Exception {

		IESIntegration esIntegrationMock = mockEsIntegrationComponent();
		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, 100000, 2, -1, null,
				SpaceIndexingMode.SIMPLE);
		Assert.assertTrue(tested.spaceKeysToIndexQueue.isEmpty());

		// case - only one thread configured, so use it for full reindex too!!
		{
			reset(esIntegrationMock);
			tested.indexFullUpdatePeriod = 1000;
			tested.maxIndexingThreads = 1;
			tested.spaceIndexerThreads.clear();
			tested.spaceKeysToIndexQueue.clear();
			tested.spaceKeysToIndexQueue.addAll(Utils.parseCsvString("ORG,AAA"));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			when(
					esIntegrationMock.readDatetimeValue("AAA",
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);

			when(esIntegrationMock.acquireIndexingThread(Mockito.anyString(), Mockito.any(Runnable.class))).thenReturn(
					new MockThread());

			tested.startIndexers();
			Assert.assertEquals(1, tested.spaceIndexerThreads.size());
			Assert.assertTrue(tested.spaceIndexerThreads.containsKey(SPACE_KEY));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("AAA"));
			Assert.assertEquals(1, tested.spaceKeysToIndexQueue.size());
		}

		// case - one slot empty from two, but only full reindex requested, so let slot empty
		{
			reset(esIntegrationMock);
			tested.indexFullUpdatePeriod = 1000;
			tested.maxIndexingThreads = 2;
			tested.spaceIndexerThreads.clear();
			tested.spaceIndexerThreads.put("BBB", new Thread());

			tested.spaceKeysToIndexQueue.clear();
			tested.spaceKeysToIndexQueue.addAll(Utils.parseCsvString(SPACE_KEY));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			when(esIntegrationMock.acquireIndexingThread(Mockito.anyString(), Mockito.any(Runnable.class))).thenReturn(
					new MockThread());

			tested.startIndexers();
			Assert.assertEquals(1, tested.spaceIndexerThreads.size());
			Assert.assertTrue(tested.spaceIndexerThreads.containsKey("BBB"));
			Assert.assertFalse(tested.spaceIndexerThreads.containsKey(SPACE_KEY));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains(SPACE_KEY));
			Assert.assertEquals(1, tested.spaceKeysToIndexQueue.size());
		}

		// case - one slot empty from two, so use it for incremental update for second space in queue
		{
			reset(esIntegrationMock);
			tested.indexFullUpdatePeriod = 1000;
			tested.maxIndexingThreads = 2;
			tested.spaceIndexerThreads.clear();
			tested.spaceIndexerThreads.put("BBB", new Thread());

			tested.spaceKeysToIndexQueue.clear();
			tested.spaceKeysToIndexQueue.addAll(Utils.parseCsvString("ORG,AAA"));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			when(
					esIntegrationMock.readDatetimeValue("AAA",
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(new Date());
			when(esIntegrationMock.acquireIndexingThread(Mockito.anyString(), Mockito.any(Runnable.class))).thenReturn(
					new MockThread());

			tested.startIndexers();
			Assert.assertEquals(2, tested.spaceIndexerThreads.size());
			Assert.assertTrue(tested.spaceIndexerThreads.containsKey("AAA"));
			Assert.assertTrue(tested.spaceIndexerThreads.containsKey("BBB"));
			Assert.assertFalse(tested.spaceIndexerThreads.containsKey(SPACE_KEY));
			// check first space stayed in queue!
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains(SPACE_KEY));
			Assert.assertEquals(1, tested.spaceKeysToIndexQueue.size());
		}

		// case - two slots empty from three, so use first for full update but second for incremental update which is third
		// in queue
		{
			reset(esIntegrationMock);
			tested.indexFullUpdatePeriod = 1000;
			tested.maxIndexingThreads = 3;
			tested.spaceIndexerThreads.clear();
			tested.spaceIndexerThreads.put("BBB", new Thread());

			tested.spaceKeysToIndexQueue.clear();
			tested.spaceKeysToIndexQueue.addAll(Utils.parseCsvString("ORG,ORG2,AAA,ORG3"));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			when(
					esIntegrationMock.readDatetimeValue("ORG2",
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			when(
					esIntegrationMock.readDatetimeValue("ORG3",
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(null);
			when(
					esIntegrationMock.readDatetimeValue("AAA",
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE)).thenReturn(new Date());
			when(esIntegrationMock.acquireIndexingThread(Mockito.anyString(), Mockito.any(Runnable.class))).thenReturn(
					new MockThread());

			tested.startIndexers();
			Assert.assertEquals(3, tested.spaceIndexerThreads.size());
			Assert.assertTrue(tested.spaceIndexerThreads.containsKey("BBB"));
			Assert.assertTrue(tested.spaceIndexerThreads.containsKey(SPACE_KEY));
			Assert.assertTrue(tested.spaceIndexerThreads.containsKey("AAA"));
			Assert.assertFalse(tested.spaceIndexerThreads.containsKey("ORG2"));
			Assert.assertFalse(tested.spaceIndexerThreads.containsKey("ORG3"));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("ORG2"));
			Assert.assertTrue(tested.spaceKeysToIndexQueue.contains("ORG3"));
			Assert.assertEquals(2, tested.spaceKeysToIndexQueue.size());
		}

	}

	@Test
	public void run() throws Exception {
		IESIntegration esIntegrationMock = mockEsIntegrationComponent();
		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, 100000, 2, -1, null,
				SpaceIndexingMode.SIMPLE);
		when(esIntegrationMock.acquireIndexingThread(Mockito.any(String.class), Mockito.any(Runnable.class))).thenReturn(
				new MockThread());

		// case - close flag is set, so interrupt all indexers and free them
		{
			MockThread mt1 = new MockThread();
			MockThread mt2 = new MockThread();
			tested.spaceIndexerThreads.put(SPACE_KEY, mt1);
			tested.spaceIndexerThreads.put("AAA", mt2);
			when(esIntegrationMock.isClosed()).thenReturn(true);

			tested.run();
			Assert.assertTrue(tested.spaceIndexerThreads.isEmpty());
			Assert.assertTrue(mt1.interruptWasCalled);
			Assert.assertTrue(mt2.interruptWasCalled);
		}

		// case - InterruptedException is thrown, so interrupt all indexers
		{
			reset(esIntegrationMock);
			tested.spaceIndexerThreads.clear();
			tested.spaceKeysToIndexQueue.clear();
			MockThread mt1 = new MockThread();
			MockThread mt2 = new MockThread();
			tested.spaceIndexerThreads.put(SPACE_KEY, mt1);
			tested.spaceIndexerThreads.put("AAA", mt2);
			when(esIntegrationMock.isClosed()).thenReturn(false);
			when(esIntegrationMock.getAllIndexedSpaceKeys()).thenThrow(new InterruptedException());

			tested.run();
			Assert.assertTrue(tested.spaceIndexerThreads.isEmpty());
			Assert.assertTrue(mt1.interruptWasCalled);
			Assert.assertTrue(mt2.interruptWasCalled);
		}

		// case - closed, so try to interrupt all indexers but not exception if empty
		{
			reset(esIntegrationMock);
			tested.spaceIndexerThreads.clear();
			tested.spaceKeysToIndexQueue.clear();
			when(esIntegrationMock.isClosed()).thenReturn(true);

			tested.run();
			Assert.assertTrue(tested.spaceIndexerThreads.isEmpty());
		}
	}

	@Test
	public void processLoopTask() throws Exception {
		IESIntegration esIntegrationMock = mockEsIntegrationComponent();
		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, 100000, 2, -1, null,
				SpaceIndexingMode.SIMPLE);
		when(esIntegrationMock.acquireIndexingThread(Mockito.any(String.class), Mockito.any(Runnable.class))).thenReturn(
				new MockThread());

		// case - spaceKeysToIndexQueue is empty so call fillSpaceKeysToIndexQueue() and then call startIndexers()
		{
			reset(esIntegrationMock);
			tested.spaceIndexerThreads.clear();
			tested.spaceKeysToIndexQueue.clear();
			when(esIntegrationMock.getAllIndexedSpaceKeys()).thenReturn(Utils.parseCsvString(SPACE_KEY));
			when(
					esIntegrationMock.readDatetimeValue(SPACE_KEY,
							SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE)).thenReturn(null);
			when(esIntegrationMock.acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"), Mockito.any(Runnable.class)))
					.thenReturn(new MockThread());

			tested.processLoopTask();
			Assert.assertEquals(1, tested.spaceIndexerThreads.size());
			verify(esIntegrationMock, times(1)).getAllIndexedSpaceKeys();
			verify(esIntegrationMock, times(1)).acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"),
					Mockito.any(Runnable.class));
			Assert.assertEquals(SpaceIndexerCoordinator.COORDINATOR_THREAD_WAITS_QUICK, tested.coordinatorThreadWaits);
		}

		// case - spaceKeysToIndexQueue is not empty, no fillSpaceKeysToIndexQueue() is called because called in near
		// history, but startIndexers is called
		{
			reset(esIntegrationMock);
			tested.spaceIndexerThreads.clear();
			tested.spaceKeysToIndexQueue.clear();
			tested.spaceKeysToIndexQueue.add(SPACE_KEY);
			when(esIntegrationMock.getAllIndexedSpaceKeys()).thenReturn(Utils.parseCsvString(SPACE_KEY));
			when(esIntegrationMock.acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"), Mockito.any(Runnable.class)))
					.thenReturn(new MockThread());

			tested.processLoopTask();
			Assert.assertEquals(1, tested.spaceIndexerThreads.size());
			verify(esIntegrationMock, times(0)).getAllIndexedSpaceKeys();
			verify(esIntegrationMock, times(1)).acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"),
					Mockito.any(Runnable.class));
			Assert.assertEquals(SpaceIndexerCoordinator.COORDINATOR_THREAD_WAITS_QUICK, tested.coordinatorThreadWaits);
		}

		// case - spaceKeysToIndexQueue is not empty, but fillSpaceKeysToIndexQueue() is called because called long ago,
		// then startIndexers is called
		{
			reset(esIntegrationMock);
			tested.lastQueueFillTime = System.currentTimeMillis() - SpaceIndexerCoordinator.COORDINATOR_THREAD_WAITS_SLOW - 1;
			tested.spaceIndexerThreads.clear();
			tested.spaceKeysToIndexQueue.clear();
			tested.spaceKeysToIndexQueue.add(SPACE_KEY);
			when(esIntegrationMock.getAllIndexedSpaceKeys()).thenReturn(Utils.parseCsvString("ORG,AAA"));
			when(esIntegrationMock.acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"), Mockito.any(Runnable.class)))
					.thenReturn(new MockThread());
			when(esIntegrationMock.acquireIndexingThread(Mockito.eq("remote_river_indexer_AAA"), Mockito.any(Runnable.class)))
					.thenReturn(new MockThread());

			tested.processLoopTask();
			Assert.assertEquals(2, tested.spaceIndexerThreads.size());
			verify(esIntegrationMock, times(1)).getAllIndexedSpaceKeys();
			verify(esIntegrationMock, times(1)).acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"),
					Mockito.any(Runnable.class));
			verify(esIntegrationMock, times(1)).acquireIndexingThread(Mockito.eq("remote_river_indexer_AAA"),
					Mockito.any(Runnable.class));
			Assert.assertEquals(SpaceIndexerCoordinator.COORDINATOR_THREAD_WAITS_QUICK, tested.coordinatorThreadWaits);
		}

		// case - spaceKeysToIndexQueue is empty so call fillSpaceKeysToIndexQueue() but still empty so slow down and
		// dont call startIndexers()
		{
			reset(esIntegrationMock);
			tested.spaceIndexerThreads.clear();
			tested.spaceKeysToIndexQueue.clear();
			when(esIntegrationMock.getAllIndexedSpaceKeys()).thenReturn(null);

			tested.processLoopTask();
			verify(esIntegrationMock, times(1)).getAllIndexedSpaceKeys();
			Assert.assertTrue(tested.spaceKeysToIndexQueue.isEmpty());
			verify(esIntegrationMock, times(0)).acquireIndexingThread(Mockito.eq("remote_river_indexer_ORG"),
					Mockito.any(Runnable.class));
			Assert.assertEquals(SpaceIndexerCoordinator.COORDINATOR_THREAD_WAITS_SLOW, tested.coordinatorThreadWaits);
		}
	}

	@Test
	public void reportIndexingFinished() throws Exception {
		IESIntegration esIntegrationMock = mockEsIntegrationComponent();

		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, 10, 2, -1, null,
				SpaceIndexingMode.SIMPLE);
		tested.spaceIndexerThreads.put(SPACE_KEY, new Thread());
		tested.spaceIndexerThreads.put("AAA", new Thread());
		tested.spaceIndexers.put(SPACE_KEY, new SpaceByLastUpdateTimestampIndexer(SPACE_KEY, false, null,
				esIntegrationMock, null));
		tested.spaceIndexers.put("AAA", new SpaceByLastUpdateTimestampIndexer("AAA", false, null, esIntegrationMock, null));

		Mockito.verify(esIntegrationMock).createLogger(SpaceIndexerCoordinator.class);
		Mockito.verify(esIntegrationMock, Mockito.times(2)).createLogger(SpaceByLastUpdateTimestampIndexer.class);

		// case - incremental indexing with success
		{
			tested.reportIndexingFinished(SPACE_KEY, true, false);
			Assert.assertEquals(1, tested.spaceIndexerThreads.size());
			Assert.assertFalse(tested.spaceIndexerThreads.containsKey(SPACE_KEY));
			Assert.assertEquals(1, tested.spaceIndexers.size());
			Assert.assertFalse(tested.spaceIndexers.containsKey(SPACE_KEY));
			// no full reindex date stored
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

		// case - full indexing without success, full reindex is enabled
		tested.indexFullUpdatePeriod = 10;
		{
			tested.reportIndexingFinished("AAA", false, true);
			Assert.assertEquals(0, tested.spaceIndexerThreads.size());
			Assert.assertEquals(0, tested.spaceIndexers.size());
			// no full reindex date stored
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

		// case - full indexing without success, full reindex is disabled so we have to force it
		tested.indexFullUpdatePeriod = -1;
		{
			tested.reportIndexingFinished("AAA", false, true);
			Assert.assertEquals(0, tested.spaceIndexerThreads.size());
			Assert.assertEquals(0, tested.spaceIndexers.size());
			verify(esIntegrationMock).storeDatetimeValue(Mockito.eq("AAA"),
					Mockito.eq(SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE), Mockito.any(Date.class),
					(BulkRequestBuilder) Mockito.isNull());
			// no full reindex date stored
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}

		// case - full indexing with success
		{
			tested.spaceIndexerThreads.put("AAA", new Thread());
			tested.spaceIndexers.put("AAA",
					new SpaceByLastUpdateTimestampIndexer("AAA", false, null, esIntegrationMock, null));
			tested.reportIndexingFinished("AAA", true, true);
			Assert.assertEquals(0, tested.spaceIndexerThreads.size());
			Assert.assertEquals(0, tested.spaceIndexers.size());
			verify(esIntegrationMock).storeDatetimeValue(Mockito.eq("AAA"),
					Mockito.eq(SpaceIndexerCoordinator.STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE), (Date) Mockito.any(),
					(BulkRequestBuilder) Mockito.isNull());
			verify(esIntegrationMock).deleteDatetimeValue(Mockito.eq("AAA"),
					Mockito.eq(SpaceIndexerCoordinator.STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE));
			verify(esIntegrationMock, times(3)).createLogger(SpaceByLastUpdateTimestampIndexer.class);
			Mockito.verifyNoMoreInteractions(esIntegrationMock);
		}
	}

	@Test
	public void getCurrentSpaceIndexingInfo() {

		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		SpaceIndexerCoordinator tested = new SpaceIndexerCoordinator(null, esIntegrationMock, null, 10, 2, -1, null,
				SpaceIndexingMode.SIMPLE);

		{
			List<SpaceIndexingInfo> l = tested.getCurrentSpaceIndexingInfo();
			Assert.assertNotNull(l);
			Assert.assertTrue(l.isEmpty());
		}

		{
			tested.spaceIndexers.put("II", new SpaceByLastUpdateTimestampIndexer("II", true, null, esIntegrationMock, null));
			tested.spaceIndexers.put("III",
					new SpaceByLastUpdateTimestampIndexer("III", false, null, esIntegrationMock, null));
			List<SpaceIndexingInfo> l = tested.getCurrentSpaceIndexingInfo();
			Assert.assertNotNull(l);
			Assert.assertEquals(2, l.size());
			Assert.assertEquals("II", l.get(0).spaceKey);
			Assert.assertEquals("III", l.get(1).spaceKey);
		}
	}

	protected IESIntegration mockEsIntegrationComponent() {
		IESIntegration esIntegrationMock = mock(IESIntegration.class);
		Mockito.when(esIntegrationMock.createLogger(Mockito.any(Class.class))).thenReturn(
				ESLoggerFactory.getLogger(SpaceIndexerCoordinator.class.getName()));
		return esIntegrationMock;
	}
}

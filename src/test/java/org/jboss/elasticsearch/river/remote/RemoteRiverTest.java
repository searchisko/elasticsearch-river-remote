/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

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
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.jboss.elasticsearch.river.remote.testtools.DataPreprocessorMock;
import org.jboss.elasticsearch.river.remote.testtools.ESRealClientTestBase;
import org.jboss.elasticsearch.river.remote.testtools.MockThread;
import org.jboss.elasticsearch.river.remote.testtools.TestUtils;
import org.jboss.elasticsearch.tools.content.StructuredContentPreprocessor;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link RemoteRiver}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class RemoteRiverTest extends ESRealClientTestBase {

	@SuppressWarnings("unchecked")
	@Test
	public void constructor_config() throws Exception {

		// case - exception if no remote URL base is defined
		try {
			prepareRiverInstanceForTest(null, null, null, false);
			Assert.fail("No SettingsException thrown");
		} catch (SettingsException e) {
			// OK
		}
		try {
			prepareRiverInstanceForTest("   ", null, null, false);
			Assert.fail("No SettingsException thrown");
		} catch (SettingsException e) {
			// OK
		}

		Map<String, Object> remoteSettingsAdd = new HashMap<String, Object>();
		Map<String, Object> toplevelSettingsAdd = new HashMap<String, Object>();
		extendToplevelSettingsByMandatoryIndexSettings(toplevelSettingsAdd);

		// case - test default settings
		RemoteRiver tested = prepareRiverInstanceForTest("https://issues.jboss.org", remoteSettingsAdd,
				toplevelSettingsAdd, false);
		Assert.assertEquals(1, tested.maxIndexingThreads);
		Assert.assertEquals(5 * 60 * 1000, tested.indexUpdatePeriod);
		Assert.assertEquals(12 * 60 * 60 * 1000, tested.indexFullUpdatePeriod);
		Assert.assertEquals("my_remote_river", tested.indexName);
		Assert.assertEquals(RemoteRiver.INDEX_DOCUMENT_TYPE_NAME_DEFAULT, tested.typeName);
		Assert.assertEquals(tested.documentIndexStructureBuilder, tested.remoteSystemClient.getIndexStructureBuilder());

		// case - test river configuration reading
		remoteSettingsAdd.put("maxIndexingThreads", "5");
		remoteSettingsAdd.put("indexUpdatePeriod", "20m");
		remoteSettingsAdd.put("indexFullUpdatePeriod", "5h");
		remoteSettingsAdd.put("maxIssuesPerRequest", 20);
		remoteSettingsAdd.put("timeout", "5s");
		remoteSettingsAdd.put("jqlTimeZone", "Europe/Prague");
		Map<String, Object> indexSettings = (Map<String, Object>) toplevelSettingsAdd.get("index");
		indexSettings.put("index", "my_index_name");
		indexSettings.put("type", "type_test");
		tested = prepareRiverInstanceForTest("https://issues.jboss.org", remoteSettingsAdd, toplevelSettingsAdd, false);

		Assert.assertEquals(5, tested.maxIndexingThreads);
		Assert.assertEquals(20 * 60 * 1000, tested.indexUpdatePeriod);
		Assert.assertEquals(5 * 60 * 60 * 1000, tested.indexFullUpdatePeriod);
		Assert.assertEquals("my_index_name", tested.indexName);
		Assert.assertEquals("type_test", tested.typeName);
		// assert index structure builder initialization
		Assert.assertEquals(tested.documentIndexStructureBuilder, tested.remoteSystemClient.getIndexStructureBuilder());
		Assert.assertEquals(tested.indexName,
				((DocumentWithCommentsIndexStructureBuilder) tested.documentIndexStructureBuilder).indexName);
		Assert.assertEquals(tested.typeName,
				((DocumentWithCommentsIndexStructureBuilder) tested.documentIndexStructureBuilder).issueTypeName);
		Assert.assertEquals(tested.riverName().getName(),
				((DocumentWithCommentsIndexStructureBuilder) tested.documentIndexStructureBuilder).riverName);

	}

	@Test
	public void constructor_postprocessors() throws Exception {

		RemoteRiver tested = prepareRiverInstanceForTest("https://issues.jboss.org", null,
				Utils.loadJSONFromJarPackagedFile("/river_configuration_test_preprocessors.json"), false);

		List<StructuredContentPreprocessor> preprocs = ((DocumentWithCommentsIndexStructureBuilder) tested.documentIndexStructureBuilder).issueDataPreprocessors;
		Assert.assertEquals(2, preprocs.size());
		Assert.assertEquals("Status Normalizer", preprocs.get(0).getName());
		Assert.assertEquals("value1", ((DataPreprocessorMock) preprocs.get(0)).settings.get("some_setting_1_1"));
		Assert.assertEquals("value2", ((DataPreprocessorMock) preprocs.get(0)).settings.get("some_setting_1_2"));
		Assert.assertEquals("Issue type Normalizer", preprocs.get(1).getName());
		Assert.assertEquals("value1", ((DataPreprocessorMock) preprocs.get(1)).settings.get("some_setting_2_1"));
		Assert.assertEquals("value2", ((DataPreprocessorMock) preprocs.get(1)).settings.get("some_setting_2_2"));
	}

	@Test
	public void configure() throws Exception {
		RemoteRiver tested = prepareRiverInstanceForTest(null);
		try {
			tested.closed = false;
			tested.configure(null);
			Assert.fail("IllegalStateException must be thrown");
		} catch (IllegalStateException e) {
			// OK
		}
		// do not test configuration read here, it's tested in constructor tests
	}

	@Test
	public void start() throws Exception {
		RemoteRiver tested = prepareRiverInstanceForTest(null);
		try {
			tested.closed = false;
			tested.start();
			Assert.fail("IllegalStateException must be thrown");
		} catch (IllegalStateException e) {
			// OK
		}
		// do not test real start here because it's hardly testable
	}

	@Test
	public void close() throws Exception {

		// case - close all correctly
		RemoteRiver tested = prepareRiverInstanceForTest(null);
		MockThread mockThread = new MockThread();
		tested.coordinatorThread = mockThread;
		tested.coordinatorInstance = mock(ISpaceIndexerCoordinator.class);
		tested.closed = false;
		Assert.assertNotNull(tested.coordinatorThread);
		Assert.assertNotNull(tested.coordinatorInstance);
		RemoteRiver.riverInstances.put(tested.riverName().getName(), tested);
		Assert.assertTrue(RemoteRiver.riverInstances.containsKey(tested.riverName().getName()));

		tested.close();
		Assert.assertTrue(tested.isClosed());
		Assert.assertTrue(mockThread.interruptWasCalled);
		Assert.assertNull(tested.coordinatorThread);
		Assert.assertNull(tested.coordinatorInstance);
		Assert.assertFalse(RemoteRiver.riverInstances.containsKey(tested.riverName().getName()));

		// case - no exception when coordinatorThread and coordinatorInstance is null
		tested = prepareRiverInstanceForTest(null);
		RemoteRiver.riverInstances.put(tested.riverName().getName(), tested);
		tested.coordinatorThread = null;
		tested.coordinatorInstance = null;
		tested.closed = false;
		Assert.assertNull(tested.coordinatorThread);
		Assert.assertNull(tested.coordinatorInstance);

		tested.close();
		Assert.assertTrue(tested.isClosed());
		Assert.assertNull(tested.coordinatorThread);
		Assert.assertNull(tested.coordinatorInstance);
		Assert.assertFalse(RemoteRiver.riverInstances.containsKey(tested.riverName().getName()));

	}

	@Test
	public void stop() throws Exception {

		// case - close all correctly
		RemoteRiver tested = prepareRiverInstanceForTest(null);
		MockThread mockThread = new MockThread();
		tested.coordinatorThread = mockThread;
		tested.coordinatorInstance = mock(ISpaceIndexerCoordinator.class);
		tested.closed = false;
		Assert.assertNotNull(tested.coordinatorThread);
		Assert.assertNotNull(tested.coordinatorInstance);
		RemoteRiver.riverInstances.put(tested.riverName().getName(), tested);
		Assert.assertTrue(RemoteRiver.riverInstances.containsKey(tested.riverName().getName()));

		tested.stop(false);
		Assert.assertTrue(tested.isClosed());
		Assert.assertTrue(mockThread.interruptWasCalled);
		Assert.assertNull(tested.coordinatorThread);
		Assert.assertNull(tested.coordinatorInstance);
		Assert.assertTrue(RemoteRiver.riverInstances.containsKey(tested.riverName().getName()));

		// case - no exception when coordinatorThread and coordinatorInstance is null
		tested = prepareRiverInstanceForTest(null);
		RemoteRiver.riverInstances.put(tested.riverName().getName(), tested);
		tested.coordinatorThread = null;
		tested.coordinatorInstance = null;
		tested.closed = false;
		Assert.assertNull(tested.coordinatorThread);
		Assert.assertNull(tested.coordinatorInstance);

		tested.stop(false);
		Assert.assertTrue(tested.isClosed());
		Assert.assertNull(tested.coordinatorThread);
		Assert.assertNull(tested.coordinatorInstance);
		Assert.assertTrue(RemoteRiver.riverInstances.containsKey(tested.riverName().getName()));
	}

	@Test
	public void stop_permanent() throws Exception {
		RemoteRiver tested = prepareRiverInstanceForTest(null);
		try {
			tested.client = prepareESClientForUnitTest();
			indexCreate(tested.getRiverIndexName());

			// case - not permanent stop
			tested.closed = false;
			tested.stop(false);
			Assert.assertNull(tested.readDatetimeValue(null, RemoteRiver.PERMSTOREPROP_RIVER_STOPPED_PERMANENTLY));
			Assert.assertTrue(tested.isClosed());

			// case - permanent stop
			tested.closed = false;
			tested.stop(true);
			Assert.assertNotNull(tested.readDatetimeValue(null, RemoteRiver.PERMSTOREPROP_RIVER_STOPPED_PERMANENTLY));
			Assert.assertTrue(tested.isClosed());

		} finally {
			finalizeESClientForUnitTest();
		}
	}

	@Test
	public void reconfigure() throws Exception {

		// case - exception when not stopped
		{
			RemoteRiver tested = prepareRiverInstanceForTest(null);
			try {
				tested.closed = false;
				tested.reconfigure();
				Assert.fail("IllegalStateException must be thrown");
			} catch (IllegalStateException e) {
				// OK
			}
		}

		// case - config reload error because no document
		{
			RemoteRiver tested = prepareRiverInstanceForTest(null);
			try {
				tested.client = prepareESClientForUnitTest();
				tested.closed = true;
				indexCreate(tested.getRiverIndexName());
				tested.reconfigure();
				Assert.fail("IllegalStateException must be thrown");
			} catch (IllegalStateException e) {
				// OK
			} finally {
				finalizeESClientForUnitTest();
			}
		}

		// case - config reload performed
		{
			RemoteRiver tested = prepareRiverInstanceForTest(null);
			try {
				tested.client = prepareESClientForUnitTest();
				tested.closed = true;
				tested.client.prepareIndex(tested.getRiverIndexName(), tested.riverName().getName(), "_meta")
						.setSource(TestUtils.readStringFromClasspathFile("/river_reconfiguration_test.json")).execute().actionGet();

				tested.reconfigure();
				Assert.assertEquals("my_remote_index_test", tested.indexName);
				Assert.assertEquals("remote_doc_test", tested.typeName);
				Assert.assertEquals("remote_river_activity_test", tested.activityLogIndexName);
				Assert.assertEquals("remote_river_indexupdate_test", tested.activityLogTypeName);
			} finally {
				finalizeESClientForUnitTest();
			}
		}
	}

	@Test
	public void restart() {
		// TODO unittest
	}

	@Test
	public void getAllIndexedSpacesKeys_FromStaticConfig() throws Exception {
		Map<String, Object> jiraSettings = new HashMap<String, Object>();
		jiraSettings.put("spacesIndexed", "ORG, UUUU, PEM, SU07");

		RemoteRiver tested = prepareRiverInstanceForTest(jiraSettings);
		IRemoteSystemClient jiraClientMock = tested.remoteSystemClient;

		List<String> r = tested.getAllIndexedSpaceKeys();
		Assert.assertEquals(4, r.size());
		Assert.assertEquals("ORG", r.get(0));
		Assert.assertEquals("UUUU", r.get(1));
		Assert.assertEquals("PEM", r.get(2));
		Assert.assertEquals("SU07", r.get(3));
		Assert.assertEquals(Long.MAX_VALUE, tested.allIndexedSpacesKeysNextRefresh);
		verify(jiraClientMock, times(0)).getAllSpaces();
	}

	@Test
	public void getAllIndexedSpacesKeys_FromRemoteNoExcludes() throws Exception {
		Map<String, Object> jiraSettings = new HashMap<String, Object>();
		jiraSettings.put("spaceKeysExcluded", "");

		RemoteRiver tested = prepareRiverInstanceForTest(jiraSettings);
		IRemoteSystemClient jiraClientMock = tested.remoteSystemClient;

		List<String> pl = Utils.parseCsvString("ORG,UUUU,PEM,SU07");
		when(jiraClientMock.getAllSpaces()).thenReturn(pl);

		List<String> r = tested.getAllIndexedSpaceKeys();
		verify(jiraClientMock, times(1)).getAllSpaces();
		Assert.assertEquals(4, r.size());
		Assert.assertEquals("ORG", r.get(0));
		Assert.assertEquals("UUUU", r.get(1));
		Assert.assertEquals("PEM", r.get(2));
		Assert.assertEquals("SU07", r.get(3));
		Assert
				.assertTrue(tested.allIndexedSpacesKeysNextRefresh <= (System.currentTimeMillis() + RemoteRiver.SPACES_REFRESH_TIME));
	}

	@Test
	public void getAllIndexedSpacesKeys_FromRemoteWithExcludes() throws Exception {
		Map<String, Object> jiraSettings = new HashMap<String, Object>();
		jiraSettings.put("spaceKeysExcluded", "PEM,UUUU");

		RemoteRiver tested = prepareRiverInstanceForTest(jiraSettings);
		IRemoteSystemClient jiraClientMock = tested.remoteSystemClient;

		List<String> pl = Utils.parseCsvString("ORG,UUUU,PEM,SU07");
		when(jiraClientMock.getAllSpaces()).thenReturn(pl);

		List<String> r = tested.getAllIndexedSpaceKeys();
		verify(jiraClientMock, times(1)).getAllSpaces();
		Assert.assertEquals(2, r.size());
		Assert.assertEquals("ORG", r.get(0));
		Assert.assertEquals("SU07", r.get(1));
		Assert
				.assertTrue(tested.allIndexedSpacesKeysNextRefresh <= (System.currentTimeMillis() + RemoteRiver.SPACES_REFRESH_TIME));
	}

	@Test
	public void storeDatetimeValueBuildDocument() throws Exception {
		RemoteRiver tested = prepareRiverInstanceForTest(null);

		String dt = "2012-09-30T12:22:44.156Z";
		Assert.assertEquals("{\"propertyName\":\"my_property\",\"value\":\"2012-09-30T14:22:44.156+0200\"}", tested
				.storeDatetimeValueBuildDocument(null, "my_property", DateTimeUtils.parseISODateTime(dt)).string());
		Assert.assertEquals(
				"{\"spaceKey\":\"AAA\",\"propertyName\":\"my_property\",\"value\":\"2012-09-30T14:22:44.156+0200\"}", tested
						.storeDatetimeValueBuildDocument("AAA", "my_property", DateTimeUtils.parseISODateTime(dt)).string());
	}

	@Test
	public void readAndStoreAndDeleteDatetimeValue() throws Exception {
		try {
			Client client = prepareESClientForUnitTest();

			RemoteRiver tested = prepareRiverInstanceForTest(null);
			tested.client = client;

			indexCreate("_river");

			Assert.assertFalse(tested.deleteDatetimeValue("ORG1", "testProperty_1_1"));

			tested
					.storeDatetimeValue("ORG1", "testProperty_1_1", DateTimeUtils.parseISODateTime("2012-09-03T18:12:45"), null);
			tested
					.storeDatetimeValue("ORG1", "testProperty_1_2", DateTimeUtils.parseISODateTime("2012-09-03T05:12:40"), null);
			tested
					.storeDatetimeValue("ORG2", "testProperty_1_1", DateTimeUtils.parseISODateTime("2012-09-02T08:12:30"), null);
			tested
					.storeDatetimeValue("ORG2", "testProperty_1_2", DateTimeUtils.parseISODateTime("2012-09-02T05:02:20"), null);

			Assert.assertEquals(DateTimeUtils.parseISODateTime("2012-09-03T18:12:45"),
					tested.readDatetimeValue("ORG1", "testProperty_1_1"));
			Assert.assertEquals(DateTimeUtils.parseISODateTime("2012-09-03T05:12:40"),
					tested.readDatetimeValue("ORG1", "testProperty_1_2"));
			Assert.assertEquals(DateTimeUtils.parseISODateTime("2012-09-02T08:12:30"),
					tested.readDatetimeValue("ORG2", "testProperty_1_1"));
			Assert.assertEquals(DateTimeUtils.parseISODateTime("2012-09-02T05:02:20"),
					tested.readDatetimeValue("ORG2", "testProperty_1_2"));

			Assert.assertTrue(tested.deleteDatetimeValue("ORG1", "testProperty_1_1"));
			Assert.assertNull(tested.readDatetimeValue("ORG1", "testProperty_1_1"));
			Assert.assertEquals(DateTimeUtils.parseISODateTime("2012-09-03T05:12:40"),
					tested.readDatetimeValue("ORG1", "testProperty_1_2"));
			Assert.assertEquals(DateTimeUtils.parseISODateTime("2012-09-02T08:12:30"),
					tested.readDatetimeValue("ORG2", "testProperty_1_1"));
			Assert.assertEquals(DateTimeUtils.parseISODateTime("2012-09-02T05:02:20"),
					tested.readDatetimeValue("ORG2", "testProperty_1_2"));

			Assert.assertTrue(tested.deleteDatetimeValue("ORG1", "testProperty_1_2"));
			Assert.assertNull(tested.readDatetimeValue("ORG1", "testProperty_1_2"));
			Assert.assertEquals(DateTimeUtils.parseISODateTime("2012-09-02T08:12:30"),
					tested.readDatetimeValue("ORG2", "testProperty_1_1"));
			Assert.assertEquals(DateTimeUtils.parseISODateTime("2012-09-02T05:02:20"),
					tested.readDatetimeValue("ORG2", "testProperty_1_2"));

		} finally {
			finalizeESClientForUnitTest();
		}
	}

	@Test
	public void storeDatetimeValue_Bulk() throws Exception {
		RemoteRiver tested = prepareRiverInstanceForTest(null);

		BulkRequestBuilder esBulk = new BulkRequestBuilder(null);
		tested.storeDatetimeValue("ORG", "prop", new Date(), esBulk);
		tested.storeDatetimeValue("ORG", "prop2", new Date(), esBulk);
		tested.storeDatetimeValue("ORG", "prop3", new Date(), esBulk);

		Assert.assertEquals(3, esBulk.numberOfActions());

	}

	@Test
	public void prepareValueStoreDocumentName() {
		Assert.assertEquals("_lastupdatedissue_ORG", RemoteRiver.prepareValueStoreDocumentName("ORG", "lastupdatedissue"));
		Assert.assertEquals("_lastupdatedissue", RemoteRiver.prepareValueStoreDocumentName(null, "lastupdatedissue"));
	}

	@Test
	public void prepareESBulkRequestBuilder() throws Exception {
		RemoteRiver tested = prepareRiverInstanceForTest(null);
		Client clientMock = tested.client;
		when(clientMock.prepareBulk()).thenReturn(new BulkRequestBuilder(null));
		Assert.assertNotNull(tested.prepareESBulkRequestBuilder());
		verify(clientMock, times(1)).prepareBulk();
	}

	@Test
	public void reportIndexingFinished() throws Exception {
		ISpaceIndexerCoordinator coordMock = mock(ISpaceIndexerCoordinator.class);

		RemoteRiver tested = prepareRiverInstanceForTest(null);
		Client clientMock = tested.client;
		tested.coordinatorInstance = coordMock;

		// case - report correctly - no activity log
		{
			tested.reportIndexingFinished(new SpaceIndexingInfo("ORG", false, 10, 0, 0, null, true, 10, null));
			verify(coordMock, times(1)).reportIndexingFinished("ORG", true, false);
			Mockito.verifyZeroInteractions(clientMock);
		}
		{
			reset(coordMock);
			tested.reportIndexingFinished(new SpaceIndexingInfo("AAA", true, 10, 0, 0, null, false, 10, null));
			verify(coordMock, times(1)).reportIndexingFinished("AAA", false, true);
			Mockito.verifyZeroInteractions(clientMock);
		}

		// report correctly with activity log
		tested.activityLogIndexName = "alindex";
		tested.activityLogTypeName = "altype";
		{
			IndexRequestBuilder irb = new IndexRequestBuilder(null);
			when(clientMock.prepareIndex("alindex", "altype")).thenReturn(irb);
			tested.reportIndexingFinished(new SpaceIndexingInfo("ORG", false, 10, 0, 0, null, true, 10, null));
			Assert.assertNotNull(irb.request().source());
		}

		// case - no exception if coordinatorInstance is null
		tested = prepareRiverInstanceForTest(null);
		tested.reportIndexingFinished(new SpaceIndexingInfo("ORG", false, 10, 0, 0, null, true, 10, null));

	}

	@Test
	public void prepareESScrollSearchRequestBuilder() throws Exception {
		RemoteRiver tested = prepareRiverInstanceForTest(null);
		Client clientMock = tested.client;

		SearchRequestBuilder srb = new SearchRequestBuilder(null);
		when(clientMock.prepareSearch("myIndex")).thenReturn(srb);

		tested.prepareESScrollSearchRequestBuilder("myIndex");

		Assert.assertNotNull(srb.request().scroll());
		Assert.assertEquals(SearchType.SCAN, srb.request().searchType());
		verify(clientMock).prepareSearch("myIndex");
		Mockito.verifyNoMoreInteractions(clientMock);

	}

	@Test
	public void getRiverOperationInfo_activityLogDisabled() throws Exception {

		RemoteRiver tested = prepareRiverInstanceForTest(null);

		ISpaceIndexerCoordinator coordMock = mock(ISpaceIndexerCoordinator.class);
		tested.coordinatorInstance = coordMock;

		List<SpaceIndexingInfo> currentIndexings = new ArrayList<SpaceIndexingInfo>();
		currentIndexings.add(new SpaceIndexingInfo("ORG", true, 256, 10, 0, DateTimeUtils
				.parseISODateTime("2012-09-27T09:21:25.422Z"), false, 0, null));
		currentIndexings.add(new SpaceIndexingInfo("AAA", false, 15, 0, 0, DateTimeUtils
				.parseISODateTime("2012-09-27T09:21:24.422Z"), false, 0, null));
		when(coordMock.getCurrentSpaceIndexingInfo()).thenReturn(currentIndexings);

		tested.allIndexedSpacesKeysNextRefresh = Long.MAX_VALUE;
		tested.allIndexedSpacesKeys = new ArrayList<String>();
		tested.allIndexedSpacesKeys.add("ORG");
		tested.allIndexedSpacesKeys.add("AAA");
		tested.allIndexedSpacesKeys.add("JJJ");
		tested.allIndexedSpacesKeys.add("FFF");

		tested.lastSpaceIndexingInfo.put("ORG",
				new SpaceIndexingInfo("ORG", true, 125, 10, 0, DateTimeUtils.parseISODateTime("2012-09-27T09:15:25.422Z"),
						true, 1500, null));
		tested.lastSpaceIndexingInfo.put("JJJ",
				new SpaceIndexingInfo("JJJ", false, 12, 0, 0, DateTimeUtils.parseISODateTime("2012-09-27T09:12:25.422Z"),
						false, 1800, "JIRA timeout"));

		// case - nothing stored in audit log index - no exception!
		String info = tested.getRiverOperationInfo(new DiscoveryNode("My Node", "fsdfsdfxzd",
				DummyTransportAddress.INSTANCE, new HashMap<String, String>()), DateTimeUtils
				.parseISODateTime("2012-09-27T09:21:26.422Z"));
		TestUtils.assertStringFromClasspathFile("/asserts/RemoteRiver_getRiverOperationInfo_1.json", info);

	}

	@Test
	public void getRiverOperationInfo_activityLogEnabled() throws Exception {
		try {

			Client client = prepareESClientForUnitTest();

			RemoteRiver tested = prepareRiverInstanceForTest(null);
			tested.client = client;
			tested.activityLogIndexName = "activity_log_index";
			tested.activityLogTypeName = "remote_river_indexupdate";

			ISpaceIndexerCoordinator coordMock = mock(ISpaceIndexerCoordinator.class);
			tested.coordinatorInstance = coordMock;

			List<SpaceIndexingInfo> currentIndexings = new ArrayList<SpaceIndexingInfo>();
			currentIndexings.add(new SpaceIndexingInfo("ORG", true, 256, 10, 0, DateTimeUtils
					.parseISODateTime("2012-09-27T09:21:25.422Z"), false, 0, null));
			currentIndexings.add(new SpaceIndexingInfo("AAA", false, 15, 0, 0, DateTimeUtils
					.parseISODateTime("2012-09-27T09:21:24.422Z"), false, 0, null));
			when(coordMock.getCurrentSpaceIndexingInfo()).thenReturn(currentIndexings);

			tested.allIndexedSpacesKeysNextRefresh = Long.MAX_VALUE;
			tested.allIndexedSpacesKeys = new ArrayList<String>();
			tested.allIndexedSpacesKeys.add("ORG");
			tested.allIndexedSpacesKeys.add("AAA");
			tested.allIndexedSpacesKeys.add("JJJ");
			tested.allIndexedSpacesKeys.add("FFF");

			tested.lastSpaceIndexingInfo.put("ORG",
					new SpaceIndexingInfo("ORG", true, 125, 10, 0, DateTimeUtils.parseISODateTime("2012-09-27T09:15:25.422Z"),
							true, 1500, null));
			tested.lastSpaceIndexingInfo.put("JJJ",
					new SpaceIndexingInfo("JJJ", false, 12, 0, 0, DateTimeUtils.parseISODateTime("2012-09-27T09:12:25.422Z"),
							false, 1800, "JIRA timeout"));

			// case - nothing stored in audit log index - no exception!
			String info = tested.getRiverOperationInfo(new DiscoveryNode("My Node", "fsdfsdfxzd",
					DummyTransportAddress.INSTANCE, new HashMap<String, String>()), DateTimeUtils
					.parseISODateTime("2012-09-27T09:21:26.422Z"));
			TestUtils.assertStringFromClasspathFile("/asserts/RemoteRiver_getRiverOperationInfo_1.json", info);

			// case - last indexed record into ES index for FFF space found
			indexCreate(tested.activityLogIndexName);
			client.admin().indices().preparePutMapping(tested.activityLogIndexName).setType(tested.activityLogTypeName)
					.setSource(TestUtils.readStringFromClasspathFile("/examples/remote_river_indexupdate.json")).execute()
					.actionGet();

			tested.writeActivityLogRecord(new SpaceIndexingInfo("FFF", false, 12, 0, 0, DateTimeUtils
					.parseISODateTime("2012-09-27T08:10:25.422Z"), true, 181, null));
			tested.writeActivityLogRecord(new SpaceIndexingInfo("FFF", false, 125, 0, 0, DateTimeUtils
					.parseISODateTime("2012-09-27T08:11:25.422Z"), true, 1810, null));
			tested.refreshSearchIndex(tested.activityLogIndexName);
			info = tested.getRiverOperationInfo(new DiscoveryNode("My Node", "fsdfsdfxzd", DummyTransportAddress.INSTANCE,
					new HashMap<String, String>()), DateTimeUtils.parseISODateTime("2012-09-27T09:21:26.422Z"));
			TestUtils.assertStringFromClasspathFile("/asserts/RemoteRiver_getRiverOperationInfo_2.json", info);

		} finally {
			finalizeESClientForUnitTest();
		}

	}

	@Test
	public void forceFullReindex() throws Exception {

		RemoteRiver tested = prepareRiverInstanceForTest(null);
		ISpaceIndexerCoordinator coordinatorMock = mock(ISpaceIndexerCoordinator.class);
		tested.coordinatorInstance = coordinatorMock;

		// case - all spaces but no any exists
		{
			tested.allIndexedSpacesKeys = null;
			Assert.assertEquals("", tested.forceFullReindex(null));
			Mockito.verifyNoMoreInteractions(coordinatorMock);
		}

		// case - all spaces and some exists
		{
			reset(coordinatorMock);
			tested.allIndexedSpacesKeys = new ArrayList<String>();
			tested.allIndexedSpacesKeys.add("ORG");
			tested.allIndexedSpacesKeys.add("AAA");
			Assert.assertEquals("ORG,AAA", tested.forceFullReindex(null));
			verify(coordinatorMock).forceFullReindex("ORG");
			verify(coordinatorMock).forceFullReindex("AAA");
			Mockito.verifyNoMoreInteractions(coordinatorMock);
		}

		// case - one space not exists
		{
			reset(coordinatorMock);
			Assert.assertNull(tested.forceFullReindex("BBB"));
			Mockito.verifyNoMoreInteractions(coordinatorMock);

		}

		// case - one space which exists
		{
			reset(coordinatorMock);
			Assert.assertEquals("ORG", tested.forceFullReindex("ORG"));
			verify(coordinatorMock).forceFullReindex("ORG");
			Mockito.verifyNoMoreInteractions(coordinatorMock);

			reset(coordinatorMock);
			Assert.assertEquals("AAA", tested.forceFullReindex("AAA"));
			verify(coordinatorMock).forceFullReindex("AAA");
			Mockito.verifyNoMoreInteractions(coordinatorMock);
		}
	}

	/**
	 * Prepare {@link RemoteRiver} instance for unit test, with Mockito moceked jiraClient and elasticSearchClient.
	 * 
	 * @param remoteSettingsAdd additional/optional config properties to be added into <code>jira</code> configuration
	 *          node
	 * @return instance for tests
	 * @throws Exception from constructor
	 */
	protected RemoteRiver prepareRiverInstanceForTest(Map<String, Object> remoteSettingsAdd) throws Exception {
		Map<String, Object> topLevelSettings = new HashMap<String, Object>();
		extendToplevelSettingsByMandatoryIndexSettings(topLevelSettings);

		return prepareRiverInstanceForTest("https://issues.jboss.org", remoteSettingsAdd, topLevelSettings, true);
	}

	@SuppressWarnings("unchecked")
	private void extendToplevelSettingsByMandatoryIndexSettings(Map<String, Object> topLevelSettings) {
		// fill some mandatory fields for index part not loaded from default
		Map<String, Object> settings = (Map<String, Object>) topLevelSettings.get("index");
		if (settings == null) {
			settings = new HashMap<String, Object>();
			topLevelSettings.put("index", settings);
		}
		settings.put(DocumentWithCommentsIndexStructureBuilder.CONFIG_FIELDS, new HashMap<String, Object>());
		settings.put(DocumentWithCommentsIndexStructureBuilder.CONFIG_FILTERS, new HashMap<String, Object>());
		settings.put(DocumentWithCommentsIndexStructureBuilder.CONFIG_REMOTEFIELD_DOCUMENTID, "docid");
		settings.put(DocumentWithCommentsIndexStructureBuilder.CONFIG_REMOTEFIELD_UPDATED, "up");
	}

	/**
	 * Prepare {@link RemoteRiver} instance for unit test, with Mockito moceked jiraClient and elasticSearchClient.
	 * 
	 * @param urlGetDocuments parameter for remote settings
	 * @param remoteSettingsAdd additional/optional config properties to be added into <code>remote</code> configuration
	 *          node
	 * @param toplevelSettingsAdd additional/optional config properties to be added into toplevel node. Do not add
	 *          <code>remote</code> here, will be ignored.
	 * @param initRemoteClientMock if set to true then Mockito mock instance is created and set into
	 *          {@link RemoteRiver#remoteSystemClient}
	 * @return instance for tests
	 * @throws Exception from constructor
	 */
	public static RemoteRiver prepareRiverInstanceForTest(String urlGetDocuments, Map<String, Object> remoteSettingsAdd,
			Map<String, Object> toplevelSettingsAdd, boolean initRemoteClientMock) throws Exception {
		Map<String, Object> settings = new HashMap<String, Object>();
		if (toplevelSettingsAdd != null)
			settings.putAll(toplevelSettingsAdd);
		if (urlGetDocuments != null || remoteSettingsAdd != null) {
			Map<String, Object> remoteSettings = new HashMap<String, Object>();
			settings.put("remote", remoteSettings);
			if (remoteSettingsAdd != null)
				remoteSettings.putAll(remoteSettingsAdd);
			remoteSettings.put(GetJSONClient.CFG_URL_GET_DOCUMENTS, urlGetDocuments);
			remoteSettings.put(GetJSONClient.CFG_URL_GET_SPACES, urlGetDocuments);
		}

		Settings gs = mock(Settings.class);
		RiverSettings rs = new RiverSettings(gs, settings);
		Client clientMock = mock(Client.class);
		RemoteRiver tested = new RemoteRiver(new RiverName("remote", "my_remote_river"), rs, clientMock);
		if (initRemoteClientMock) {
			IRemoteSystemClient remoteClientMock = mock(IRemoteSystemClient.class);
			tested.remoteSystemClient = remoteClientMock;
		}
		return tested;
	}

}

package org.jboss.elasticsearch.river.remote;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.jboss.elasticsearch.tools.content.StructuredContentPreprocessorFactory;

/**
 * Remote River implementation class.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class RemoteRiver extends AbstractRiverComponent implements River, IESIntegration, IRiverMgm, IPwdLoader {

	/**
	 * Map of running river instances. Used for management operations dispatching. See {@link #getRunningInstance(String)}
	 */
	protected static Map<String, IRiverMgm> riverInstances = new HashMap<String, IRiverMgm>();

	/**
	 * Name of datetime property where permanent indexing stop date is stored
	 * 
	 * @see #storeDatetimeValue(String, String, Date, BulkRequestBuilder)
	 * @see #readDatetimeValue(String, String)
	 */
	protected static final String PERMSTOREPROP_RIVER_STOPPED_PERMANENTLY = "river_stopped_permanently";

	/**
	 * How often is Space list refreshed from remote system instance [ms].
	 */
	protected static final long SPACES_REFRESH_TIME = 30 * 60 * 1000;

	public static final String INDEX_DOCUMENT_TYPE_NAME_DEFAULT = "remote_document";

	public static final String INDEX_ACTIVITY_TYPE_NAME_DEFAULT = "remote_river_indexupdate";

	/**
	 * ElasticSearch client to be used for indexing
	 */
	protected Client client;

	/**
	 * Configured client to access data from remote system
	 */
	protected IRemoteSystemClient remoteSystemClient;

	/**
	 * Configured index structure builder to be used.
	 */
	protected IDocumentIndexStructureBuilder documentIndexStructureBuilder;

	/**
	 * Config - maximal number of parallel indexing threads
	 */
	protected int maxIndexingThreads;

	/**
	 * Config - index update period [ms]
	 */
	protected long indexUpdatePeriod;

	/**
	 * Config - index full update period [ms]
	 */
	protected long indexFullUpdatePeriod = -1;

	/**
	 * Config - name of ElasticSearch index used to store documents from this river
	 */
	protected String indexName;

	/**
	 * Config - name of ElasticSearch type used to store documents from this river in index
	 */
	protected String typeName;

	/**
	 * Config - name of ElasticSearch index used to store river activity records - null means no activity stored
	 */
	protected String activityLogIndexName;

	/**
	 * Config - name of ElasticSearch type used to store river activity records in index
	 */
	protected String activityLogTypeName;

	/**
	 * Thread running {@link ISpaceIndexerCoordinator} is stored here.
	 */
	protected Thread coordinatorThread;

	/**
	 * USed {@link ISpaceIndexerCoordinator} instance is stored here.
	 */
	protected ISpaceIndexerCoordinator coordinatorInstance;

	/**
	 * Flag set to true if this river is stopped from ElasticSearch server.
	 */
	protected volatile boolean closed = true;

	/**
	 * List of indexing excluded Space keys loaded from river configuration
	 * 
	 * @see #getAllIndexedSpaceKeys()
	 */
	protected List<String> spaceKeysExcluded = null;

	/**
	 * List of all Space keys to be indexed. Loaded from river configuration, or from remote system (excludes removed)
	 * 
	 * @see #getAllIndexedSpaceKeys()
	 */
	protected List<String> allIndexedSpacesKeys = null;

	/**
	 * Next time when {@link #allIndexedSpacesKeys} need to be refreshed from remote system.
	 * 
	 * @see #getAllIndexedSpaceKeys()
	 */
	protected long allIndexedSpacesKeysNextRefresh = 0;

	/**
	 * Last Space indexing info store. Key in map is Space key.
	 */
	protected Map<String, SpaceIndexingInfo> lastSpaceIndexingInfo = new HashMap<String, SpaceIndexingInfo>();

	/**
	 * Date of last restart of this river.
	 */
	protected Date lastRestartDate;

	/**
	 * Timestamp of permanent stop of this river.
	 */
	protected Date permanentStopDate;

	/**
	 * Public constructor used by ElasticSearch.
	 * 
	 * @param riverName
	 * @param settings
	 * @param client
	 * @throws MalformedURLException
	 */
	@Inject
	public RemoteRiver(RiverName riverName, RiverSettings settings, Client client) throws MalformedURLException {
		super(riverName, settings);
		this.client = client;
		configure(settings.settings());
	}

	/**
	 * Configure the river.
	 * 
	 * @param settings used for configuration.
	 */
	@SuppressWarnings({ "unchecked" })
	protected void configure(Map<String, Object> settings) {

		if (!closed)
			throw new IllegalStateException("Remote River must be stopped to configure it!");

		if (settings.containsKey("remote")) {
			Map<String, Object> remoteSettings = (Map<String, Object>) settings.get("remote");
			maxIndexingThreads = XContentMapValues.nodeIntegerValue(remoteSettings.get("maxIndexingThreads"), 1);
			indexUpdatePeriod = Utils.parseTimeValue(remoteSettings, "indexUpdatePeriod", 5, TimeUnit.MINUTES);
			indexFullUpdatePeriod = Utils.parseTimeValue(remoteSettings, "indexFullUpdatePeriod", 12, TimeUnit.HOURS);
			if (remoteSettings.containsKey("spacesIndexed")) {
				allIndexedSpacesKeys = Utils.parseCsvString(XContentMapValues.nodeStringValue(
						remoteSettings.get("spacesIndexed"), null));
				if (allIndexedSpacesKeys != null) {
					// stop spaces loading from remote system
					allIndexedSpacesKeysNextRefresh = Long.MAX_VALUE;
				}
			}
			if (remoteSettings.containsKey("spaceKeysExcluded")) {
				spaceKeysExcluded = Utils.parseCsvString(XContentMapValues.nodeStringValue(
						remoteSettings.get("spaceKeysExcluded"), null));
			}
			String remoteClientClass = Utils.trimToNull(XContentMapValues.nodeStringValue(
					remoteSettings.get("remoteClientClass"), null));
			if (remoteClientClass != null) {
				try {
					remoteSystemClient = (IRemoteSystemClient) Class.forName(remoteClientClass).newInstance();
				} catch (Exception e) {
					throw new SettingsException("Unable to instantiate class defined by 'remote/remoteClientClass': "
							+ e.getMessage());
				}
			} else {
				remoteSystemClient = new GetJSONClient();
			}
			remoteSystemClient.init(remoteSettings, allIndexedSpacesKeysNextRefresh != Long.MAX_VALUE, this);
		} else {
			throw new SettingsException("'remote' element of river configuration structure not found");
		}

		Map<String, Object> indexSettings = null;
		if (settings.containsKey("index")) {
			indexSettings = (Map<String, Object>) settings.get("index");
			indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
			typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), INDEX_DOCUMENT_TYPE_NAME_DEFAULT);
		} else {
			indexName = riverName.name();
			typeName = INDEX_DOCUMENT_TYPE_NAME_DEFAULT;
		}

		Map<String, Object> activityLogSettings = null;
		if (settings.containsKey("activity_log")) {
			activityLogSettings = (Map<String, Object>) settings.get("activity_log");
			activityLogIndexName = Utils
					.trimToNull(XContentMapValues.nodeStringValue(activityLogSettings.get("index"), null));
			if (activityLogIndexName == null) {
				throw new SettingsException(
						"'activity_log/index' element of river configuration structure must be defined with some string");
			}
			activityLogTypeName = Utils.trimToNull(XContentMapValues.nodeStringValue(activityLogSettings.get("type"),
					INDEX_ACTIVITY_TYPE_NAME_DEFAULT));
		}

		documentIndexStructureBuilder = new DocumentWithCommentsIndexStructureBuilder(riverName.getName(), indexName,
				typeName, indexSettings);
		preparePreprocessors(indexSettings, documentIndexStructureBuilder);

		remoteSystemClient.setIndexStructureBuilder(documentIndexStructureBuilder);

		logger.info("Configured Remote River '{}'. Search index name '{}', document type for issues '{}'.",
				riverName.getName(), indexName, typeName);
		if (activityLogIndexName != null) {
			logger
					.info(
							"Activity log for Remote River '{}' is enabled. Search index name '{}', document type for index updates '{}'.",
							riverName.getName(), activityLogIndexName, activityLogTypeName);
		}
	}

	@SuppressWarnings("unchecked")
	private void preparePreprocessors(Map<String, Object> indexSettings,
			IDocumentIndexStructureBuilder indexStructureBuilder) {
		if (indexSettings != null) {
			List<Map<String, Object>> preproclist = (List<Map<String, Object>>) indexSettings.get("preprocessors");
			if (preproclist != null && preproclist.size() > 0) {
				for (Map<String, Object> ppc : preproclist) {
					try {
						indexStructureBuilder.addDataPreprocessor(StructuredContentPreprocessorFactory.createPreprocessor(ppc,
								client));
					} catch (IllegalArgumentException e) {
						throw new SettingsException(e.getMessage(), e);
					}
				}
			}
		}
	}

	/**
	 * Constructor for unit tests, nothing is initialized/configured in river.
	 * 
	 * @param riverName
	 * @param settings
	 */
	protected RemoteRiver(RiverName riverName, RiverSettings settings) {
		super(riverName, settings);
	}

	@Override
	public synchronized void start() {
		if (!closed)
			throw new IllegalStateException("Can't start already running river");
		logger.info("starting Remote River");
		synchronized (riverInstances) {
			addRunningInstance(this);
		}
		refreshSearchIndex(getRiverIndexName());
		try {
			if ((permanentStopDate = readDatetimeValue(null, PERMSTOREPROP_RIVER_STOPPED_PERMANENTLY)) != null) {
				logger
						.info("Remote River indexing process not started because stopped permanently, you can restart it over management REST API");
				return;
			}
		} catch (IOException e) {
			// OK, we will start river
		}
		logger.info("starting Remote River indexing process");
		closed = false;
		lastRestartDate = new Date();
		coordinatorInstance = new SpaceIndexerCoordinator(remoteSystemClient, this, documentIndexStructureBuilder,
				indexUpdatePeriod, maxIndexingThreads, indexFullUpdatePeriod);
		coordinatorThread = acquireIndexingThread("remote_river_coordinator", coordinatorInstance);
		coordinatorThread.start();
	}

	@Override
	public synchronized void close() {
		logger.info("closing Remote River on this node");
		closed = true;
		if (coordinatorThread != null) {
			coordinatorThread.interrupt();
		}
		// free instances created in #start()
		coordinatorThread = null;
		coordinatorInstance = null;
		synchronized (riverInstances) {
			riverInstances.remove(riverName().getName());
		}
	}

	/**
	 * Stop remote river, but leave instance existing in {@link #riverInstances} so it can be found over management REST
	 * calls and/or reconfigured and started later again. Note that standard ES river {@link #close()} method
	 * implementation removes river instance from {@link #riverInstances}.
	 * 
	 * @param permanent set to true if info about river stopped can be persisted
	 */
	@Override
	public synchronized void stop(boolean permanent) {
		logger.info("stopping Remote River indexing process");
		closed = true;
		if (coordinatorThread != null) {
			coordinatorThread.interrupt();
		}
		// free instances created in #start()
		coordinatorThread = null;
		coordinatorInstance = null;
		if (permanent) {
			try {
				permanentStopDate = new Date();
				storeDatetimeValue(null, PERMSTOREPROP_RIVER_STOPPED_PERMANENTLY, permanentStopDate, null);
				refreshSearchIndex(getRiverIndexName());
				logger.info("Remote River indexing process stopped permanently, you can restart it over management REST API");
			} catch (IOException e) {
				logger.warn("Permanent stopped value storing failed {}", e.getMessage());
			}
		}
	}

	/**
	 * Reconfigure the river. Must be stopped!
	 */
	public synchronized void reconfigure() {
		if (!closed)
			throw new IllegalStateException("Remote River must be stopped to reconfigure it!");

		logger.info("reconfiguring Remote River");
		String riverIndexName = getRiverIndexName();
		refreshSearchIndex(riverIndexName);
		GetResponse resp = client.prepareGet(riverIndexName, riverName().name(), "_meta").execute().actionGet();
		if (resp.exists()) {
			if (logger.isDebugEnabled()) {
				logger.debug("Configuration document: {}", resp.getSourceAsString());
			}
			Map<String, Object> newset = resp.getSource();
			configure(newset);
		} else {
			throw new IllegalStateException("Configuration document not found to reconfigure remote river "
					+ riverName().name());
		}
	}

	/**
	 * Restart the river. Configuration of river is updated.
	 */
	@Override
	public synchronized void restart() {
		logger.info("restarting Remote River");
		boolean cleanPermanent = true;
		if (!closed) {
			cleanPermanent = false;
			stop(false);
			// wait a while to allow currently running indexers to finish??
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				return;
			}
		} else {
			logger.debug("stopped already");
		}
		reconfigure();
		if (cleanPermanent) {
			deleteDatetimeValue(null, PERMSTOREPROP_RIVER_STOPPED_PERMANENTLY);
		}
		start();
		logger.info("Remote River restarted");
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	/**
	 * Force full index update for some Space(s) in this river. Used for REST management operations handling.
	 * 
	 * @param spaceKey optional key of space to reindex, if null or empty then all spaces are forced to full reindex
	 * @return CSV list of spaces forced to reindex. <code>null</code> if space passed over <code>spaceKey</code>
	 *         parameter was not found in this indexer
	 * @throws Exception
	 */
	@Override
	public String forceFullReindex(String spaceKey) throws Exception {
		if (coordinatorInstance == null)
			return null;
		List<String> pkeys = getAllIndexedSpaceKeys();
		if (Utils.isEmpty(spaceKey)) {
			if (pkeys != null) {
				for (String k : pkeys) {
					coordinatorInstance.forceFullReindex(k);
				}
				return Utils.createCsvString(pkeys);
			} else {
				return "";
			}

		} else {
			if (pkeys != null && pkeys.contains(spaceKey)) {
				coordinatorInstance.forceFullReindex(spaceKey);
				return spaceKey;
			} else {
				return null;
			}
		}
	}

	/**
	 * Get info about current operation of this river. Used for REST management operations handling.
	 * 
	 * @return String with JSON formatted info.
	 * @throws Exception
	 */
	@Override
	public String getRiverOperationInfo(DiscoveryNode esNode, Date currentDate) throws Exception {

		XContentBuilder builder = jsonBuilder().prettyPrint();
		builder.startObject();
		builder.field("river_name", riverName().getName());
		builder.field("info_date", currentDate);
		builder.startObject("indexing");
		builder.field("state", closed ? "stopped" : "running");
		if (!closed)
			builder.field("last_restart", lastRestartDate);
		else if (permanentStopDate != null)
			builder.field("stopped_permanently", permanentStopDate);
		builder.endObject();
		if (esNode != null) {
			builder.startObject("node");
			builder.field("id", esNode.getId());
			builder.field("name", esNode.getName());
			builder.endObject();
		}
		if (coordinatorInstance != null) {
			List<SpaceIndexingInfo> currProjectIndexingInfo = coordinatorInstance.getCurrentSpaceIndexingInfo();
			if (currProjectIndexingInfo != null) {
				builder.startArray("current_indexing");
				for (SpaceIndexingInfo pi : currProjectIndexingInfo) {
					pi.buildDocument(builder, true, false);
				}
				builder.endArray();
			}
		}
		List<String> pkeys = getAllIndexedSpaceKeys();
		if (pkeys != null) {
			builder.startArray("indexed_spaces");
			for (String spaceKey : pkeys) {
				builder.startObject();
				builder.field("space_key", spaceKey);
				SpaceIndexingInfo lastIndexing = getLastSpaceIndexingInfo(spaceKey);
				if (lastIndexing != null) {
					builder.field("last_indexing");
					lastIndexing.buildDocument(builder, false, true);
				}
				builder.endObject();
			}
			builder.endArray();
		}
		builder.endObject();
		return builder.string();
	}

	/**
	 * @param spaceKey to get info for
	 * @return spaces indexing info or null if not found.
	 */
	protected SpaceIndexingInfo getLastSpaceIndexingInfo(String spaceKey) {
		SpaceIndexingInfo lastIndexing = lastSpaceIndexingInfo.get(spaceKey);
		if (lastIndexing == null && activityLogIndexName != null) {
			try {
				refreshSearchIndex(activityLogIndexName);
				SearchResponse sr = client.prepareSearch(activityLogIndexName).setTypes(activityLogTypeName)
						.setFilter(FilterBuilders.termFilter(SpaceIndexingInfo.DOCFIELD_SPACE_KEY, spaceKey))
						.setQuery(QueryBuilders.matchAllQuery()).addSort(SpaceIndexingInfo.DOCFIELD_START_DATE, SortOrder.DESC)
						.addField("_source").setSize(1).execute().actionGet();
				if (sr.hits().getTotalHits() > 0) {
					SearchHit hit = sr.hits().getAt(0);
					lastIndexing = SpaceIndexingInfo.readFromDocument(hit.sourceAsMap());
				} else {
					logger.debug("No last indexing info found in activity log for space {}", spaceKey);
				}
			} catch (Exception e) {
				logger.warn("Error during LastSpaceIndexingInfo reading from activity log ES index: {} {}", e.getClass()
						.getName(), e.getMessage());
			}
		}
		return lastIndexing;
	}

	/**
	 * Get running instance of remote river for given name. Used for REST management operations handling.
	 * 
	 * @param riverName to get instance for
	 * @return river instance or null if not found
	 * @see #addRunningInstance(IRiverMgm)
	 * @see #getRunningInstances()
	 */
	public static IRiverMgm getRunningInstance(String riverName) {
		if (riverName == null)
			return null;
		return riverInstances.get(riverName);
	}

	/**
	 * Put running instance of remote river into registry. Used for REST management operations handling.
	 * 
	 * @param riverName to get instance for
	 * @see #getRunningInstances()
	 * @see #getRunningInstance(String)
	 */
	public static void addRunningInstance(IRiverMgm remoteRiver) {
		riverInstances.put(remoteRiver.riverName().getName(), remoteRiver);
	}

	/**
	 * Get running instances of all remote rivers. Used for REST management operations handling.
	 * 
	 * @return Set with names of all remote river instances registered for management
	 * @see #addRunningInstance(IRiverMgm)
	 * @see #getRunningInstance(String)
	 */
	public static Set<String> getRunningInstances() {
		return Collections.unmodifiableSet((riverInstances.keySet()));
	}

	@Override
	public List<String> getAllIndexedSpaceKeys() throws Exception {
		if (allIndexedSpacesKeys == null || allIndexedSpacesKeysNextRefresh < System.currentTimeMillis()) {
			allIndexedSpacesKeys = remoteSystemClient.getAllSpaces();
			if (spaceKeysExcluded != null) {
				allIndexedSpacesKeys.removeAll(spaceKeysExcluded);
			}
			allIndexedSpacesKeysNextRefresh = System.currentTimeMillis() + SPACES_REFRESH_TIME;
		}

		return allIndexedSpacesKeys;
	}

	@Override
	public void reportIndexingFinished(SpaceIndexingInfo indexingInfo) {
		lastSpaceIndexingInfo.put(indexingInfo.spaceKey, indexingInfo);
		if (coordinatorInstance != null) {
			try {
				coordinatorInstance.reportIndexingFinished(indexingInfo.spaceKey, indexingInfo.finishedOK,
						indexingInfo.fullUpdate);
			} catch (Exception e) {
				logger.warn("Indexing finished reporting to coordinator failed due {}", e.getMessage());
			}
		}
		writeActivityLogRecord(indexingInfo);
	}

	/**
	 * Write indexing info into activity log if enabled.
	 * 
	 * @param indexingInfo to write
	 */
	protected void writeActivityLogRecord(SpaceIndexingInfo indexingInfo) {
		if (activityLogIndexName != null) {
			try {
				client.prepareIndex(activityLogIndexName, activityLogTypeName)
						.setSource(indexingInfo.buildDocument(jsonBuilder(), true, true)).execute().actionGet();
			} catch (Exception e) {
				logger.error("Error during index update result writing to the audit log {}", e.getMessage());
			}
		}
	}

	@Override
	public void storeDatetimeValue(String spaceKey, String propertyName, Date datetime, BulkRequestBuilder esBulk)
			throws IOException {
		String documentName = prepareValueStoreDocumentName(spaceKey, propertyName);
		if (logger.isDebugEnabled())
			logger.debug(
					"Going to write {} property with datetime value {} for space {} using {} update. Document name is {}.",
					propertyName, datetime, spaceKey, (esBulk != null ? "bulk" : "direct"), documentName);
		if (esBulk != null) {
			esBulk.add(indexRequest(getRiverIndexName()).type(riverName.name()).id(documentName)
					.source(storeDatetimeValueBuildDocument(spaceKey, propertyName, datetime)));
		} else {
			client.prepareIndex(getRiverIndexName(), riverName.name(), documentName)
					.setSource(storeDatetimeValueBuildDocument(spaceKey, propertyName, datetime)).execute().actionGet();
		}
	}

	/**
	 * Constant for field in JSON document used to store values.
	 * 
	 * @see #storeDatetimeValue(String, String, Date, BulkRequestBuilder)
	 * @see #readDatetimeValue(String, String)
	 * @see #storeDatetimeValueBuildDocument(String, String, Date)
	 * 
	 */
	protected static final String STORE_FIELD_VALUE = "value";

	/**
	 * Prepare JSON document to be stored inside {@link #storeDatetimeValue(String, String, Date, BulkRequestBuilder)}.
	 * 
	 * @param spaceKey key of Space value is for
	 * @param propertyName name of property
	 * @param datetime value to store
	 * @return JSON document
	 * @throws IOException
	 * @see #storeDatetimeValue(String, String, Date, BulkRequestBuilder)
	 * @see #readDatetimeValue(String, String)
	 */
	protected XContentBuilder storeDatetimeValueBuildDocument(String spaceKey, String propertyName, Date datetime)
			throws IOException {
		XContentBuilder builder = jsonBuilder().startObject();
		if (spaceKey != null)
			builder.field("spaceKey", spaceKey);
		builder.field("propertyName", propertyName).field(STORE_FIELD_VALUE, DateTimeUtils.formatISODateTime(datetime));
		builder.endObject();
		return builder;
	}

	@Override
	public Date readDatetimeValue(String spaceKey, String propertyName) throws IOException {
		Date lastDate = null;
		String documentName = prepareValueStoreDocumentName(spaceKey, propertyName);

		if (logger.isDebugEnabled())
			logger.debug("Going to read datetime value from {} property for space {}. Document name is {}.", propertyName,
					spaceKey, documentName);

		refreshSearchIndex(getRiverIndexName());
		GetResponse lastSeqGetResponse = client.prepareGet(getRiverIndexName(), riverName.name(), documentName).execute()
				.actionGet();
		if (lastSeqGetResponse.exists()) {
			Object timestamp = lastSeqGetResponse.sourceAsMap().get(STORE_FIELD_VALUE);
			if (timestamp != null) {
				lastDate = DateTimeUtils.parseISODateTime(timestamp.toString());
			}
		} else {
			if (logger.isDebugEnabled())
				logger.debug("{} document doesn't exist in remore river persistent store", documentName);
		}
		return lastDate;
	}

	@Override
	public boolean deleteDatetimeValue(String spaceKey, String propertyName) {
		String documentName = prepareValueStoreDocumentName(spaceKey, propertyName);

		if (logger.isDebugEnabled())
			logger.debug("Going to delete datetime value from {} property for space {}. Document name is {}.", propertyName,
					spaceKey, documentName);

		refreshSearchIndex(getRiverIndexName());

		DeleteResponse lastSeqGetResponse = client.prepareDelete(getRiverIndexName(), riverName.name(), documentName)
				.execute().actionGet();
		if (lastSeqGetResponse.notFound()) {
			if (logger.isDebugEnabled()) {
				logger.debug("{} document doesn't exist in remote river persistent store", documentName);
			}
			return false;
		} else {
			return true;
		}

	}

	/**
	 * @return
	 */
	protected String getRiverIndexName() {
		return "_river";
		// return RiverIndexName.Conf.indexName(settings.globalSettings());
	}

	/**
	 * Prepare name of document where Space related persistent value is stored
	 * 
	 * @param spaceKey key of Space stored value is for
	 * @param propertyName name of value
	 * @return document name
	 * 
	 * @see #storeDatetimeValue(String, String, Date, BulkRequestBuilder)
	 * @see #readDatetimeValue(String, String)
	 */
	protected static String prepareValueStoreDocumentName(String spaceKey, String propertyName) {
		if (spaceKey != null)
			return "_" + propertyName + "_" + spaceKey;
		else
			return "_" + propertyName;
	}

	@Override
	public BulkRequestBuilder prepareESBulkRequestBuilder() {
		return client.prepareBulk();
	}

	@Override
	public void executeESBulkRequest(BulkRequestBuilder esBulk) throws Exception {
		BulkResponse response = esBulk.execute().actionGet();
		if (response.hasFailures()) {
			throw new ElasticSearchException("Failed to execute ES index bulk update: " + response.buildFailureMessage());
		}
	}

	@Override
	public Thread acquireIndexingThread(String threadName, Runnable runnable) {
		return EsExecutors.daemonThreadFactory(settings.globalSettings(), threadName).newThread(runnable);
	}

	@Override
	public void refreshSearchIndex(String indexName) {
		client.admin().indices().prepareRefresh(indexName).execute().actionGet();
	}

	private static final long ES_SCROLL_KEEPALIVE = 60000;

	@Override
	public SearchRequestBuilder prepareESScrollSearchRequestBuilder(String indexName) {
		return client.prepareSearch(indexName).setScroll(new TimeValue(ES_SCROLL_KEEPALIVE)).setSearchType(SearchType.SCAN)
				.setSize(100);
	}

	public SearchResponse executeESSearchRequest(SearchRequestBuilder searchRequestBuilder) {
		return searchRequestBuilder.execute().actionGet();
	}

	@Override
	public SearchResponse executeESScrollSearchNextRequest(SearchResponse scrollResp) {
		return client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(ES_SCROLL_KEEPALIVE)).execute()
				.actionGet();
	}

	@Override
	public String loadPassword(String username) {
		logger.info("loading password for username {}", username);
		String ret = null;
		String riverIndexName = getRiverIndexName();
		refreshSearchIndex(riverIndexName);
		GetResponse resp = client.prepareGet(riverIndexName, riverName().name(), "_pwd").execute().actionGet();
		if (resp.exists()) {
			if (logger.isDebugEnabled()) {
				logger.debug("Password document: {}", resp.getSourceAsString());
			}
			Map<String, Object> newset = resp.getSource();
			ret = XContentMapValues.nodeStringValue(newset.get("pwd"), null);
		}
		return ret;
	}

}

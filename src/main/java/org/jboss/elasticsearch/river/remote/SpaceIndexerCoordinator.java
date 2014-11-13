/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SettingsException;

/**
 * Space indexing coordinator components. Coordinate parallel indexing of more Spaces, and also handles how often one
 * space updates should be checked.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceIndexerCoordinator implements ISpaceIndexerCoordinator {

	private static final ESLogger logger = Loggers.getLogger(SpaceIndexerCoordinator.class);

	/**
	 * Property value where "last index update start date" is stored for Space
	 * 
	 * @see IESIntegration#storeDatetimeValue(String, String, Date, BulkRequestBuilder)
	 * @see IESIntegration#readDatetimeValue(String, String)
	 * @see #spaceIndexUpdateNecessary(String)
	 */
	protected static final String STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE = "lastIndexUpdateStartDate";

	/**
	 * Property value where "last index full update date" is stored for Space
	 * 
	 * @see IESIntegration#storeDatetimeValue(String, String, Date, BulkRequestBuilder)
	 * @see IESIntegration#readDatetimeValue(String, String)
	 * @see #spaceIndexFullUpdateNecessary(String)
	 */
	protected static final String STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE = "lastIndexFullUpdateDate";

	/**
	 * Property value where "full index force date" is stored for Space
	 * 
	 * @see IESIntegration#storeDatetimeValue(String, String, Date, BulkRequestBuilder)
	 * @see IESIntegration#readDatetimeValue(String, String)
	 * @see #spaceIndexFullUpdateNecessary(String)
	 */
	protected static final String STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE = "forceIndexFullUpdateDate";

	protected static final int COORDINATOR_THREAD_WAITS_QUICK = 2 * 1000;
	protected static final int COORDINATOR_THREAD_WAITS_SLOW = 15 * 1000;
	protected int coordinatorThreadWaits = COORDINATOR_THREAD_WAITS_QUICK;

	protected IESIntegration esIntegrationComponent;

	/**
	 * Remote system client to access data
	 */
	protected IRemoteSystemClient remoteSystemClient;

	protected IDocumentIndexStructureBuilder documentIndexStructureBuilder;

	protected int maxIndexingThreads;

	/**
	 * Period of index update from remote system [ms].
	 */
	protected long indexUpdatePeriod;

	/**
	 * Period of index automatic full update from remote system [ms]. value <= 0 means never.
	 */
	protected long indexFullUpdatePeriod = -1;

	/**
	 * Cron expression to schedule automatic full update from remote system. Ignore <code>indexFullUpdatePeriod</code> if
	 * this one is not null.
	 */
	protected CronExpression indexFullUpdateCronExpression;

	/**
	 * <code>true</code> to run simple indexing mode - "List Documents" is called only once in this run
	 */
	protected SpaceIndexingMode spaceIndexingMode;

	/**
	 * Queue of Space keys which needs to be reindexed in near future.
	 * 
	 * @see SpaceIndexerCoordinator
	 */
	protected Queue<String> spaceKeysToIndexQueue = new LinkedBlockingQueue<String>();

	/**
	 * Map where currently running Space indexer threads are stored.
	 */
	protected final Map<String, Thread> spaceIndexerThreads = new HashMap<String, Thread>();

	/**
	 * Map where currently running Space indexers are stored.
	 */
	protected final Map<String, SpaceIndexerBase> spaceIndexers = new HashMap<String, SpaceIndexerBase>();

	/**
	 * Constructor with parameters.
	 * 
	 * @param remoteSystemClient configured remote system access client to be passed into
	 *          {@link SpaceByLastUpdateTimestampIndexer} instances started from coordinator
	 * @param esIntegrationComponent to be used to call River component and ElasticSearch functions
	 * @param documentIndexStructureBuilder component used to build structures for search index
	 * @param indexUpdatePeriod index update period [ms]
	 * @param maxIndexingThreads maximal number of parallel JIRA indexing threads started by this coordinator
	 * @param indexFullUpdatePeriod period of index automatic full update from remote system [ms]. value <= 0 means never.
	 * @param indexFullUpdateCronExpression cron expression for full updates.
	 * @param spaceIndexingMode mode of space indexing
	 */
	public SpaceIndexerCoordinator(IRemoteSystemClient remoteSystemClient, IESIntegration esIntegrationComponent,
			IDocumentIndexStructureBuilder documentIndexStructureBuilder, long indexUpdatePeriod, int maxIndexingThreads,
			long indexFullUpdatePeriod, CronExpression indexFullUpdateCronExpression, SpaceIndexingMode spaceIndexingMode) {
		super();
		this.remoteSystemClient = remoteSystemClient;
		this.esIntegrationComponent = esIntegrationComponent;
		this.indexUpdatePeriod = indexUpdatePeriod;
		this.maxIndexingThreads = maxIndexingThreads;
		this.documentIndexStructureBuilder = documentIndexStructureBuilder;
		this.indexFullUpdatePeriod = indexFullUpdatePeriod;
		this.spaceIndexingMode = spaceIndexingMode;
		this.indexFullUpdateCronExpression = indexFullUpdateCronExpression;
	}

	@Override
	public void run() {
		logger.info("Remote river spaces indexing coordinator task started");
		try {
			while (true) {
				if (esIntegrationComponent.isClosed()) {
					return;
				}
				try {
					processLoopTask();
				} catch (InterruptedException e1) {
					return;
				} catch (Exception e) {
					if (esIntegrationComponent.isClosed())
						return;
					logger.error("Failed to process Remote update coordination task {}", e, e.getMessage());
				}
				try {
					if (esIntegrationComponent.isClosed())
						return;
					logger.debug("Remote river coordinator task is going to sleep for {} ms", coordinatorThreadWaits);
					Thread.sleep(coordinatorThreadWaits);
				} catch (InterruptedException e1) {
					return;
				}
			}
		} finally {
			synchronized (spaceIndexerThreads) {
				for (Thread pi : spaceIndexerThreads.values()) {
					pi.interrupt();
				}
				spaceIndexerThreads.clear();
				spaceIndexers.clear();
			}
			logger.info("Remote river spaces indexing coordinator task stopped");
		}
	}

	protected long lastQueueFillTime = 0;

	/**
	 * Process coordination tasks in one loop of coordinator.
	 * 
	 * @throws Exception
	 * @throws InterruptedException id interrupted
	 */
	protected void processLoopTask() throws Exception, InterruptedException {
		long now = System.currentTimeMillis();
		if (spaceKeysToIndexQueue.isEmpty() || (lastQueueFillTime < (now - COORDINATOR_THREAD_WAITS_SLOW))) {
			lastQueueFillTime = now;
			fillSpaceKeysToIndexQueue();
		}
		if (spaceKeysToIndexQueue.isEmpty()) {
			// no spaces to process now, we can slow down looping
			coordinatorThreadWaits = COORDINATOR_THREAD_WAITS_SLOW;
		} else {
			// some spaces to process now, we need to loop quickly to process it
			coordinatorThreadWaits = COORDINATOR_THREAD_WAITS_QUICK;
			startIndexers();
		}
	}

	/**
	 * Fill {@link #spaceKeysToIndexQueue} by spaces which needs to be indexed now.
	 * 
	 * @throws Exception in case of problem
	 * @throws InterruptedException if indexing interruption is requested by ES server
	 */
	protected void fillSpaceKeysToIndexQueue() throws Exception, InterruptedException {
		List<String> ap = esIntegrationComponent.getAllIndexedSpaceKeys();
		if (ap != null && !ap.isEmpty()) {
			for (String spaceKey : ap) {
				if (esIntegrationComponent.isClosed())
					throw new InterruptedException();
				// do not schedule space for indexing if indexing runs already for it
				synchronized (spaceIndexerThreads) {
					if (spaceIndexerThreads.containsKey(spaceKey)) {
						continue;
					}
				}
				if (!spaceKeysToIndexQueue.contains(spaceKey) && spaceIndexUpdateNecessary(spaceKey)) {
					spaceKeysToIndexQueue.add(spaceKey);
				}
			}
		}
	}

	/**
	 * Start indexers for spaces in {@link #spaceKeysToIndexQueue} but not more than {@link #maxIndexingThreads}.
	 * 
	 * @throws InterruptedException if indexing process is interrupted
	 * @throws Exception
	 */
	protected void startIndexers() throws InterruptedException, Exception {
		String firstSkippedFullIndex = null;
		while (spaceIndexerThreads.size() < maxIndexingThreads && !spaceKeysToIndexQueue.isEmpty()) {
			if (esIntegrationComponent.isClosed())
				throw new InterruptedException();
			String spaceKey = spaceKeysToIndexQueue.poll();

			boolean fullUpdateNecessary = spaceIndexFullUpdateNecessary(spaceKey);

			// reserve last free thread for incremental updates!!!
			if (fullUpdateNecessary && maxIndexingThreads > 1 && spaceIndexerThreads.size() == (maxIndexingThreads - 1)) {
				spaceKeysToIndexQueue.add(spaceKey);
				// try to find some space for incremental update, if not any found then end
				if (firstSkippedFullIndex == null) {
					firstSkippedFullIndex = spaceKey;
				} else {
					if (firstSkippedFullIndex == spaceKey)
						return;
				}
				continue;
			}

			SpaceIndexerBase indexer = prepareSpaceIndexer(spaceKey, fullUpdateNecessary);
			Thread it = esIntegrationComponent.acquireIndexingThread("remote_river_indexer_" + spaceKey, indexer);
			esIntegrationComponent.storeDatetimeValue(spaceKey, STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE, new Date(),
					null);
			synchronized (spaceIndexerThreads) {
				spaceIndexerThreads.put(spaceKey, it);
				spaceIndexers.put(spaceKey, indexer);
			}
			it.start();
		}
	}

	/**
	 * Select correct space indexer implementation based on {@link #spaceIndexingMode}.
	 * 
	 * @param spaceKey to create indexer for
	 * @param fullUpdateNecessary flag for indexer
	 * @return indexer
	 */
	protected SpaceIndexerBase prepareSpaceIndexer(String spaceKey, boolean fullUpdateNecessary) {
		if (spaceIndexingMode == null)
			throw new SettingsException("undefined space indexing mode");
		switch (spaceIndexingMode) {
		case SIMPLE:
			return new SpaceSimpleIndexer(spaceKey, remoteSystemClient, esIntegrationComponent, documentIndexStructureBuilder);
		case PAGINATION:
			return new SpacePaginatingIndexer(spaceKey, remoteSystemClient, esIntegrationComponent,
					documentIndexStructureBuilder);
		case UPDATE_TIMESTAMP:
			return new SpaceByLastUpdateTimestampIndexer(spaceKey, fullUpdateNecessary, remoteSystemClient,
					esIntegrationComponent, documentIndexStructureBuilder);
		default:
			throw new SettingsException("unsupported space indexing mode");
		}
	}

	/**
	 * Check if search index update for given Space have to be performed now.
	 * 
	 * @param spaceKey to check for
	 * @return true to perform index update now
	 * @throws IOException
	 */
	protected boolean spaceIndexUpdateNecessary(String spaceKey) throws Exception {
		if (esIntegrationComponent.readDatetimeValue(spaceKey, STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE) != null)
			return true;

		Date lastIndexing = esIntegrationComponent.readDatetimeValue(spaceKey,
				STORE_PROPERTYNAME_LAST_INDEX_UPDATE_START_DATE);
		if (logger.isDebugEnabled())
			logger.debug("Space {} last indexing start date is {}. We perform next indexing after {}ms.", spaceKey,
					lastIndexing, indexUpdatePeriod);
		if (lastIndexing == null || lastIndexing.getTime() < ((System.currentTimeMillis() - indexUpdatePeriod))) {
			return true;
		}
		if (indexFullUpdateCronExpression != null || indexFullUpdatePeriod > 0) {
			// evaluate full update necessary condition here to start it if necessary (added during #49 implementation)
			return spaceIndexFullUpdateNecessary(spaceKey);
		}
		return false;
	}

	/**
	 * Check if search index full update for given Space have to be performed now.
	 * 
	 * @param spaceKey to check for
	 * @return true to perform index full update now
	 * @throws IOException
	 */
	protected boolean spaceIndexFullUpdateNecessary(String spaceKey) throws Exception {
		if (esIntegrationComponent.readDatetimeValue(spaceKey, STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE) != null)
			return true;

		if (indexFullUpdateCronExpression != null) {
			Date lastFullIndexing = esIntegrationComponent.readDatetimeValue(spaceKey,
					STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE);
			if (lastFullIndexing == null) {
				lastFullIndexing = new Date(0);
			}
			Date nextFullIndexing = indexFullUpdateCronExpression.getNextValidTimeAfter(lastFullIndexing);
			return (nextFullIndexing != null && (nextFullIndexing.getTime() < System.currentTimeMillis()));
		} else {
			if (indexFullUpdatePeriod < 1) {
				return false;
			}
			Date lastFullIndexing = esIntegrationComponent.readDatetimeValue(spaceKey,
					STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE);
			if (logger.isDebugEnabled())
				logger.debug("Space {} last full update date is {}. We perform next full indexing after {}ms.", spaceKey,
						lastFullIndexing, indexFullUpdatePeriod);
			return lastFullIndexing == null
					|| lastFullIndexing.getTime() < ((System.currentTimeMillis() - indexFullUpdatePeriod));
		}
	}

	@Override
	public void forceFullReindex(String spaceKey) throws Exception {
		esIntegrationComponent.storeDatetimeValue(spaceKey, STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE, new Date(),
				null);
	}

	@Override
	public void reportIndexingFinished(String spaceKey, boolean finishedOK, boolean fullUpdate) {
		synchronized (spaceIndexerThreads) {
			spaceIndexerThreads.remove(spaceKey);
			spaceIndexers.remove(spaceKey);
		}
		if (fullUpdate) {
			if (finishedOK) {
				try {
					esIntegrationComponent.deleteDatetimeValue(spaceKey, STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE);
				} catch (Exception e) {
					logger
							.error("Can't delete {} value due: {}", STORE_PROPERTYNAME_FORCE_INDEX_FULL_UPDATE_DATE, e.getMessage());
				}
				try {
					esIntegrationComponent.storeDatetimeValue(spaceKey, STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE,
							new Date(), null);
				} catch (Exception e) {
					logger.error("Can't store {} value due: {}", STORE_PROPERTYNAME_LAST_INDEX_FULL_UPDATE_DATE, e.getMessage());
				}
			} else {
				// bugfix for #3
				if (indexFullUpdatePeriod < 1) {
					logger.info("Full update failed for space {} so we are going to force it again next time ", spaceKey);
					try {
						forceFullReindex(spaceKey);
					} catch (Exception e) {
						logger.error("Can't force full update due: {}", e.getMessage());
					}
				}
			}
		}
	}

	/**
	 * Configuration - Set period of index automatic full update from remote system [ms]. value <= 0 means never.
	 * 
	 * @param indexFullUpdatePeriod to set
	 */
	public void setIndexFullUpdatePeriod(int indexFullUpdatePeriod) {
		this.indexFullUpdatePeriod = indexFullUpdatePeriod;
	}

	@Override
	public List<SpaceIndexingInfo> getCurrentSpaceIndexingInfo() {
		List<SpaceIndexingInfo> ret = new ArrayList<SpaceIndexingInfo>();
		synchronized (spaceIndexerThreads) {
			for (SpaceIndexerBase indexer : spaceIndexers.values()) {
				ret.add(indexer.getIndexingInfo());
			}
		}
		return ret;
	}

}

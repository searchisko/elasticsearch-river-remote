/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.logging.Loggers;

/**
 * Class used to run one index update process for one Space. Incremental indexing process is based on date of last
 * document update.
 * <p>
 * Uses search of data from remote system over timestamp of last update. Documents returned from remote system client
 * MUST BE ascending ordered by timestamp of last update also!
 * <p>
 * Can be used only for one run, then must be discarded and new instance created!
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceByLastUpdateTimestampIndexer extends SpaceIndexerBase {

	/**
	 * Property value where "last indexed document update date" is stored
	 * 
	 * @see IESIntegration#storeDatetimeValue(String, String, Date, BulkRequestBuilder)
	 * @see IESIntegration#readDatetimeValue(String, String)
	 */
	protected static final String STORE_PROPERTYNAME_LAST_INDEXED_DOC_UPDATE_DATE = "lastIndexedDocumentUpdateDate";

	/**
	 * Create and configure indexer.
	 * 
	 * @param spaceKey to be indexed by this indexer.
	 * @param fullUpdate true to request full index update
	 * @param simpleGetDocuments true to run simple indexing mode - "List Documents" is called only once in this run
	 * @param remoteSystemClient configured client to be used to obtain informations from remote system.
	 * @param esIntegrationComponent to be used to call River component and ElasticSearch functions
	 * @param documentIndexStructureBuilder to be used during indexing
	 */
	public SpaceByLastUpdateTimestampIndexer(String spaceKey, boolean fullUpdate, IRemoteSystemClient remoteSystemClient,
			IESIntegration esIntegrationComponent, IDocumentIndexStructureBuilder documentIndexStructureBuilder) {
		super(spaceKey, remoteSystemClient, esIntegrationComponent, documentIndexStructureBuilder);
		logger = Loggers.getLogger(SpaceByLastUpdateTimestampIndexer.class);
		indexingInfo = new SpaceIndexingInfo(spaceKey, fullUpdate);
	}

	@Override
	protected void processUpdate() throws Exception {
		indexingInfo.documentsUpdated = 0;
		Date updatedAfter = null;
		if (!indexingInfo.fullUpdate) {
			updatedAfter = readLastDocumentUpdatedDate(spaceKey);
		}
		Date updatedAfterStarting = updatedAfter;
		if (updatedAfter == null)
			indexingInfo.fullUpdate = true;
		Date lastDocumentUpdatedDate = null;

		int startAt = 0;

		logger.info("Go to perform {} update for Space {}", indexingInfo.fullUpdate ? "full" : "incremental", spaceKey);

		boolean cont = true;
		while (cont) {
			if (isClosed())
				throw new InterruptedException("Interrupted because River is closed");

			if (logger.isDebugEnabled())
				logger.debug("Go to ask remote system for updated documents for space {} with startAt {} and updated {}",
						spaceKey, startAt, (updatedAfter != null ? ("after " + updatedAfter) : "in whole history"));

			ChangedDocumentsResults res = remoteSystemClient.getChangedDocuments(spaceKey, startAt, updatedAfter);

			if (res.getDocumentsCount() == 0) {
				cont = false;
			} else {
				if (isClosed())
					throw new InterruptedException("Interrupted because River is closed");

				Date firstDocumentUpdatedDate = null;
				int updatedInThisBulk = 0;
				BulkRequestBuilder esBulk = esIntegrationComponent.prepareESBulkRequestBuilder();
				for (Map<String, Object> document : res.getDocuments()) {
					String documentId = getDocumentIdChecked(document);
					if (getDocumentDetail(documentId, document)) {
						lastDocumentUpdatedDate = documentIndexStructureBuilder.extractDocumentUpdated(document);
						logger.debug("Go to update index for document '{}' with updated {}", documentId, lastDocumentUpdatedDate);
						if (lastDocumentUpdatedDate == null) {
							throw new IllegalArgumentException("Last update timestamp not found in data for document " + documentId);
						}
						if (firstDocumentUpdatedDate == null) {
							firstDocumentUpdatedDate = lastDocumentUpdatedDate;
						}

						documentIndexStructureBuilder.indexDocument(esBulk, spaceKey, document);
						updatedInThisBulk++;

					}
					if (isClosed())
						throw new InterruptedException("Interrupted because River is closed");
				}

				if (lastDocumentUpdatedDate != null)
					storeLastDocumentUpdatedDate(esBulk, spaceKey, lastDocumentUpdatedDate);

				if (updatedInThisBulk > 0) {
					executeBulkUpdate(esBulk);
					indexingInfo.documentsUpdated += updatedInThisBulk;
				}

				// next logic depends on documents sorted by update timestamp ascending when returned from remote system
				if (lastDocumentUpdatedDate != null && firstDocumentUpdatedDate != null
						&& !lastDocumentUpdatedDate.equals(firstDocumentUpdatedDate)) {
					// processed documents updated in different times, so we can continue by document filtering based on latest
					// time
					// of update which is more safe for concurrent changes in the remote system
					updatedAfter = lastDocumentUpdatedDate;
					if (res.getTotal() != null)
						cont = res.getTotal() > (res.getStartAt() + res.getDocumentsCount());
					startAt = 0;
				} else {
					// no any documents found in batch
					// OR
					// more documents updated in same time, we must go over them using pagination only, which may sometimes lead
					// to some document update lost due concurrent changes in the remote system. But we can do it only if Total
					// is available from response!
					if (res.getTotal() != null) {
						startAt = res.getStartAt() + res.getDocumentsCount();
						cont = res.getTotal() > startAt;
					} else {
						long t = 0;
						if (lastDocumentUpdatedDate != null) {
							t = lastDocumentUpdatedDate.getTime();
						} else if (firstDocumentUpdatedDate != null) {
							t = firstDocumentUpdatedDate.getTime();
						}

						if (t > 0) {
							updatedAfter = new Date(t + 1000);
							logger
									.warn(
											"All documents loaded from remote system for space '{}' contain same update timestamp {}, but we have no total count from response, so we may miss some documents because we shift timestamp for new request by one second to {}!",
											spaceKey, lastDocumentUpdatedDate, updatedAfter);
							startAt = 0;
						} else {
							logger
									.warn(
											"All documents loaded from remote system for space '{}' are unreachable and we have no total count of records, so we have to finish indexing for now.",
											spaceKey);
							cont = false;
						}
					}

				}
			}
		}

		if (indexingInfo.documentsUpdated > 0 && lastDocumentUpdatedDate != null && updatedAfterStarting != null
				&& updatedAfterStarting.equals(lastDocumentUpdatedDate)) {
			// no any new document during this update cycle, go to increment lastDocumentUpdatedDate in store by one second
			// not to index last document again and again in next cycle
			storeLastDocumentUpdatedDate(null, spaceKey, new Date(lastDocumentUpdatedDate.getTime() + 1000));
		}
	}

	/**
	 * Get date of last document updated for given Space from persistent store inside ES cluster, so we can continue in
	 * update process from this point.
	 * 
	 * @param spaceKey to get date for.
	 * @return date of last document updated or null if not available (in this case indexing starts from the beginning of
	 *         Space history)
	 * @throws IOException
	 * @see #storeLastDocumentUpdatedDate(BulkRequestBuilder, String, Date)
	 */
	protected Date readLastDocumentUpdatedDate(String spaceKey) throws Exception {
		return esIntegrationComponent.readDatetimeValue(spaceKey, STORE_PROPERTYNAME_LAST_INDEXED_DOC_UPDATE_DATE);
	}

	/**
	 * Store date of last document updated for given Space into persistent store inside ES cluster, so we can continue in
	 * update process from this point next time.
	 * 
	 * @param esBulk ElasticSearch bulk request to be used for update
	 * @param spaceKey store date for.
	 * @param lastDocumentUpdatedDate date to store
	 * @throws Exception
	 * @see #readLastDocumentUpdatedDate(String)
	 */
	protected void storeLastDocumentUpdatedDate(BulkRequestBuilder esBulk, String spaceKey, Date lastDocumentUpdatedDate)
			throws Exception {
		esIntegrationComponent.storeDatetimeValue(spaceKey, STORE_PROPERTYNAME_LAST_INDEXED_DOC_UPDATE_DATE,
				lastDocumentUpdatedDate, esBulk);
	}

}

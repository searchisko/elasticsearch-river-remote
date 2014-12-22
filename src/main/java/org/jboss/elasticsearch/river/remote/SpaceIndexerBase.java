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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.search.SearchHit;
import org.jboss.elasticsearch.river.remote.exception.RemoteDocumentNotFoundException;

/**
 * Base abstract class for indexers used to run one index update process for one Space.
 * <p>
 * Can be used only for one run, then must be discarded and new instance created!
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public abstract class SpaceIndexerBase implements Runnable {

	public static final String KEY_DETAIL = "detail";

	protected ESLogger logger;

	protected final IRemoteSystemClient remoteSystemClient;

	protected final IESIntegration esIntegrationComponent;

	/**
	 * Configured document index structure builder to be used.
	 */
	protected final IDocumentIndexStructureBuilder documentIndexStructureBuilder;

	/**
	 * Key of Space updated by this indexer.
	 */
	protected final String spaceKey;

	/**
	 * Time when indexing started.
	 */
	protected long startTime = 0;

	/**
	 * Info about current indexing.
	 */
	protected SpaceIndexingInfo indexingInfo;

	/**
	 * Create and configure indexer.
	 * 
	 * @param spaceKey to be indexed by this indexer.
	 * @param remoteSystemClient configured client to be used to obtain informations from remote system.
	 * @param esIntegrationComponent to be used to call River component and ElasticSearch functions
	 * @param documentIndexStructureBuilder to be used during indexing
	 */
	public SpaceIndexerBase(String spaceKey, IRemoteSystemClient remoteSystemClient,
			IESIntegration esIntegrationComponent, IDocumentIndexStructureBuilder documentIndexStructureBuilder) {
		if (Utils.isEmpty(spaceKey))
			throw new IllegalArgumentException("spaceKey must be defined");
		this.remoteSystemClient = remoteSystemClient;
		this.spaceKey = spaceKey;
		this.esIntegrationComponent = esIntegrationComponent;
		this.documentIndexStructureBuilder = documentIndexStructureBuilder;
	}

	@Override
	public void run() {
		startTime = System.currentTimeMillis();
		indexingInfo.startDate = new Date(startTime);
		try {
			processUpdate();
			processDelete(new Date(startTime));
			indexingInfo.timeElapsed = (System.currentTimeMillis() - startTime);
			indexingInfo.finishedOK = true;
			esIntegrationComponent.reportIndexingFinished(indexingInfo);
			logger.info("Finished {} update for Space {}. {} updated and {} deleted documents. Time elapsed {}s.",
					indexingInfo.fullUpdate ? "full" : "incremental", spaceKey, indexingInfo.documentsUpdated,
					indexingInfo.documentsDeleted, (indexingInfo.timeElapsed / 1000));
			if (indexingInfo.getErrorMessage() != null) {
				logger
						.info(
								"Update for Space {} contained {} documents with skipped unfatal errors: "
										+ indexingInfo.getErrorMessage(), spaceKey, indexingInfo.documentsWithError);
			}
		} catch (Throwable e) {
			indexingInfo.timeElapsed = (System.currentTimeMillis() - startTime);
			indexingInfo.addErrorMessage(e.getMessage());
			indexingInfo.finishedOK = false;
			esIntegrationComponent.reportIndexingFinished(indexingInfo);
			Throwable cause = e;
			// do not log stacktrace for some operational exceptions to keep log file much clear
			if (((cause instanceof IOException) || (cause instanceof InterruptedException)) && cause.getMessage() != null)
				cause = null;
			logger.error("Failed {} update for Space {} due: {}", cause, indexingInfo.fullUpdate ? "full" : "incremental",
					spaceKey, e.getMessage());
		}
	}

	/**
	 * Process update of search index for configured Space. A {@link #indexingInfo.updatedCount} field is updated inside of this
	 * method. A {@link #indexingInfo.fullUpdate} field can be updated inside of this method also.
	 * 
	 * @throws Exception
	 */
	protected abstract void processUpdate() throws Exception;

	/**
	 * Get document detail from remote system if configured, place it under <code>detail</code> key in data.
	 * {@link IRemoteSystemClient#getChangedDocumentDetails(String, String, Map)} is used inside.
	 * 
	 * @param documentId of document to get
	 * @param document structure to get details for and place them into
	 * @return true if document is found correctly, false if not found in remote system
	 * @throws Exception in case of runtime problem
	 */
	protected boolean getDocumentDetail(String documentId, Map<String, Object> document) throws Exception {
		try {
			Object detail = remoteSystemClient.getChangedDocumentDetails(spaceKey, documentId, document);
			if (detail != null) {
				document.put(KEY_DETAIL, detail);
			}
			return true;
		} catch (RemoteDocumentNotFoundException e) {
			// skip rest of processing in this case
			String msg = "Detail processing problem for document with id ' documentId', so we skip it: " + e.getMessage();
			indexingInfo.addErrorMessage(msg);
			indexingInfo.documentsWithError++;
			logger.warn(msg);
			return false;
		}
	}

	/**
	 * Get document id from document. Throw exception if not there.
	 * 
	 * @param document to get id from
	 * @return document id
	 * @throws IllegalArgumentException if document id is not found in document
	 */
	protected String getDocumentIdChecked(Map<String, Object> document) {
		String documentId = documentIndexStructureBuilder.extractDocumentId(document);
		if (Utils.isEmpty(documentId)) {
			throw new IllegalArgumentException("Document ID not found in remote system response for Space " + spaceKey
					+ " within data: " + document);
		}
		return documentId;
	}

	protected void executeBulkUpdate(BulkRequestBuilder esBulk) {
		try {
			esIntegrationComponent.executeESBulkRequest(esBulk);
		} catch (BulkUpdatePartialFailureException e) {
			indexingInfo.addErrorMessage(e.getMessage());
			indexingInfo.documentsWithError += e.getNumOfFailures();
			indexingInfo.documentsUpdated -= e.getNumOfFailures();
		}
	}

	/**
	 * Process delete of documents from search index for configured Space. A {@link #deleteCount} field is updated inside
	 * of this method.
	 * 
	 * @param boundDate date when full update was started. We delete all search index documents not updated after this
	 *          date (which means these documents are not in remote system anymore).
	 */
	protected void processDelete(Date boundDate) throws Exception {

		if (boundDate == null)
			throw new IllegalArgumentException("boundDate must be set");

		if (!indexingInfo.fullUpdate)
			return;

		logger.debug("Go to process remote system deletes for Space {} for documents not updated in index after {}",
				spaceKey, boundDate);

		String indexName = documentIndexStructureBuilder.getDocumentSearchIndexName(spaceKey);
		esIntegrationComponent.refreshSearchIndex(indexName);

		logger.debug("go to delete indexed documents for space {} not updated after {}", spaceKey, boundDate);
		SearchRequestBuilder srb = esIntegrationComponent.prepareESScrollSearchRequestBuilder(indexName);
		documentIndexStructureBuilder.buildSearchForIndexedDocumentsNotUpdatedAfter(srb, spaceKey, boundDate);

		SearchResponse scrollResp = esIntegrationComponent.executeESSearchRequest(srb);

		if (scrollResp.getHits().getTotalHits() > 0) {
			if (isClosed())
				throw new InterruptedException("Interrupted because River is closed");
			scrollResp = esIntegrationComponent.executeESScrollSearchNextRequest(scrollResp);
			BulkRequestBuilder esBulk = esIntegrationComponent.prepareESBulkRequestBuilder();
			while (scrollResp.getHits().getHits().length > 0) {
				for (SearchHit hit : scrollResp.getHits()) {
					logger.debug("Go to delete indexed document for ES document id {}", hit.getId());
					if (documentIndexStructureBuilder.deleteESDocument(esBulk, hit)) {
						indexingInfo.documentsDeleted++;
					} else {
						indexingInfo.commentsDeleted++;
					}
				}
				if (isClosed())
					throw new InterruptedException("Interrupted because River is closed");
				scrollResp = esIntegrationComponent.executeESScrollSearchNextRequest(scrollResp);
			}
			esIntegrationComponent.executeESBulkRequest(esBulk);
		}
	}

	/**
	 * Prepare delete of es index documents based on remote document id.
	 * 
	 * @param esBulk to prepare delete into
	 * @param documentId to prepare delete for
	 * @return true if at least one delete has been prepared in the method
	 * @throws InterruptedException
	 * @throws Exception
	 */
	protected boolean prepareDeleteByRemoteDocumentId(BulkRequestBuilder esBulk, String documentId)
			throws InterruptedException, Exception {
		boolean deletedInThisBulk = false;
		String indexName = documentIndexStructureBuilder.getDocumentSearchIndexName(spaceKey);
		esIntegrationComponent.refreshSearchIndex(indexName);

		logger.debug("go to delete indexed documents for space {} and remote id {}", spaceKey, documentId);
		SearchRequestBuilder srb = esIntegrationComponent.prepareESScrollSearchRequestBuilder(indexName);
		documentIndexStructureBuilder.buildSearchForIndexedDocumentsWithRemoteId(srb, spaceKey, documentId);

		SearchResponse scrollResp = esIntegrationComponent.executeESSearchRequest(srb);

		if (scrollResp.getHits().getTotalHits() > 0) {
			if (isClosed())
				throw new InterruptedException("Interrupted because River is closed");
			scrollResp = esIntegrationComponent.executeESScrollSearchNextRequest(scrollResp);
			while (scrollResp.getHits().getHits().length > 0) {
				for (SearchHit hit : scrollResp.getHits()) {
					logger.debug("Go to delete indexed document for ES document id {}", hit.getId());
					if (documentIndexStructureBuilder.deleteESDocument(esBulk, hit)) {
						indexingInfo.documentsDeleted++;
					} else {
						indexingInfo.commentsDeleted++;
					}
					deletedInThisBulk = true;
				}
				scrollResp = esIntegrationComponent.executeESScrollSearchNextRequest(scrollResp);
			}
		}
		return deletedInThisBulk;
	}

	/**
	 * Check if we must interrupt update process because ElasticSearch runtime needs it.
	 * 
	 * @return true if we must interrupt update process
	 */
	protected boolean isClosed() {
		return esIntegrationComponent != null && esIntegrationComponent.isClosed();
	}

	/**
	 * Get current indexing info.
	 * 
	 * @return indexing info instance.
	 */
	public SpaceIndexingInfo getIndexingInfo() {
		return indexingInfo;
	}

}

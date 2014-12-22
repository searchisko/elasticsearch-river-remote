/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;

/**
 * Class used to run one index update process for one Space. Full update indexing process with paginating support.
 * Incremental indexing not supported.
 * <p>
 * Can be used only for one run, then must be discarded and new instance created!
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpacePaginatingIndexer extends SpaceIndexerBase {

	/**
	 * Create and configure indexer.
	 * 
	 * @param spaceKey to be indexed by this indexer.
	 * @param remoteSystemClient configured client to be used to obtain informations from remote system.
	 * @param esIntegrationComponent to be used to call River component and ElasticSearch functions
	 * @param documentIndexStructureBuilder to be used during indexing
	 */
	public SpacePaginatingIndexer(String spaceKey, IRemoteSystemClient remoteSystemClient,
			IESIntegration esIntegrationComponent, IDocumentIndexStructureBuilder documentIndexStructureBuilder) {
		super(spaceKey, remoteSystemClient, esIntegrationComponent, documentIndexStructureBuilder);
		logger = esIntegrationComponent.createLogger(SpacePaginatingIndexer.class);
		indexingInfo = new SpaceIndexingInfo(spaceKey, true);
	}

	@Override
	protected void processUpdate() throws Exception {
		indexingInfo.documentsUpdated = 0;

		int startAt = 0;

		logger.info("Go to perform full update for Space {}", spaceKey);

		boolean cont = true;
		while (cont) {
			if (isClosed())
				throw new InterruptedException("Interrupted because River is closed");

			if (logger.isDebugEnabled())
				logger.debug("Go to ask remote system for updated documents for space {} with startAt {}", spaceKey, startAt);

			ChangedDocumentsResults res = remoteSystemClient.getChangedDocuments(spaceKey, startAt, null);

			if (res.getDocumentsCount() == 0) {
				cont = false;
			} else {
				if (isClosed())
					throw new InterruptedException("Interrupted because River is closed");

				int updatedInThisBulk = 0;
				boolean deletedInThisBulk = false;
				BulkRequestBuilder esBulk = esIntegrationComponent.prepareESBulkRequestBuilder();
				for (Map<String, Object> document : res.getDocuments()) {
					String documentId = getDocumentIdChecked(document);
					if (getDocumentDetail(documentId, document)) {
						logger.debug("Go to update index for document '{}'", documentId);
						if (documentIndexStructureBuilder.extractDocumentDeleted(document)) {
							deletedInThisBulk = prepareDeleteByRemoteDocumentId(esBulk, documentId) || deletedInThisBulk;
						} else {
							documentIndexStructureBuilder.indexDocument(esBulk, spaceKey, document);
							updatedInThisBulk++;
						}
					}
					if (isClosed())
						throw new InterruptedException("Interrupted because River is closed");
				}

				if (updatedInThisBulk > 0 || deletedInThisBulk) {
					executeBulkUpdate(esBulk);
					indexingInfo.documentsUpdated += updatedInThisBulk;
				}

				startAt = res.getStartAt() + res.getDocumentsCount();
				if (res.getTotal() != null) {
					cont = res.getTotal() > startAt;
				}
			}
		}
	}
}

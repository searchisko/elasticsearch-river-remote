/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.logging.Loggers;

/**
 * Class used to run one index update process for one Space. Full indexing is done always with one call to get list of
 * documents from emote system.
 * <p>
 * Can be used only for one run, then must be discarded and new instance created!
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceSimpleIndexer extends SpaceIndexerBase {

	private static final int MAX_BULK_SIZE_IN_SIMPLE_GET = 50;

	protected int bulkSize = MAX_BULK_SIZE_IN_SIMPLE_GET;

	/**
	 * Create and configure indexer.
	 * 
	 * @param spaceKey to be indexed by this indexer.
	 * @param remoteSystemClient configured client to be used to obtain informations from remote system.
	 * @param esIntegrationComponent to be used to call River component and ElasticSearch functions
	 * @param documentIndexStructureBuilder to be used during indexing
	 */
	public SpaceSimpleIndexer(String spaceKey, IRemoteSystemClient remoteSystemClient,
			IESIntegration esIntegrationComponent, IDocumentIndexStructureBuilder documentIndexStructureBuilder) {
		super(spaceKey, remoteSystemClient, esIntegrationComponent, documentIndexStructureBuilder);
		logger = Loggers.getLogger(SpaceSimpleIndexer.class);
		indexingInfo = new SpaceIndexingInfo(spaceKey, true);
	}

	@Override
	protected void processUpdate() throws Exception {
		indexingInfo.documentsUpdated = 0;

		logger.info("Go to perform full simple update for Space {}", spaceKey);

		ChangedDocumentsResults res = remoteSystemClient.getChangedDocuments(spaceKey, 0, null);

		if (res.getDocuments() != null && !res.getDocuments().isEmpty()) {
			if (isClosed())
				throw new InterruptedException("Interrupted because River is closed");

			int updatedInThisBulk = 0;
			BulkRequestBuilder esBulk = esIntegrationComponent.prepareESBulkRequestBuilder();
			for (Map<String, Object> document : res.getDocuments()) {
				String documentId = getDocumentIdChecked(document);
				if (getDocumentDetail(documentId, document)) {

					documentIndexStructureBuilder.indexDocument(esBulk, spaceKey, document);
					updatedInThisBulk++;

					if (updatedInThisBulk >= bulkSize) {
						executeBulkUpdate(esBulk);
						indexingInfo.documentsUpdated += updatedInThisBulk;
						esBulk = esIntegrationComponent.prepareESBulkRequestBuilder();
						updatedInThisBulk = 0;
					}
				}
				if (isClosed())
					throw new InterruptedException("Interrupted because River is closed");
			}

			if (updatedInThisBulk > 0) {
				executeBulkUpdate(esBulk);
				indexingInfo.documentsUpdated += updatedInThisBulk;
			}
		}
	}
}

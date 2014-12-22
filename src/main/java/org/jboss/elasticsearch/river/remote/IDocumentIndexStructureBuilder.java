/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.Date;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.jboss.elasticsearch.tools.content.StructuredContentPreprocessor;

/**
 * Interface for component responsible to transform document data obtained from remote instance call to the document
 * stored in ElasticSearch index. Implementation of this interface must be thread safe!
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public interface IDocumentIndexStructureBuilder {

	/**
	 * Add preprocessor to be used during document indexing in {@link #indexDocument(BulkRequestBuilder, String, Map)}.
	 * Config time method.
	 * 
	 * @param preprocessor to add
	 */
	void addDataPreprocessor(StructuredContentPreprocessor preprocessor);

	/**
	 * Get name of search index where documents are stored for given remote system Space
	 * 
	 * @return search index name
	 */
	String getDocumentSearchIndexName(String spaceKey);

	/**
	 * Get unique identifier for document from data obtained from remote system.
	 * 
	 * @param document data obtained from remote system to be indexed (JSON parsed into Map of Map structure)
	 * @return
	 */
	String extractDocumentId(Map<String, Object> document);

	/**
	 * Get date of last document update from data obtained from remote system.
	 * 
	 * @param document data obtained from remote system to be indexed (JSON parsed into Map of Map structure)
	 * @return date of last update
	 */
	Date extractDocumentUpdated(Map<String, Object> document);

	/**
	 * Get deleted flag from data obtained from remote system.
	 * 
	 * @param document data obtained from remote system to be indexed (JSON parsed into Map of Map structure)
	 * @return true if document is marked as deleted in remote data.
	 */
	boolean extractDocumentDeleted(Map<String, Object> document);

	/**
	 * Store/Update document obtained from remote system into search index.
	 * 
	 * @param esBulk bulk operation builder used to update document data in search index
	 * @param spaceKey indexed document is for
	 * @param document data obtained from remote system to be indexed (JSON parsed into Map of Map structure)
	 * @throws Exception
	 */
	void indexDocument(BulkRequestBuilder esBulk, String spaceKey, Map<String, Object> document) throws Exception;

	/**
	 * Construct search request to find remote document and comments indexed documents not updated after given date. Used
	 * during full index update to remove documents not presented in remote system anymore. Results from this query are
	 * processed by {@link #deleteESDocument(BulkRequestBuilder, SearchHit)}
	 * 
	 * @param srb search request builder to add necessary conditions into
	 * @param spaceKey to search documents for
	 * @param date bound date for search. All documents last updated in ES index before this date must be found by
	 *          constructed query
	 */
	void buildSearchForIndexedDocumentsNotUpdatedAfter(SearchRequestBuilder srb, String spaceKey, Date date);

	/**
	 * Construct search request to find remote document and comments indexed documents for given remote id. Used to delete
	 * documents marked with deleted flag in remote data. Results from this query are processed by
	 * {@link #deleteESDocument(BulkRequestBuilder, SearchHit)}
	 * 
	 * @param srb search request builder to add necessary conditions into
	 * @param spaceKey to search documents for
	 * @param remoteId all documents in ES index belonging to this remote id must be found by constructed query
	 */
	void buildSearchForIndexedDocumentsWithRemoteId(SearchRequestBuilder srb, String spaceKey, String remoteId);

	/**
	 * Delete remote doc related es document (document or comment) from search index. Query to obtain documents to be
	 * deleted is constructed using
	 * {@link #buildSearchForIndexedDocumentsNotUpdatedAfter(SearchRequestBuilder, String, Date)}
	 * 
	 * @param esBulk bulk operation builder used to delete data from search index
	 * @param documentToDelete found issue or comment document to delete from index
	 * @return true if deleted es document is document, else otherwise (eg. if deleted es document is comment)
	 * @throws Exception
	 */
	boolean deleteESDocument(BulkRequestBuilder esBulk, SearchHit documentToDelete) throws Exception;

}

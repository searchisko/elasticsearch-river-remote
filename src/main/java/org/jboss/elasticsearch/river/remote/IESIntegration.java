/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.river.RiverName;

/**
 * Interface for component which allows integration of indexer components into ElasticSearch River instance.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public interface IESIntegration {

	/**
	 * Get Space identifiers (keys) for all spaces in remote system which needs to be indexed. Is loaded from river
	 * configuration or from the remote system instance (in this case excludes are removed from it) - depends on river
	 * configuration.
	 * 
	 * @return list of space identifiers.
	 * @throws Exception
	 */
	List<String> getAllIndexedSpaceKeys() throws Exception;

	/**
	 * Callback method - report that indexing of some Space was finished. Used to coordinate parallel indexing of all
	 * spaces and gather indexing statistics/audit data.
	 * 
	 * @param indexingInfo info about finished indexing
	 */
	void reportIndexingFinished(SpaceIndexingInfo indexingInfo);

	/**
	 * Check if EclipseSearch instance is closed, so we must interrupt long running indexing processes.
	 * 
	 * @return true if ES instance is closed, so we must interrupt long running indexing processes
	 */
	boolean isClosed();

	/**
	 * Persistently store datetime value for remote system Space as document into ElasticSearch river configuration area.
	 * 
	 * @param spaceKey remote system space key this value is for
	 * @param propertyName name of property for this value identification
	 * @param datetime to be stored
	 * @param esBulk to be used for value store process, if <code>null</code> then value is stored immediately
	 * @throws IOException
	 * 
	 * @see {@link #readDatetimeValue(String, String)}
	 * @see #deleteDatetimeValue(String, String)
	 */
	void storeDatetimeValue(String spaceKey, String propertyName, Date datetime, BulkRequestBuilder esBulk)
			throws Exception;

	/**
	 * Read datetime value for remote system Space from document in ElasticSearch river configuration persistent area.
	 * 
	 * @param spaceKey remote system space key this value is for
	 * @param propertyName name of property for this value identification
	 * @return datetime or null if do not exists
	 * @throws IOException
	 * @see #storeDatetimeValue(String, String, Date, BulkRequestBuilder)
	 * @see #deleteDatetimeValue(String, String)
	 */
	Date readDatetimeValue(String spaceKey, String propertyName) throws Exception;

	/**
	 * Delete datetime value for remote system Space from document in ElasticSearch river configuration persistent area.
	 * 
	 * @param spaceKey to delete value for
	 * @param propertyName name of property for value identification
	 * @return true if document was found and deleted, false if not found
	 * @see #readDatetimeValue(String, String)
	 * @see #storeDatetimeValue(String, String, Date, BulkRequestBuilder)
	 */
	boolean deleteDatetimeValue(String spaceKey, String propertyName);

	/**
	 * Prepare ElasticSearch bulk request to be used for index update by more issues.
	 * 
	 * @return bulk request instance
	 * @see #executeESBulkRequest(BulkRequestBuilder)
	 */
	BulkRequestBuilder prepareESBulkRequestBuilder();

	/**
	 * Execute ElasticSearch bulk request against ElasticSearch cluster.
	 * 
	 * @param esBulk to perform
	 * @throws ElasticsearchException in case of fatal ES update failure
	 * @throws BulkUpdatePartialFailureException
	 * @see #prepareESBulkRequestBuilder()
	 */
	void executeESBulkRequest(BulkRequestBuilder esBulk) throws ElasticsearchException, BulkUpdatePartialFailureException;

	/**
	 * Acquire thread from ElasticSearch infrastructure to run indexing.
	 * 
	 * @param threadName name of thread
	 * @param runnable to run in this thread
	 * @return {@link Thread} instance - not started yet!
	 */
	Thread acquireIndexingThread(String threadName, Runnable runnable);

	/**
	 * Refresh search index to be up to date for search operations. See
	 * {@link IndicesAdminClient#refresh(org.elasticsearch.action.admin.indices.refresh.RefreshRequest)}.
	 * 
	 * @param indexName to be refreshed
	 */
	void refreshSearchIndex(String indexName);

	/**
	 * Prepare builder for Scroll Search request. See http://www.elasticsearch.org/guide/reference/java-api/search.html.
	 * 
	 * @param indexName name of index to prepare scroll for
	 * @return scroll search builder to be used
	 * @see #executeESSearchRequest(SearchRequestBuilder)
	 * @see #executeESScrollSearchNextRequest(SearchResponse)
	 */
	SearchRequestBuilder prepareESScrollSearchRequestBuilder(String indexName);

	/**
	 * Execute search on passed search request builder and return result. Can be used for normal search, or first search
	 * in Scroll scenario (http://www.elasticsearch.org/guide/reference/java-api/search.html).
	 * 
	 * @param searchRequestBuilder to execute
	 * @return actual response
	 */
	SearchResponse executeESSearchRequest(SearchRequestBuilder searchRequestBuilder);

	/**
	 * Execute subsequent scroll search request. See http://www.elasticsearch.org/guide/reference/java-api/search.html.
	 * 
	 * @param scrollResp response from previous scroll search request
	 * @return actual response
	 * @see #prepareESScrollSearchRequestBuilder(String)
	 * @see #executeESSearchRequest(SearchRequestBuilder)
	 */
	SearchResponse executeESScrollSearchNextRequest(SearchResponse scrollResp);

	/**
	 * Get name of the river.
	 * 
	 * @return name of the river.
	 */
	public RiverName riverName();

	/**
	 * Create logger for given class with all necessary context settings (eg. river name etc).
	 * 
	 * @param clazz to create logger for
	 * @return logger
	 */
	public ESLogger createLogger(Class<?> clazz);

}

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.List;
import java.util.Map;

/**
 * Info about changed documents returned from remote server. List of documents with pagination informations.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see IRemoteSystemClient#getChangedDocuments(String, int, java.util.Date)
 */
public class ChangedDocumentsResults {

	/**
	 * Starting position of returned documents in complete list of documents matching search in the remote system. 0
	 * based.
	 */
	private int startAt;

	/**
	 * Total number of documents in the remote system matching performed search criteria. Optional (null means not
	 * available from response).
	 */
	private Integer total;

	/**
	 * Documents returned from remote system - count may be limited due maxResults constraint, first document is from
	 * {@link #startAt} position.
	 */
	private List<Map<String, Object>> documents;

	/**
	 * Constructor.
	 * 
	 * @param documents returned from the remote system - count may be limited due maxResults constraint, first document
	 *          is from {@link #startAt} position.
	 * @param startAt Starting position of returned documents in complete list of documents matching search in the remote
	 *          system. 0 based.
	 * @param total number of documents in the remote system matching performed search criteria. Optional (only if remote
	 *          system is capable to return it).
	 */
	public ChangedDocumentsResults(List<Map<String, Object>> documents, Integer startAt, Integer total) {
		super();
		if (startAt == null) {
			throw new IllegalArgumentException("startAt cant be null");
		}
		this.documents = documents;
		this.startAt = startAt;
		this.total = total;
	}

	/**
	 * @return the startAt
	 */
	public int getStartAt() {
		return startAt;
	}

	/**
	 * @return the total
	 */
	public Integer getTotal() {
		return total;
	}

	/**
	 * @return the documents
	 */
	public List<Map<String, Object>> getDocuments() {
		return documents;
	}

	/**
	 * Get number of documents in this result part
	 * 
	 * @return
	 * @see #getDocuments()
	 */
	public int getDocumentsCount() {
		if (documents == null)
			return 0;
		return documents.size();
	}

	@Override
	public String toString() {
		return "ChangedDocumentsResults [startAt=" + startAt + ", total=" + total + ", documents=" + documents + "]";
	}

}

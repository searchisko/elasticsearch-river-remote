/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Value object holding info about one indexing run.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceIndexingInfo {

	private static final String DOCFIELD_DOCUMENTS_DELETED = "documents_deleted";
	private static final String DOCFIELD_COMMENTS_DELETED = "comments_deleted";
	private static final String DOCVAL_RESULT_OK = "OK";
	private static final String DOCVAL_TYPE_FULL = "FULL";
	public static final String DOCFIELD_ERROR_MESSAGE = "error_message";
	public static final String DOCFIELD_TIME_ELAPSED = "time_elapsed";
	public static final String DOCFIELD_RESULT = "result";
	public static final String DOCFIELD_DOCUMENTS_UPDATED = "documents_updated";
	public static final String DOCFIELD_DOCUMENTS_WITH_ERROR = "documents_with_error";
	public static final String DOCFIELD_UPDATE_TYPE = "update_type";
	public static final String DOCFIELD_START_DATE = "start_date";
	public static final String DOCFIELD_SPACE_KEY = "space_key";
	public static final String DOCFIELD_RIVER_NAME = "river_name";

	/**
	 * Key of Space this indexing is for.
	 */
	public String spaceKey;
	/**
	 * <code>true</code> if reported indexing was full update, <code>false</code> on incremental update.
	 */
	public boolean fullUpdate;
	/**
	 * Number of documents updated during this indexing run.
	 */
	public int documentsUpdated;
	/**
	 * Number of documents deleted during this indexing run.
	 */
	public int documentsDeleted;
	/**
	 * Number of documents not updated during this indexing run due errors.
	 */
	public int documentsWithError;
	/**
	 * Number of comments saved as separate es documents deleted during this indexing run.
	 */
	public int commentsDeleted;

	/**
	 * Date of indexing start.
	 */
	public Date startDate;
	/**
	 * <code>true</code> if indexing finished OK, <code>false</code> if finished due error.
	 */
	public boolean finishedOK;
	/**
	 * time of this indexing run [ms]. Available after finished.
	 */
	public long timeElapsed;
	/**
	 * error message if indexing finished with error
	 */
	private StringBuilder errorMessage = new StringBuilder();

	/**
	 * Partially filling constructor.
	 * 
	 * @param spaceKey
	 * @param fullUpdate
	 */
	public SpaceIndexingInfo(String spaceKey, boolean fullUpdate) {
		super();
		this.spaceKey = spaceKey;
		this.fullUpdate = fullUpdate;
	}

	/**
	 * Full filling constructor.
	 * 
	 * @param spaceKey
	 * @param fullUpdate
	 * @param documentsUpdated
	 * @param documentsDeleted
	 * @param commentsDeleted
	 * @param startDate
	 * @param finishedOK
	 * @param timeElapsed
	 * @param errorMessage
	 */
	public SpaceIndexingInfo(String spaceKey, boolean fullUpdate, int documentsUpdated, int documentsDeleted,
			int commentsDeleted, Date startDate, boolean finishedOK, long timeElapsed, String errorMessage) {
		super();
		this.spaceKey = spaceKey;
		this.fullUpdate = fullUpdate;
		this.documentsUpdated = documentsUpdated;
		this.documentsDeleted = documentsDeleted;
		this.commentsDeleted = commentsDeleted;
		this.startDate = startDate;
		this.finishedOK = finishedOK;
		this.timeElapsed = timeElapsed;
		errorMessage = Utils.trimToNull(errorMessage);
		if (errorMessage != null)
			this.errorMessage.append(errorMessage);
	}

	/**
	 * Add row of text into error message.
	 * 
	 * @param msgRow text to be added into message as row
	 */
	public void addErrorMessage(String msgRow) {
		msgRow = Utils.trimToNull(msgRow);
		if (msgRow != null) {
			if (errorMessage.length() != 0) {
				errorMessage.append("\n");
			}
			errorMessage.append(msgRow);
		}
	}

	/**
	 * Get error message.
	 * 
	 * @return error message.
	 */
	public String getErrorMessage() {
		if (errorMessage.length() == 0)
			return null;
		return errorMessage.toString();
	}

	/**
	 * Add object with space indexing info to given document builder.
	 * 
	 * @param builder to add information Object into
	 * @param riverName to be added into data. Not added if null
	 * @param printSpaceKey set to true to print Space key into document
	 * @param printFinalStatus set to true to print final status info into document
	 * @return builder same as on input.
	 * @throws IOException
	 */
	public XContentBuilder buildDocument(XContentBuilder builder, String riverName, boolean printSpaceKey,
			boolean printFinalStatus) throws IOException {
		builder.startObject();
		if (riverName != null)
			builder.field(DOCFIELD_RIVER_NAME, riverName);
		if (printSpaceKey) {
			builder.field(DOCFIELD_SPACE_KEY, spaceKey);
		}
		builder.field(DOCFIELD_UPDATE_TYPE, fullUpdate ? DOCVAL_TYPE_FULL : "INCREMENTAL");
		builder.field(DOCFIELD_START_DATE, startDate);
		builder.field(DOCFIELD_DOCUMENTS_UPDATED, documentsUpdated);
		builder.field(DOCFIELD_DOCUMENTS_DELETED, documentsDeleted);
		builder.field(DOCFIELD_COMMENTS_DELETED, commentsDeleted);
		builder.field(DOCFIELD_DOCUMENTS_WITH_ERROR, documentsWithError);
		if (printFinalStatus) {
			builder.field(DOCFIELD_RESULT, finishedOK ? DOCVAL_RESULT_OK : "ERROR");
			builder.field(DOCFIELD_TIME_ELAPSED, timeElapsed + "ms");
			if (!Utils.isEmpty(getErrorMessage())) {
				builder.field(DOCFIELD_ERROR_MESSAGE, getErrorMessage());
			}
		}
		builder.endObject();
		return builder;
	}

	/**
	 * Read object back from document created over {@link #buildDocument(XContentBuilder, boolean, boolean)}.
	 * 
	 * @param document to read
	 * @return object instance or null
	 */
	public static SpaceIndexingInfo readFromDocument(Map<String, Object> document) {
		if (document == null)
			return null;
		SpaceIndexingInfo ret = new SpaceIndexingInfo((String) document.get(DOCFIELD_SPACE_KEY),
				DOCVAL_TYPE_FULL.equals(document.get(DOCFIELD_UPDATE_TYPE)));
		ret.startDate = DateTimeUtils.parseISODateTime((String) document.get(DOCFIELD_START_DATE));
		ret.documentsUpdated = Utils.nodeIntegerValue(document.get(DOCFIELD_DOCUMENTS_UPDATED));
		ret.documentsDeleted = Utils.nodeIntegerValue(document.get(DOCFIELD_DOCUMENTS_DELETED));
		ret.commentsDeleted = Utils.nodeIntegerValue(document.get(DOCFIELD_COMMENTS_DELETED));
		ret.documentsWithError = Utils.nodeIntegerValue(document.get(DOCFIELD_DOCUMENTS_WITH_ERROR));
		ret.finishedOK = DOCVAL_RESULT_OK.equals(document.get(DOCFIELD_RESULT));
		ret.timeElapsed = Long.parseLong(((String) document.get(DOCFIELD_TIME_ELAPSED)).replace("ms", ""));
		ret.addErrorMessage((String) document.get(DOCFIELD_ERROR_MESSAGE));
		return ret;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((spaceKey == null) ? 0 : spaceKey.hashCode());
		return result;
	}

}

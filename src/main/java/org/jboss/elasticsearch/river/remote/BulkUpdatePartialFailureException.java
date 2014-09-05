/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

/**
 * Exception used when bulk update only fails partially.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see IESIntegration#executeESBulkRequest(org.elasticsearch.action.bulk.BulkRequestBuilder)
 */
public class BulkUpdatePartialFailureException extends Exception {

	private int numOfFailures;

	/**
	 * @param failureMessage
	 * @param numOfFailures number of failures in batch
	 */
	public BulkUpdatePartialFailureException(String failureMessage, int numOfFailures) {
		super(failureMessage);
		this.numOfFailures = numOfFailures;
	}

	/**
	 * Get number of failures in batch processing.
	 * 
	 * @return the numOfFailures
	 */
	public int getNumOfFailures() {
		return numOfFailures;
	}

}

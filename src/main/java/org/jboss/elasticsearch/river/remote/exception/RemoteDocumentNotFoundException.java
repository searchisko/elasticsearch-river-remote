/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.exception;

/**
 * Exception used when remote document details are not found on server.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class RemoteDocumentNotFoundException extends Exception {

	public RemoteDocumentNotFoundException() {
	}

	public RemoteDocumentNotFoundException(String message) {
		super(message);
	}

	public RemoteDocumentNotFoundException(Throwable cause) {
		super(cause);
	}

	public RemoteDocumentNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

}

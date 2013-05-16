/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.mgm.state;

import org.jboss.elasticsearch.river.remote.mgm.JRMgmBaseRequest;

/**
 * Request for RemoteRiver state information.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class JRStateRequest extends JRMgmBaseRequest<JRStateRequest> {

	JRStateRequest() {

	}

	/**
	 * Construct request.
	 * 
	 * @param riverName for request
	 */
	public JRStateRequest(String riverName) {
		super(riverName);
	}

	@Override
	public String toString() {
		return "JRStateRequest [riverName=" + riverName + "]";
	}

}

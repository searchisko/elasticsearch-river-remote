/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.mgm.riverslist;

import org.jboss.elasticsearch.river.remote.mgm.JRMgmBaseRequest;

/**
 * Request to list names of all Remote Rivers running in ES cluster.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class ListRiversRequest extends JRMgmBaseRequest<ListRiversRequest> {

	public ListRiversRequest() {
		super();
	}

}

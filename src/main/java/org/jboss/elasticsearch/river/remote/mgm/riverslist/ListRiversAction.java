/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.mgm.riverslist;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

/**
 * All Remote River names in ES cluster listing action implementation.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class ListRiversAction extends Action<ListRiversRequest, ListRiversResponse, ListRiversRequestBuilder> {

	public static final ListRiversAction INSTANCE = new ListRiversAction();
	public static final String NAME = "remote_river/list_river_names";

	protected ListRiversAction() {
		super(NAME);
	}

	@Override
	public ListRiversRequestBuilder newRequestBuilder(Client client) {
		return new ListRiversRequestBuilder(client);
	}

	@Override
	public ListRiversResponse newResponse() {
		return new ListRiversResponse();
	}

}

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.mgm.fullupdate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;

/**
 * Request builder to force full index update for some Remote river and some or all spaces in it.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class FullUpdateRequestBuilder extends
		NodesOperationRequestBuilder<FullUpdateRequest, FullUpdateResponse, FullUpdateRequestBuilder> {

	public FullUpdateRequestBuilder(ClusterAdminClient client) {
		super(client, new FullUpdateRequest());
	}

	/**
	 * Set name of river to force full index update for.
	 * 
	 * @param riverName name of river to force full index update for
	 * @return builder for chaining
	 */
	public FullUpdateRequestBuilder setRiverName(String riverName) {
		this.request.setRiverName(riverName);
		return this;
	}

	/**
	 * Set Space key to force full index update for. If not specified then full update is forced for all spaces managed by
	 * given river.
	 * 
	 * @param spaceKey to force full index update for
	 * @return builder for chaining
	 */
	public FullUpdateRequestBuilder setProjectKey(String spaceKey) {
		this.request.setSpaceKey(spaceKey);
		return this;
	}

	@Override
	protected void doExecute(ActionListener<FullUpdateResponse> listener) {
		if (request.getRiverName() == null)
			throw new IllegalArgumentException("riverName must be provided for request");
		client.execute(FullUpdateAction.INSTANCE, request, listener);
	}

}

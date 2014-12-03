/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.mgm.incrementalupdate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;

/**
 * Request builder to force incremental index update for some Remote river and some or all spaces in it.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class IncrementalUpdateRequestBuilder extends
		NodesOperationRequestBuilder<IncrementalUpdateRequest, IncrementalUpdateResponse, IncrementalUpdateRequestBuilder> {

	public IncrementalUpdateRequestBuilder(ClusterAdminClient client) {
		super(client, new IncrementalUpdateRequest());
	}

	/**
	 * Set name of river to force incremental index update for.
	 * 
	 * @param riverName name of river to force incremental index update for
	 * @return builder for chaining
	 */
	public IncrementalUpdateRequestBuilder setRiverName(String riverName) {
		this.request.setRiverName(riverName);
		return this;
	}

	/**
	 * Set Space key to force incremental index update for. If not specified then update is forced for all spaces managed
	 * by given river.
	 * 
	 * @param spaceKey to force incremental index update for
	 * @return builder for chaining
	 */
	public IncrementalUpdateRequestBuilder setProjectKey(String spaceKey) {
		this.request.setSpaceKey(spaceKey);
		return this;
	}

	@Override
	protected void doExecute(ActionListener<IncrementalUpdateResponse> listener) {
		if (request.getRiverName() == null)
			throw new IllegalArgumentException("riverName must be provided for request");
		client.execute(IncrementalUpdateAction.INSTANCE, request, listener);
	}

}

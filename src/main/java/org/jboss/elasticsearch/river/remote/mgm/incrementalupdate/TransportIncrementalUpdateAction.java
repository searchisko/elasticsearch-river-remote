/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.mgm.incrementalupdate;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jboss.elasticsearch.river.remote.IRiverMgm;
import org.jboss.elasticsearch.river.remote.mgm.TransportJRMgmBaseAction;

/**
 * Incremental reindex transport action.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class TransportIncrementalUpdateAction
		extends
		TransportJRMgmBaseAction<IncrementalUpdateRequest, IncrementalUpdateResponse, NodeIncrementalUpdateRequest, NodeIncrementalUpdateResponse> {

	@Inject
	public TransportIncrementalUpdateAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
			ClusterService clusterService, TransportService transportService) {
		super(settings, IncrementalUpdateAction.NAME, clusterName, threadPool, clusterService, transportService);
	}

	@Override
	protected NodeIncrementalUpdateResponse performOperationOnRiver(IRiverMgm river, IncrementalUpdateRequest req,
			DiscoveryNode node) throws Exception {
		logger.debug("Go to force incremental reindex for river '{}' and space {}", req.getRiverName(), req.getSpaceKey());
		String ret = river.forceIncrementalReindex(req.getSpaceKey());
		return new NodeIncrementalUpdateResponse(node, true, ret != null, ret);
	}

	@Override
	protected IncrementalUpdateRequest newRequest() {
		return new IncrementalUpdateRequest();
	}

	@Override
	protected NodeIncrementalUpdateRequest newNodeRequest() {
		return new NodeIncrementalUpdateRequest();
	}

	@Override
	protected NodeIncrementalUpdateRequest newNodeRequest(String nodeId, IncrementalUpdateRequest request) {
		return new NodeIncrementalUpdateRequest(nodeId, request);
	}

	@Override
	protected NodeIncrementalUpdateResponse newNodeResponse() {
		return new NodeIncrementalUpdateResponse(clusterService.localNode());
	}

	@Override
	protected NodeIncrementalUpdateResponse[] newNodeResponseArray(int len) {
		return new NodeIncrementalUpdateResponse[len];
	}

	@Override
	protected IncrementalUpdateResponse newResponse(ClusterName clusterName, NodeIncrementalUpdateResponse[] array) {
		return new IncrementalUpdateResponse(clusterName, array);
	}

}

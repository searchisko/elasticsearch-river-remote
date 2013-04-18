/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.mgm.fullupdate;

import static org.elasticsearch.rest.RestStatus.OK;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.jboss.elasticsearch.river.remote.mgm.JRMgmBaseActionListener;
import org.jboss.elasticsearch.river.remote.mgm.RestJRMgmBaseAction;

/**
 * REST action handler for force full index update operation.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class RestFullUpdateAction extends RestJRMgmBaseAction {

	@Inject
	protected RestFullUpdateAction(Settings settings, Client client, RestController controller) {
		super(settings, client);
		String baseUrl = baseRestMgmUrl();
		controller.registerHandler(org.elasticsearch.rest.RestRequest.Method.POST, baseUrl + "fullupdate", this);
		controller.registerHandler(org.elasticsearch.rest.RestRequest.Method.POST, baseUrl + "fullupdate/{spaceKey}", this);
	}

	@Override
	public void handleRequest(final RestRequest restRequest, final RestChannel restChannel) {

		final String riverName = restRequest.param("riverName");
		final String spaceKey = restRequest.param("spaceKey");

		FullUpdateRequest actionRequest = new FullUpdateRequest(riverName, spaceKey);

		client.execute(FullUpdateAction.INSTANCE, actionRequest,
				new JRMgmBaseActionListener<FullUpdateRequest, FullUpdateResponse, NodeFullUpdateResponse>(actionRequest,
						restRequest, restChannel) {

					@Override
					protected void handleRiverResponse(NodeFullUpdateResponse nodeInfo) throws Exception {
						if (actionRequest.isSpaceKeyRequest() && !nodeInfo.spaceFound) {
							restChannel.sendResponse(new XContentRestResponse(restRequest, RestStatus.NOT_FOUND,
									buildMessageDocument(restRequest, "Space '" + spaceKey
											+ "' is not indexed by RemoteRiver with name: " + riverName)));
						} else {
							restChannel.sendResponse(new XContentRestResponse(restRequest, OK, buildMessageDocument(restRequest,
									"Scheduled full reindex for Spaces: " + nodeInfo.reindexedSpaces)));
						}
					}

				});
	}

}

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.mgm.riverslist;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.jboss.elasticsearch.river.remote.mgm.RestJRMgmBaseAction;

/**
 * REST action handler for Remote river get state operation.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class RestListRiversAction extends RestJRMgmBaseAction {

	@Inject
	protected RestListRiversAction(Settings settings, Client client, RestController controller) {
		super(settings, client);
		controller.registerHandler(org.elasticsearch.rest.RestRequest.Method.GET, "/_remote_river/list", this);
	}

	@Override
	public void handleRequest(final RestRequest restRequest, final RestChannel restChannel) {

		ListRiversRequest actionRequest = new ListRiversRequest();

		logger.debug("Go to look for remote rivers in the cluster");
		client.execute(ListRiversAction.INSTANCE, actionRequest, new ActionListener<ListRiversResponse>() {

			@Override
			public void onResponse(ListRiversResponse response) {
				try {
					Set<String> allRivers = new HashSet<String>();
					for (NodeListRiversResponse node : response.nodes()) {
						if (node.riverNames != null) {
							allRivers.addAll(node.riverNames);
						}
					}

					XContentBuilder builder = RestXContentBuilder.restContentBuilder(restRequest);
					builder.startObject();
					builder.field("river_names", allRivers);
					builder.endObject();
					restChannel.sendResponse(new XContentRestResponse(restRequest, RestStatus.OK, builder));
				} catch (Exception e) {
					onFailure(e);
				}
			}

			@Override
			public void onFailure(Throwable e) {
				try {
					restChannel.sendResponse(new XContentThrowableRestResponse(restRequest, e));
				} catch (IOException e1) {
					logger.error("Failed to send failure response", e1);
				}
			}

		});
	}
}

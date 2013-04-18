package org.jboss.elasticsearch.river.remote.mgm.state;

import org.jboss.elasticsearch.river.remote.mgm.NodeJRMgmBaseRequest;

/**
 * Node request for RemoteRiver state information.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class NodeJRStateRequest extends NodeJRMgmBaseRequest<JRStateRequest> {

	NodeJRStateRequest() {
		super();
	}

	/**
	 * Construct node request with data.
	 * 
	 * @param nodeId this request is for
	 * @param request to be send to the node
	 */
	NodeJRStateRequest(String nodeId, JRStateRequest request) {
		super(nodeId, request);
	}

	@Override
	protected JRStateRequest newRequest() {
		return new JRStateRequest();
	}

}

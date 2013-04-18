package org.jboss.elasticsearch.river.remote.mgm.riverslist;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jboss.elasticsearch.river.remote.mgm.NodeJRMgmBaseResponse;

/**
 * node response with list names of all Remote Rivers running in ES cluster.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class NodeListRiversResponse extends NodeJRMgmBaseResponse {

	Set<String> riverNames;

	protected NodeListRiversResponse() {
	}

	public NodeListRiversResponse(DiscoveryNode node) {
		super(node);
	}

	/**
	 * Create response with values to be send back to requestor.
	 * 
	 * @param node this response is for.
	 * @param riverNames set of remote river names found on this node.
	 */
	public NodeListRiversResponse(DiscoveryNode node, Set<String> riverNames) {
		super(node, riverNames != null && !riverNames.isEmpty());
		this.riverNames = riverNames;
	}

	@Override
	public void readFrom(StreamInput in) throws IOException {
		super.readFrom(in);
		int len = in.readInt();
		if (len >= 0) {
			riverNames = new HashSet<String>();
			for (int i = 0; i < len; i++) {
				riverNames.add(in.readString());
			}
		}
	}

	@Override
	public void writeTo(StreamOutput out) throws IOException {
		super.writeTo(out);
		if (riverNames == null) {
			out.writeInt(-1);
		} else {
			out.writeInt(riverNames.size());
			if (riverNames != null) {
				for (String s : riverNames) {
					out.writeString(s);
				}
			}
		}
	}

}

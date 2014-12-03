package org.jboss.elasticsearch.river.remote.mgm.incrementalupdate;

import java.io.IOException;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jboss.elasticsearch.river.remote.mgm.NodeJRMgmBaseResponse;

/**
 * Incfremental reindex node response.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class NodeIncrementalUpdateResponse extends NodeJRMgmBaseResponse {

	protected boolean spaceFound;

	protected String reindexedSpaces;

	protected NodeIncrementalUpdateResponse() {
	}

	public NodeIncrementalUpdateResponse(DiscoveryNode node) {
		super(node);
	}

	/**
	 * Create response with values to be send back to requestor.
	 * 
	 * @param node this response is for.
	 * @param riverFound set to true if you found river on this node
	 * @param spaceFound set to true if space reindex was requested and we found this space in given river
	 * @param reindexedSpaces CSV names of Spaces which was forced for incremental reindex
	 */
	public NodeIncrementalUpdateResponse(DiscoveryNode node, boolean riverFound, boolean spaceFound,
			String reindexedSpaces) {
		super(node, riverFound);
		this.spaceFound = spaceFound;
		this.reindexedSpaces = reindexedSpaces;
	}

	@Override
	public void readFrom(StreamInput in) throws IOException {
		super.readFrom(in);
		spaceFound = in.readBoolean();
		reindexedSpaces = in.readOptionalString();
	}

	@Override
	public void writeTo(StreamOutput out) throws IOException {
		super.writeTo(out);
		out.writeBoolean(spaceFound);
		out.writeOptionalString(reindexedSpaces);
	}

	public boolean isSpaceFound() {
		return spaceFound;
	}

	public String getReindexedSpaces() {
		return reindexedSpaces;
	}

}

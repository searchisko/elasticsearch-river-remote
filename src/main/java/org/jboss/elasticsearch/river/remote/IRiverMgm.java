package org.jboss.elasticsearch.river.remote;

import java.util.Date;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.river.RiverName;

/**
 * Interface with remote river management operations called over REST API.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public interface IRiverMgm {

	/**
	 * Stop river, but leave instance existing in {@link #riverInstances} so it can be found over management REST calls
	 * and/or reconfigured and started later again. Note that standard ES river {@link #close()} method implementation
	 * removes river instance from {@link #riverInstances}.
	 * 
	 * @param permanent set to true if info about river stopped can be persisted
	 */
	public abstract void stop(boolean permanent);

	/**
	 * Restart river. Configuration of river is updated.
	 */
	public abstract void restart();

	/**
	 * Force full index update for some Space(s) in this river. Used for REST management operations handling.
	 * 
	 * @param spaceKey optional key of space to reindex, if null or empty then all spaces are forced to full reindex
	 * @return CSV list of spaces forced to reindex. <code>null</code> if space passed over <code>spaceKey</code>
	 *         parameter was not found in this indexer
	 * @throws Exception
	 */
	public abstract String forceFullReindex(String spaceKey) throws Exception;

	/**
	 * Force incremental index update for some Space(s) in this river. Used for REST management operations handling.
	 * 
	 * @param spaceKey optional key of space to reindex, if null or empty then all spaces are forced to reindex
	 * @return CSV list of spaces forced to reindex. <code>null</code> if space passed over <code>spaceKey</code>
	 *         parameter was not found in this indexer
	 * @throws Exception
	 */
	public abstract String forceIncrementalReindex(String spaceKey) throws Exception;

	/**
	 * Get info about current operation of this river. Used for REST management operations handling.
	 * 
	 * @return String with JSON formatted info.
	 * @throws Exception
	 */
	public abstract String getRiverOperationInfo(DiscoveryNode esNode, Date currentDate) throws Exception;

	/**
	 * Get name of river.
	 * 
	 * @return name of the river
	 */
	public abstract RiverName riverName();

}
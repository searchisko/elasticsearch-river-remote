package org.jboss.elasticsearch.river.remote;

import java.util.List;

/**
 * Interface for remote system Spaces indexer coordinator component.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public interface ISpaceIndexerCoordinator extends Runnable {

	/**
	 * Report that indexing of one Space has been finished. Used to coordinate parallel indexing of all spaces.
	 * Implementation of this method must be thread safe!
	 * 
	 * @param spaceKey for finished indexing
	 * @param finishedOK set to <code>true</code> if indexing finished OK, <code>false</code> if finished due error
	 * @param fullUpdate set to <code>true</code> if reported indexing was full update, <code>false</code> on incremental
	 *          update
	 */
	public abstract void reportIndexingFinished(String spaceKey, boolean finishedOK, boolean fullUpdate);

	/**
	 * Force full reindex for given Space.
	 * 
	 * @param spaceKey to force reindex for
	 * @throws Exception
	 */
	void forceFullReindex(String spaceKey) throws Exception;

	/**
	 * Force incremental reindex for given Space.
	 * 
	 * @param spaceKey to force reindex for
	 * @throws Exception
	 */
	void forceIncrementalReindex(String spaceKey) throws Exception;

	/**
	 * Get info about current indexings in process.
	 * 
	 * @return list of currently processed indexings.
	 */
	public abstract List<SpaceIndexingInfo> getCurrentSpaceIndexingInfo();

}
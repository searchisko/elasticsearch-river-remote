package org.jboss.elasticsearch.river.remote;

import java.util.Date;
import java.util.List;

/**
 * Interface for Remote system calls Client implementation.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public interface IRemoteSystemClient {

	/**
	 * Get keys of all Spaces in configured remote system.
	 * 
	 * @return list of remote system Space keys
	 * @throws Exception
	 */
	public abstract List<String> getAllSpaces() throws Exception;

	/**
	 * Get list of documents from remote system and parse them into <code>Map of Maps</code> structure. Documents MUST BE
	 * ascending ordered by date of last update. List is limited to only some number of documents (given by both remote
	 * system and this client configuration).
	 * 
	 * @param spaceKey mandatory key of Space to get documents for
	 * @param startAt the index of the first issue to return (0-based)
	 * @param updatedAfter optional parameter to return documents updated only after given date.
	 * @return List of document informations parsed from remote system reply into <code>Map of Maps</code> structure.
	 * @throws Exception
	 */
	public abstract ChangedDocumentsResults getChangedDocuments(String spaceKey, int startAt, Date updatedAfter)
			throws Exception;

	/**
	 * Add index structure builder so remote system client can use it (for example it can request from remote system only
	 * fields necessary for indexing etc.).
	 * 
	 * @param indexStructureBuilder
	 * @see IDocumentIndexStructureBuilder#getRequiredRemoteCallFields()
	 */
	public void setIndexStructureBuilder(IDocumentIndexStructureBuilder indexStructureBuilder);

	/**
	 * Get actual index structure builder.
	 * 
	 * @return actual index structure builder
	 */
	public IDocumentIndexStructureBuilder getIndexStructureBuilder();

}
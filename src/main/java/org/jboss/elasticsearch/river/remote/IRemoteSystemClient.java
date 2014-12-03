package org.jboss.elasticsearch.river.remote;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;
import org.jboss.elasticsearch.river.remote.exception.RemoteDocumentNotFoundException;

/**
 * Interface for Remote system calls Client implementation. Only one instance is created by river, so implementation
 * must be thread safe.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public interface IRemoteSystemClient {

	/**
	 * Initialize client. Called from river in init time.
	 * 
	 * @param esIntegration providing context
	 * @param config configuration structure (taken from <code>remote</code> element in river configuration)
	 * @param spaceListLoadingEnabled if <code>true</code> then {@link #getAllSpaces()} will be called by river to load
	 *          list of spaces from remote system. So client must be initialized to be able to use this method.
	 * @param pwdLoader used to load password if not defined in <code>config</code>
	 * @throws SettingsException
	 */
	public abstract void init(IESIntegration esIntegration, Map<String, Object> config, boolean spaceListLoadingEnabled,
			IPwdLoader pwdLoader) throws SettingsException;

	/**
	 * Set index structure builder so remote system client can use it (for example it can request from remote system only
	 * fields necessary for indexing etc.). Called after <code>init</code method.
	 * 
	 * @param indexStructureBuilder
	 * @see IDocumentIndexStructureBuilder#getRequiredRemoteCallFields()
	 */
	public void setIndexStructureBuilder(IDocumentIndexStructureBuilder indexStructureBuilder);

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
	 * Get detailed data for document from remote system.
	 * 
	 * @param spaceKey mandatory key of Space to get document details for
	 * @param documentId mandatory document id to return document details for
	 * @param document whole document data returned from list operation
	 * @return detailed document informations parsed from remote system reply (may be Map, or List, or simple value). May
	 *         be null.
	 * @throws RemoteDocumentNotFoundException if document is not found on remote server
	 * @throws Exception in case of other problems
	 */
	public abstract Object getChangedDocumentDetails(String spaceKey, String documentId, Map<String, Object> document)
			throws RemoteDocumentNotFoundException, Exception;

	/**
	 * Get actual index structure builder.
	 * 
	 * @return actual index structure builder
	 */
	public IDocumentIndexStructureBuilder getIndexStructureBuilder();

}
/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.jboss.elasticsearch.tools.content.StructuredContentPreprocessor;

import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Universal configurable implementation of component responsible to transform document data obtained from remote system
 * call to the document stored in ElasticSearch index. Supports comment tied to document too.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class DocumentWithCommentsIndexStructureBuilder implements IDocumentIndexStructureBuilder {

	private static final ESLogger logger = Loggers.getLogger(DocumentWithCommentsIndexStructureBuilder.class);

	/**
	 * Name of River to be stored in document to mark indexing source
	 */
	protected String riverName;

	/**
	 * Name of ElasticSearch index used to store issues
	 */
	protected String indexName;

	/**
	 * Name of ElasticSearch type used to store issues into index
	 */
	protected String issueTypeName;

	protected static final String CONFIG_FIELDS = "fields";
	protected static final String CONFIG_FIELDS_REMOTEFIELD = "remote_field";
	protected static final String CONFIG_FIELDS_VALUEFILTER = "value_filter";
	protected static final String CONFIG_FILTERS = "value_filters";
	protected static final String CONFIG_REMOTEFIELD_DOCUMENTID = "remote_field_document_id";
	protected static final String CONFIG_REMOTEFIELD_UPDATED = "remote_field_updated";
	protected static final String CONFIG_REMOTEFIELD_COMMENTS = "remote_field_comments";
	protected static final String CONFIG_REMOTEFIELD_COMMENTID = "remote_field_comment_id";
	protected static final String CONFIG_FIELDRIVERNAME = "field_river_name";
	protected static final String CONFIG_FIELDSPACEKEY = "field_space_key";
	protected static final String CONFIG_FIELDDOCUMENTID = "field_document_id";
	protected static final String CONFIG_COMMENTMODE = "comment_mode";
	protected static final String CONFIG_FIELDCOMMENTS = "field_comments";
	protected static final String CONFIG_COMMENTTYPE = "comment_type";
	protected static final String CONFIG_COMMENTFILEDS = "comment_fields";

	/**
	 * Field in remote document data to get indexed document id from.
	 */
	protected String remoteDataFieldForDocumentId = null;

	/**
	 * Field in remote document data to get indexed document last update timestamp from.
	 */
	protected String remoteDataFieldForUpdated = null;

	/**
	 * Field in remote document data to get array of comments from.
	 */
	protected String remoteDataFieldForComments = null;

	/**
	 * Field in remote document's comment structure to get comment id from. Is necessary if comment is indexed as
	 * standalone document.
	 */
	protected String remoteDataFieldForCommentId = null;

	/**
	 * Fields configuration structure. Key is name of field in search index. Value is map of configurations for given
	 * index field containing <code>CONFIG_FIELDS_xx</code> constants as keys.
	 */
	protected Map<String, Map<String, String>> fieldsConfig;

	/**
	 * Value filters configuration structure. Key is name of filter. Value is map of filter configurations to be used in
	 * {@link Utils#remapDataInMap(Map, Map)}.
	 */
	protected Map<String, Map<String, String>> filtersConfig;

	/**
	 * Name of field in search index where river name is stored.
	 */
	protected String indexFieldForRiverName = null;

	/**
	 * Name of field in search index where Space key is stored.
	 */
	protected String indexFieldForSpaceKey = null;

	/**
	 * Name of field in search index where remote document unique identifier is stored.
	 */
	protected String indexFieldForRemoteDocumentId = null;

	/**
	 * Issue comment indexing mode.
	 */
	protected CommentIndexingMode commentIndexingMode;

	/**
	 * Name of field in search index issue document where array of comments is stored in case of
	 * {@link CommentIndexingMode#EMBEDDED}.
	 */
	protected String indexFieldForComments = null;

	/**
	 * Name of ElasticSearch type used to store issues comments into index in case of
	 * {@link CommentIndexingMode#STANDALONE} or {@link CommentIndexingMode#CHILD}.
	 */
	protected String commentTypeName;

	/**
	 * Fields configuration structure for comment document. Key is name of field in search index. Value is map of
	 * configurations for given index field containing <code>CONFIG_FIELDS_xx</code> constants as keys.
	 */
	protected Map<String, Map<String, String>> commentFieldsConfig;

	/**
	 * List of data preprocessors used inside {@link #indexDocument(BulkRequestBuilder, String, Map)}.
	 */
	protected List<StructuredContentPreprocessor> issueDataPreprocessors = null;

	/**
	 * Constructor for unit tests. Nothing is filled inside.
	 */
	protected DocumentWithCommentsIndexStructureBuilder() {
		super();
	}

	/**
	 * Constructor.
	 * 
	 * @param riverName name of ElasticSearch River instance this indexer is running inside to be stored in search index
	 *          to identify indexed documents source.
	 * @param indexName name of ElasticSearch index used to store issues
	 * @param issueTypeName name of ElasticSearch type used to store issues into index
	 * @param settings map to load other structure builder settings from
	 * @param dateOfUpdateFieldMandatory defines if {@link #CONFIG_REMOTEFIELD_UPDATED} config field is mandatory or not
	 *          (depends on used indexing process)
	 * @throws SettingsException
	 */
	@SuppressWarnings("unchecked")
	public DocumentWithCommentsIndexStructureBuilder(String riverName, String indexName, String issueTypeName,
			Map<String, Object> settings, boolean dateOfUpdateFieldMandatory) throws SettingsException {
		super();
		this.riverName = riverName;
		this.indexName = indexName;
		this.issueTypeName = issueTypeName;

		if (settings != null) {
			remoteDataFieldForDocumentId = Utils.trimToNull(XContentMapValues.nodeStringValue(
					settings.get(CONFIG_REMOTEFIELD_DOCUMENTID), null));
			remoteDataFieldForUpdated = Utils.trimToNull(XContentMapValues.nodeStringValue(
					settings.get(CONFIG_REMOTEFIELD_UPDATED), null));
			remoteDataFieldForComments = Utils.trimToNull(XContentMapValues.nodeStringValue(
					settings.get(CONFIG_REMOTEFIELD_COMMENTS), null));
			remoteDataFieldForCommentId = Utils.trimToNull(XContentMapValues.nodeStringValue(
					settings.get(CONFIG_REMOTEFIELD_COMMENTID), null));

			indexFieldForRiverName = XContentMapValues.nodeStringValue(settings.get(CONFIG_FIELDRIVERNAME), null);
			indexFieldForSpaceKey = XContentMapValues.nodeStringValue(settings.get(CONFIG_FIELDSPACEKEY), null);
			indexFieldForRemoteDocumentId = XContentMapValues.nodeStringValue(settings.get(CONFIG_FIELDDOCUMENTID), null);
			filtersConfig = (Map<String, Map<String, String>>) settings.get(CONFIG_FILTERS);
			fieldsConfig = (Map<String, Map<String, String>>) settings.get(CONFIG_FIELDS);

			commentIndexingMode = CommentIndexingMode.parseConfiguration(XContentMapValues.nodeStringValue(
					settings.get(CONFIG_COMMENTMODE), null));
			indexFieldForComments = XContentMapValues.nodeStringValue(settings.get(CONFIG_FIELDCOMMENTS), null);
			commentTypeName = XContentMapValues.nodeStringValue(settings.get(CONFIG_COMMENTTYPE), null);
			commentFieldsConfig = (Map<String, Map<String, String>>) settings.get(CONFIG_COMMENTFILEDS);
		}
		loadDefaultsIfNecessary();
		validateConfiguration(dateOfUpdateFieldMandatory);
	}

	private void loadDefaultsIfNecessary() {
		Map<String, Object> settingsDefault = loadDefaultSettingsMapFromFile();

		indexFieldForRiverName = loadDefaultStringIfNecessary(indexFieldForRiverName, CONFIG_FIELDRIVERNAME,
				settingsDefault);
		indexFieldForSpaceKey = loadDefaultStringIfNecessary(indexFieldForSpaceKey, CONFIG_FIELDSPACEKEY, settingsDefault);
		indexFieldForRemoteDocumentId = loadDefaultStringIfNecessary(indexFieldForRemoteDocumentId, CONFIG_FIELDDOCUMENTID,
				settingsDefault);

		if (filtersConfig == null)
			filtersConfig = new HashMap<String, Map<String, String>>();

		if (commentIndexingMode == null) {
			commentIndexingMode = CommentIndexingMode.parseConfiguration(XContentMapValues.nodeStringValue(
					settingsDefault.get(CONFIG_COMMENTMODE), null));
		}
	}

	private void validateConfiguration(boolean dateOfUpdateMandatory) {

		validateConfigurationString(remoteDataFieldForDocumentId, "index/" + CONFIG_REMOTEFIELD_DOCUMENTID);
		if (dateOfUpdateMandatory)
			validateConfigurationString(remoteDataFieldForUpdated, "index/" + CONFIG_REMOTEFIELD_UPDATED);

		validateConfigurationObject(filtersConfig, "index/value_filters");
		validateConfigurationObject(fieldsConfig, "index/fields");
		validateConfigurationString(indexFieldForRiverName, "index/field_river_name");
		validateConfigurationString(indexFieldForSpaceKey, "index/field_space_key");
		validateConfigurationString(indexFieldForRemoteDocumentId, "index/field_document_id");
		validateConfigurationObject(commentIndexingMode, "index/comment_mode");
		if (commentIndexingMode != CommentIndexingMode.NONE) {
			validateConfigurationString(remoteDataFieldForComments, "index/" + CONFIG_REMOTEFIELD_COMMENTS);
			validateConfigurationString(indexFieldForComments, "index/field_comments");
			validateConfigurationObject(commentFieldsConfig, "index/comment_fields");
			validateConfigurationFieldsStructure(commentFieldsConfig, "index/comment_fields");
			if (commentIndexingMode.isExtraDocumentIndexed()) {
				validateConfigurationString(commentTypeName, "index/comment_type");
				validateConfigurationString(remoteDataFieldForCommentId, "index/" + CONFIG_REMOTEFIELD_COMMENTID);
			}
		}

		validateConfigurationFieldsStructure(fieldsConfig, "index/fields");

	}

	@Override
	public void addDataPreprocessor(StructuredContentPreprocessor preprocessor) {
		if (preprocessor == null)
			return;
		if (issueDataPreprocessors == null)
			issueDataPreprocessors = new ArrayList<StructuredContentPreprocessor>();
		issueDataPreprocessors.add(preprocessor);
	}

	@Override
	public String getDocumentSearchIndexName(String spaceKey) {
		return indexName;
	}

	@Override
	public void indexDocument(BulkRequestBuilder esBulk, String spaceKey, Map<String, Object> document) throws Exception {

		document.put("spaceKey", spaceKey);
		document = preprocessDocumentData(spaceKey, document);
		esBulk.add(indexRequest(indexName).type(issueTypeName).id(extractDocumentId(document))
				.source(prepareIndexedDocument(spaceKey, document)));

		if (commentIndexingMode.isExtraDocumentIndexed()) {
			List<Map<String, Object>> comments = extractComments(document);
			if (comments != null && !comments.isEmpty()) {
				String issueKey = extractDocumentId(document);
				for (Map<String, Object> comment : comments) {
					String commentId = extractCommentId(comment);
					IndexRequest irq = indexRequest(indexName).type(commentTypeName).id(commentId)
							.source(prepareCommentIndexedDocument(spaceKey, issueKey, comment));
					if (commentIndexingMode == CommentIndexingMode.CHILD) {
						irq.parent(issueKey);
					}
					esBulk.add(irq);
				}
			}
		}

	}

	@Override
	public String extractDocumentId(Map<String, Object> document) {
		return extractIdValueFromDocumentField(document, remoteDataFieldForDocumentId, CONFIG_REMOTEFIELD_DOCUMENTID);
	}

	private String extractIdValueFromDocumentField(Map<String, Object> document, String idFieldName,
			String idFieldConfigPropertyName) {
		Object id = XContentMapValues.extractValue(idFieldName, document);
		if (id == null)
			return null;
		if (!isSimpleValue(id))
			throw new SettingsException("Remote data field '" + idFieldName + "' defined in 'index/"
					+ idFieldConfigPropertyName + "' config param must provide simple value, but value is " + id);

		return id.toString();
	}

	private boolean isSimpleValue(Object value) {
		return (value instanceof String || value instanceof Integer || value instanceof Long || value instanceof Date);
	}

	@Override
	public Date extractDocumentUpdated(Map<String, Object> document) {
		Object val = XContentMapValues.extractValue(remoteDataFieldForUpdated, document);
		if (val == null)
			return null;
		if (!isSimpleValue(val))
			throw new SettingsException("Remote data field '" + remoteDataFieldForUpdated
					+ "' must provide simple value, but value is " + val);

		if (val instanceof Date)
			return (Date) val;

		try {
			// try simple timestamp
			return new Date(Long.parseLong(val.toString()));
		} catch (NumberFormatException e) {
			// try ISO format
			try {
				return DateTimeUtils.parseISODateTime(val.toString());
			} catch (IllegalArgumentException e1) {
				throw new SettingsException("Remote data field '" + remoteDataFieldForUpdated
						+ "' is not reecognized as timestamp value (ISO format or number with millis from 1.1.1970): " + val);
			}
		}
	}

	protected String extractCommentId(Map<String, Object> comment) {
		String commentId = extractIdValueFromDocumentField(comment, remoteDataFieldForCommentId,
				CONFIG_REMOTEFIELD_COMMENTID);
		if (commentId == null) {
			throw new IllegalArgumentException("Comment ID not found in remote system response within data: " + comment);
		}
		return commentId;
	}

	/**
	 * Preprocess document data over all configured preprocessors.
	 * 
	 * @param spaceKey document is for
	 * @param document data to preprocess
	 * @return preprocessed data
	 */
	protected Map<String, Object> preprocessDocumentData(String spaceKey, Map<String, Object> document) {
		if (issueDataPreprocessors != null) {
			for (StructuredContentPreprocessor prepr : issueDataPreprocessors) {
				document = prepr.preprocessData(document);
			}
		}
		return document;
	}

	@Override
	public void buildSearchForIndexedDocumentsNotUpdatedAfter(SearchRequestBuilder srb, String spaceKey, Date date) {
		FilterBuilder filterTime = FilterBuilders.rangeFilter("_timestamp").lt(date);
		FilterBuilder filterProject = FilterBuilders.termFilter(indexFieldForSpaceKey, spaceKey);
		FilterBuilder filterSource = FilterBuilders.termFilter(indexFieldForRiverName, riverName);
		FilterBuilder filter = FilterBuilders.boolFilter().must(filterTime).must(filterProject).must(filterSource);
		srb.setQuery(QueryBuilders.matchAllQuery()).addField("_id").setFilter(filter);
		if (commentIndexingMode.isExtraDocumentIndexed())
			srb.setTypes(issueTypeName, commentTypeName);
		else
			srb.setTypes(issueTypeName);
	}

	@Override
	public boolean deleteESDocument(BulkRequestBuilder esBulk, SearchHit documentToDelete) throws Exception {
		esBulk.add(deleteRequest(indexName).type(documentToDelete.getType()).id(documentToDelete.getId()));
		return issueTypeName.equals(documentToDelete.getType());
	}

	/**
	 * Convert remote system returned document data into JSON document to be stored in search index.
	 * 
	 * @param spaceKey key of space document is for.
	 * @param documentRemote data from remote system REST call
	 * @return JSON builder with document for index
	 * @throws Exception
	 */
	protected XContentBuilder prepareIndexedDocument(String spaceKey, Map<String, Object> documentRemote)
			throws Exception {
		String documentId = extractDocumentId(documentRemote);

		XContentBuilder out = jsonBuilder().startObject();
		addValueToTheIndexField(out, indexFieldForRiverName, riverName);
		addValueToTheIndexField(out, indexFieldForSpaceKey, spaceKey);
		addValueToTheIndexField(out, indexFieldForRemoteDocumentId, documentId);

		for (String indexFieldName : fieldsConfig.keySet()) {
			Map<String, String> fieldConfig = fieldsConfig.get(indexFieldName);
			addValueToTheIndex(out, indexFieldName, fieldConfig.get(CONFIG_FIELDS_REMOTEFIELD), documentRemote,
					fieldConfig.get(CONFIG_FIELDS_VALUEFILTER));
		}

		if (commentIndexingMode == CommentIndexingMode.EMBEDDED) {
			List<Map<String, Object>> comments = extractComments(documentRemote);
			if (comments != null && !comments.isEmpty()) {
				out.startArray(indexFieldForComments);
				for (Map<String, Object> comment : comments) {
					out.startObject();
					addCommonFieldsToCommentIndexedDocument(out, documentId, comment);
					out.endObject();
				}
				out.endArray();
			}
		}
		return out.endObject();
	}

	/**
	 * Convert remote system's returned data into JSON document to be stored in search index for comments in child and
	 * standalone mode.
	 * 
	 * @param spaceKey key of space document is for.
	 * @param documentId this comment is for
	 * @param comment data from remote system document
	 * @return JSON builder with comment document for index
	 * @throws Exception
	 */
	protected XContentBuilder prepareCommentIndexedDocument(String spaceKey, String documentId,
			Map<String, Object> comment) throws Exception {
		XContentBuilder out = jsonBuilder().startObject();
		addValueToTheIndexField(out, indexFieldForRiverName, riverName);
		addValueToTheIndexField(out, indexFieldForSpaceKey, spaceKey);
		addValueToTheIndexField(out, indexFieldForRemoteDocumentId, documentId);
		addCommonFieldsToCommentIndexedDocument(out, documentId, comment);
		return out.endObject();
	}

	private void addCommonFieldsToCommentIndexedDocument(XContentBuilder out, String documentId,
			Map<String, Object> comment) throws Exception {
		for (String indexFieldName : commentFieldsConfig.keySet()) {
			Map<String, String> commentFieldConfig = commentFieldsConfig.get(indexFieldName);
			addValueToTheIndex(out, indexFieldName, commentFieldConfig.get(CONFIG_FIELDS_REMOTEFIELD), comment,
					commentFieldConfig.get(CONFIG_FIELDS_VALUEFILTER));
		}
	}

	/**
	 * Get all comments for document from remote system JSON data.
	 * 
	 * @param document Map of Maps document data structure loaded from remote system.
	 * @return list of comments if available in data
	 */
	@SuppressWarnings("unchecked")
	protected List<Map<String, Object>> extractComments(Map<String, Object> document) {
		List<Map<String, Object>> comments = (List<Map<String, Object>>) XContentMapValues.extractValue(
				remoteDataFieldForComments, document);
		return comments;
	}

	/**
	 * Get defined value from values structure and add it into index document. Calls
	 * {@link #addValueToTheIndex(XContentBuilder, String, String, Map, Map)} and receive filter from
	 * {@link #filtersConfig} based on passed <code>valueFieldFilterName</code>)
	 * 
	 * @param out content builder to add indexed value field into
	 * @param indexField name of field for index
	 * @param valuePath path to get value from <code>values</code> structure. Dot notation for nested values can be used
	 *          here (see {@link XContentMapValues#extractValue(String, Map)}).
	 * @param values structure to get value from. Can be <code>null</code> - nothing added in this case, but not
	 *          exception.
	 * @param valueFieldFilterName name of filter definition to get it from {@link #filtersConfig}
	 * @throws Exception
	 */
	protected void addValueToTheIndex(XContentBuilder out, String indexField, String valuePath,
			Map<String, Object> values, String valueFieldFilterName) throws Exception {
		Map<String, String> filter = null;
		if (!Utils.isEmpty(valueFieldFilterName)) {
			filter = filtersConfig.get(valueFieldFilterName);
		}
		addValueToTheIndex(out, indexField, valuePath, values, filter);
	}

	/**
	 * Get defined value from values structure and add it into index document.
	 * 
	 * @param out content builder to add indexed value field into
	 * @param indexField name of field for index
	 * @param valuePath path to get value from <code>values</code> structure. Dot notation for nested values can be used
	 *          here (see {@link XContentMapValues#extractValue(String, Map)}).
	 * @param values structure to get value from. Can be <code>null</code> - nothing added in this case, but not
	 *          exception.
	 * @param valueFieldFilter if value is JSON Object (java Map here) or List of JSON Objects, then fields in this
	 *          objects are filtered to leave only fields named here and remap them - see
	 *          {@link Utils#remapDataInMap(Map, Map)}. No filtering performed if this is <code>null</code>.
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	protected void addValueToTheIndex(XContentBuilder out, String indexField, String valuePath,
			Map<String, Object> values, Map<String, String> valueFieldFilter) throws Exception {
		if (values == null) {
			return;
		}
		Object v = null;
		if (valuePath.contains(".")) {
			v = XContentMapValues.extractValue(valuePath, values);
		} else {
			v = values.get(valuePath);
		}
		if (v != null && valueFieldFilter != null && !valueFieldFilter.isEmpty()) {
			if (v instanceof Map) {
				Utils.remapDataInMap((Map<String, Object>) v, valueFieldFilter);
			} else if (v instanceof List) {
				for (Object o : (List<?>) v) {
					if (o instanceof Map) {
						Utils.remapDataInMap((Map<String, Object>) o, valueFieldFilter);
					} else {
						logger.warn(
								"Filter defined for field which is not filterable - remote document array field '{}' with value: {}",
								valuePath, v);
					}
				}
			} else {
				logger.warn("Filter defined for field which is not filterable - remote document field '{}' with value: {}",
						valuePath, v);
			}
		}
		addValueToTheIndexField(out, indexField, v);
	}

	/**
	 * Add value into field in index document. Do not add it if value is <code>null</code>!
	 * 
	 * @param out builder to add field into.
	 * @param indexField real name of field used in index.
	 * @param value to be added to the index field. Can be <code>null</code>, nothing added in this case
	 * @throws Exception
	 * 
	 * @see {@link XContentBuilder#field(String, Object)}.
	 */
	protected void addValueToTheIndexField(XContentBuilder out, String indexField, Object value) throws Exception {
		if (value != null)
			out.field(indexField, value);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> loadDefaultSettingsMapFromFile() throws SettingsException {
		Map<String, Object> json = Utils.loadJSONFromJarPackagedFile("/templates/remote_river_configuration_default.json");
		return (Map<String, Object>) json.get("index");
	}

	private String loadDefaultStringIfNecessary(String valueToCheck, String valueConfigKey,
			Map<String, Object> settingsDefault) {
		if (Utils.isEmpty(valueToCheck)) {
			return XContentMapValues.nodeStringValue(settingsDefault.get(valueConfigKey), null);
		} else {
			return valueToCheck.trim();
		}
	}

	private void validateConfigurationFieldsStructure(Map<String, Map<String, String>> value, String configFieldName) {
		for (String idxFieldName : value.keySet()) {
			if (Utils.isEmpty(idxFieldName)) {
				throw new SettingsException("Empty key found in '" + configFieldName + "' map.");
			}
			Map<String, String> fc = value.get(idxFieldName);
			if (Utils.isEmpty(fc.get(CONFIG_FIELDS_REMOTEFIELD))) {
				throw new SettingsException("'remote_field' is not defined in '" + configFieldName + "/" + idxFieldName + "'");
			}
			String fil = fc.get(CONFIG_FIELDS_VALUEFILTER);
			if (fil != null && !filtersConfig.containsKey(fil)) {
				throw new SettingsException("Filter definition not found for filter name '" + fil + "' defined in '"
						+ configFieldName + "/" + idxFieldName + "/value_filter'");
			}
		}
	}

	private void validateConfigurationString(String value, String configFieldName) throws SettingsException {
		if (Utils.isEmpty(value)) {
			throw new SettingsException("String value must be provided for '" + configFieldName + "' configuration!");
		}
	}

	private void validateConfigurationObject(Object value, String configFieldName) throws SettingsException {
		if (value == null) {
			throw new SettingsException("Value must be provided for '" + configFieldName + "' configuration!");
		}
	}

}

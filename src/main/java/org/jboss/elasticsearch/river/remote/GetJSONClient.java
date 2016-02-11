/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.jboss.elasticsearch.river.remote.exception.RemoteDocumentNotFoundException;

/**
 * Class used to call remote system by http GET operation with JSON response.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class GetJSONClient extends HttpRemoteSystemClientBase {

	protected static final String CFG_GET_DOCS_RES_FIELD_TOTALCOUNT = "getDocsResFieldTotalcount";

	protected static final String CFG_GET_DOCS_RES_FIELD_DOCUMENTS = "getDocsResFieldDocuments";
	
	protected static final String CFG_GET_ROOT_RES_FIELDS_MAPPING="getRootResFieldsMapping";

	protected static final String CFG_GET_SPACES_RESPONSE_FIELD = "getSpacesResField";

	protected static final String CFG_URL_GET_SPACES = "urlGetSpaces";

	protected static final String CFG_URL_GET_DOCUMENTS = "urlGetDocuments";

    protected static final String CFG_MIN_GET_DOCUMENTS_DELAY = "minGetDocumentsDelay";

    protected static final String CFG_FORCED_INDEXING_PAUSE_FIELD= "forcedIndexingPauseField";
    protected static final String CFG_FORCED_INDEXING_PAUSE_FIELD_TIME_UNIT= "forcedIndexingPauseFieldTimeUnit";

	protected static final String CFG_URL_GET_DOCUMENT_DETAILS = "urlGetDocumentDetails";
	protected static final String CFG_URL_GET_DOCUMENT_DETAILS_FIELD = "urlGetDocumentDetailsField";
	
	protected static final String CFG_UPDATED_AFTER_FORMAT = "updatedAfterFormat";
	protected static final String CFG_UPDATED_AFTER_INITIAL_VALUE = "updatedAfterInitialValue";
	
	protected static final String CFG_UPDATED_BEFORE_TIME_SPAN_FROM_UPDATED_AFTER = "updatedBeforeTimeSpanFromUpdatedAfter";
	
	protected static final String CFG_HTTP_METHOD = "httpMethod";

	protected static final String CFG_HEADER_ACCEPT = "headerAccept";

	private ESLogger logger = Loggers.getLogger(GetJSONClient.class);

	protected String urlGetSpaces;

	protected String getSpacesResField;

	protected String getDocsResFieldDocuments;
	
	protected Map<String,Object> getRootResFieldsMapping;

	protected String getDocsResFieldTotalcount;
	
	protected String updatedAfterFormat;
	
	protected Long updatedAfterInitialValue;
	
	protected Long updatedBeforeTimeSpanFromUpdatedAfter;

	protected HttpMethodType httpMethod;
	
	protected String urlGetDocuments;
	
	protected Long minGetDocumentsDelay;
	
	protected String forcedIndexingPauseField;
	protected TimeUnit forcedIndexingPauseFieldTimeUnit;

	protected String urlGetDocumentDetails;

	protected String urlGetDocumentDetailsField;
	
	protected AtomicReference<Long> blockHttpCallsTill = new AtomicReference<Long>();

	protected static final String HEADER_ACCEPT_DEFAULT = "application/json";

	protected Map<String, String> headers = new HashMap<String, String>();
	
	protected ThreadLocal<Long> previousHttpCall = new ThreadLocal<Long>();

	@Override
	public void init(IESIntegration esIntegration, Map<String, Object> config, boolean spaceListLoadingEnabled,
			IPwdLoader pwdLoader) {
		logger = esIntegration.createLogger(GetJSONClient.class);
		urlGetDocuments = getUrlFromConfig(config, CFG_URL_GET_DOCUMENTS, true);
		
		String minGetDocumentsDelayStr = Utils.trimToNull(XContentMapValues.nodeStringValue(config.get(CFG_MIN_GET_DOCUMENTS_DELAY),null));
		if(minGetDocumentsDelayStr!=null) {
		    try {
		        minGetDocumentsDelay = Long.parseLong(minGetDocumentsDelayStr);
		        minGetDocumentsDelay = minGetDocumentsDelay<=0L || minGetDocumentsDelay==null ? null : minGetDocumentsDelay;
		    } catch(NumberFormatException e) {
		        logger.warn("River configuration field "+CFG_MIN_GET_DOCUMENTS_DELAY+" was initiated with a bad value: "+minGetDocumentsDelayStr);
		        minGetDocumentsDelay = null;
		    }
		}
		
		forcedIndexingPauseField = Utils.trimToNull(XContentMapValues.nodeStringValue(config.get(CFG_FORCED_INDEXING_PAUSE_FIELD),null));
		String forcedIndexingPauseFieldTimeUnitStr = Utils.trimToNull(
		        XContentMapValues.nodeStringValue(config.get(CFG_FORCED_INDEXING_PAUSE_FIELD_TIME_UNIT),null));
		
		if( forcedIndexingPauseFieldTimeUnitStr!=null ) {
    		try {
    		    forcedIndexingPauseFieldTimeUnit = TimeUnit.valueOf(forcedIndexingPauseFieldTimeUnitStr);
    		} catch( IllegalArgumentException e ) {
    		    logger.warn("Provided "+CFG_FORCED_INDEXING_PAUSE_FIELD_TIME_UNIT+" field value of: "+forcedIndexingPauseFieldTimeUnit
    		            + ". For this reason the default assumption of MILLISECONDS will be applied.");
    		    forcedIndexingPauseFieldTimeUnit = TimeUnit.MILLISECONDS;
    		}
		} else {
		    forcedIndexingPauseFieldTimeUnit = TimeUnit.MILLISECONDS;
		}
		
		urlGetDocumentDetails = getUrlFromConfig(config, CFG_URL_GET_DOCUMENT_DETAILS, false);
		urlGetDocumentDetailsField = Utils.trimToNull(XContentMapValues.nodeStringValue(
				config.get(CFG_URL_GET_DOCUMENT_DETAILS_FIELD), null));

		if (urlGetDocumentDetails != null && urlGetDocumentDetailsField != null) {
			throw new SettingsException("You can use only one of remote/" + CFG_URL_GET_DOCUMENT_DETAILS + " and remote/"
					+ CFG_URL_GET_DOCUMENT_DETAILS_FIELD + " configuration parametr.");
		}

		getDocsResFieldDocuments = Utils.trimToNull(XContentMapValues.nodeStringValue(
				config.get(CFG_GET_DOCS_RES_FIELD_DOCUMENTS), null));
		getDocsResFieldTotalcount = Utils.trimToNull(XContentMapValues.nodeStringValue(
				config.get(CFG_GET_DOCS_RES_FIELD_TOTALCOUNT), null));
		getRootResFieldsMapping = config.get(CFG_GET_ROOT_RES_FIELDS_MAPPING)!=null ? XContentMapValues.nodeMapValue(config.get(CFG_GET_ROOT_RES_FIELDS_MAPPING),
				CFG_GET_ROOT_RES_FIELDS_MAPPING) : null;
		getRootResFieldsMapping = getRootResFieldsMapping==null || getRootResFieldsMapping.size()==0 ? null : getRootResFieldsMapping;
		
		updatedAfterFormat = Utils.trimToNull(XContentMapValues.nodeStringValue(
                config.get(CFG_UPDATED_AFTER_FORMAT), DateTimeUtils.CUSTOM_MILLISEC_EPOCH_DATETIME_FORMAT ));
		String updatedAfterStartStr = Utils.trimToNull(XContentMapValues.nodeStringValue(
                config.get(CFG_UPDATED_AFTER_INITIAL_VALUE), null ));
		try {
		    updatedAfterInitialValue = updatedAfterStartStr!=null ? Long.valueOf(updatedAfterStartStr) : null;
        } catch (NumberFormatException e) {
            updatedAfterInitialValue=null;
            throw new SettingsException("updatedAfterStart field could not be parsed as a long number. Please specify the number of miliseconds"
                    + " for the initial updatedAfter date value using only digit characters.",e);
        }
		
		String timeSpanStr = Utils.trimToNull(XContentMapValues.nodeStringValue(
                config.get(CFG_UPDATED_BEFORE_TIME_SPAN_FROM_UPDATED_AFTER), null ));
		try {
            updatedBeforeTimeSpanFromUpdatedAfter = timeSpanStr!=null ? Long.valueOf(timeSpanStr) : null;
        } catch (NumberFormatException e) {
            updatedBeforeTimeSpanFromUpdatedAfter=null;
            throw new SettingsException("timeSpan field could not be parsed as a long number. Please specify the number of miliseconds"
                    + " for the time span using only digit characters.",e);
        }
		
		String httpMethodStr = Utils.trimToNull(XContentMapValues.nodeStringValue(
                config.get(CFG_HTTP_METHOD), null ));
		httpMethod = httpMethodStr==null ? HttpMethodType.GET : HttpMethodType.valueOf(httpMethodStr);
		
		String headerAccept = Utils.trimToNull(XContentMapValues.nodeStringValue(config.get(CFG_HEADER_ACCEPT),
				HEADER_ACCEPT_DEFAULT));
		if (headerAccept == null)
			headerAccept = HEADER_ACCEPT_DEFAULT;

		headers.put("Accept", headerAccept);

		if (spaceListLoadingEnabled) {
			urlGetSpaces = getUrlFromConfig(config, CFG_URL_GET_SPACES, true);
			getSpacesResField = Utils.trimToNull(XContentMapValues.nodeStringValue(config.get(CFG_GET_SPACES_RESPONSE_FIELD),
					null));
		}

		String remoteUsername = initHttpClient(logger, config, pwdLoader, urlGetDocuments);

		logger
				.info(
						"Configured GET JSON remote client. Spaces listing URL '{}', documents listing url '{}', document detail url '{}', document detail url field '{}', remote system user '{}'.",
						urlGetSpaces != null ? urlGetSpaces : "unused", urlGetDocuments,
						urlGetDocumentDetails != null ? urlGetDocumentDetails : "",
						urlGetDocumentDetailsField != null ? urlGetDocumentDetailsField : "",
						remoteUsername != null ? remoteUsername : "Anonymous access");

	}

	@Override
	@SuppressWarnings("unchecked")
	public List<String> getAllSpaces() throws Exception {
		byte[] responseData = performHttpGetCall(urlGetSpaces, headers).content;
		logger.debug("Get Spaces REST response data: {}", new String(responseData));

		Object responseParsed = parseJSONResponse(responseData);

		List<String> ret = new ArrayList<String>();
		if (getSpacesResField != null) {
			if (responseParsed instanceof Map) {
				responseParsed = XContentMapValues.extractValue(getSpacesResField, ((Map<String, Object>) responseParsed));
			} else {
				throw new Exception(
						"Get Spaces REST response structure is unsupported (we need a Map to take configured getSpacesResponseField from it) "
								+ responseParsed);
			}
		}
		try {
			if (responseParsed instanceof List) {
				List<Object> l = (List<Object>) responseParsed;
				for (Object s : l) {
					ret.add(s.toString());
				}
			} else if (responseParsed instanceof Map) {
				Map<String, Object> l = (Map<String, Object>) responseParsed;
				for (String s : l.keySet()) {
					ret.add(s);
				}
			} else {
				throw new Exception("Get Spaces REST response structure is unsupported " + responseParsed);
			}
		} catch (ClassCastException e) {
			throw new Exception("Get Spaces REST response structure is unsupported " + responseParsed);
		}
		return ret;
	}

	/**
	 * Parse JSON response into Object Structure.
	 * 
	 * @param responseData to parse
	 * @return parsed response (May be Map, or List, or simple value)
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	protected Object parseJSONResponse(byte[] responseData) throws UnsupportedEncodingException, IOException {
		XContentParser parser = null;
		// wrap response so we are able to process JSON array of values on root level of response, which is not supported
		// by XContentFactory
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("{ \"wrapit\" : ").append(new String(responseData, "UTF-8")).append("}");
			responseData = sb.toString().getBytes("UTF-8");
			parser = XContentFactory.xContent(XContentType.JSON).createParser(responseData);
			Map<String, Object> wrappedResponseParsed = parser.mapAndClose();
			return wrappedResponseParsed.get("wrapit");
		} finally {
			if (parser != null)
				parser.close();
		}
	}

	@Override
	public Object getChangedDocumentDetails(String spaceKey, String documentId, Map<String, Object> document)
			throws Exception, RemoteDocumentNotFoundException {
		try {
			String url = null;
			if (urlGetDocumentDetailsField != null) {
				if (document != null) {
					try {
						url = Utils.trimToNull((String) XContentMapValues.extractValue(urlGetDocumentDetailsField, document));
					} catch (Exception e) {
						// warning logged later
					}
				}
				if (url == null) {
					logger.warn("Document detail URL not found in field '{}' for space '" + spaceKey + "' and document id="
							+ documentId, urlGetDocumentDetailsField);
				} else {
					try {
						new URL(url);
					} catch (MalformedURLException e) {
						logger.warn("Invalid document detail URL '{}' obtained from field '{}' for space '" + spaceKey
								+ "' and document id=" + documentId, new Object[] { url, urlGetDocumentDetailsField });
						url = null;
					}
				}
			} else if (urlGetDocumentDetails != null) {
				url = enhanceUrlGetDocumentDetails(urlGetDocumentDetails, spaceKey, documentId);
			}
			if (url == null)
				return null;
			byte[] responseData = performHttpGetCall(url, headers).content;
			return parseJSONResponse(responseData);
		} catch (HttpCallException e) {
			if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
				throw new RemoteDocumentNotFoundException(e);
			} else {
				throw e;
			}
		}
	}

	protected static String enhanceUrlGetDocumentDetails(String url, String spaceKey, String documentId)
			throws UnsupportedEncodingException {
		url = url.replaceAll("\\{space\\}", URLEncoder.encode(spaceKey, "UTF-8"));
		url = url.replaceAll("\\{id\\}", URLEncoder.encode(documentId, "UTF-8"));
		return url;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public ChangedDocumentsResults getChangedDocuments(String spaceKey, int startAt, boolean fullUpdate, Date updatedAfter)
			throws Exception {
		String url = enhanceUrlGetDocuments(urlGetDocuments, spaceKey, updatedAfter, updatedAfterFormat, updatedAfterInitialValue, updatedBeforeTimeSpanFromUpdatedAfter, startAt, fullUpdate);
		
		if( blockHttpCallsTill.get()!=null ) {
			
			// IF THE THREAD PAUSE TIME HAPPENS TO BE IN THE PAST WE JUST CLEAR THE VALUE AND CONTINUE PROCESSING.
			if( blockHttpCallsTill.get() <= System.currentTimeMillis() ) {
				blockHttpCallsTill.set(null);
			} else {
				try {
		            Thread.sleep( Math.abs( blockHttpCallsTill.get()-System.currentTimeMillis() ) );
		        } catch( InterruptedException e ) {
		            logger.warn("Thread was unexpectedly woken up from sleep. Trying to keep indexing the content.");
		        }
			}
		}
		
		if( minGetDocumentsDelay!=null ) {
		    // If we are running faster than the defined delay between HTTP calls we need to wait for the remaining time.
		    if( previousHttpCall.get()!=null && (previousHttpCall.get()+minGetDocumentsDelay) > System.currentTimeMillis() ) {
		    	try {
		            Thread.sleep( Math.abs( previousHttpCall.get()+minGetDocumentsDelay-System.currentTimeMillis() ) );
		        } catch( InterruptedException e ) {
		            logger.warn("Thread was unexpectedly woken up from sleep. Trying to keep indexing the content.");
		        }
		    }
		    
		    previousHttpCall.set(System.currentTimeMillis());
		}
		
		byte[] responseData = performHttpCall(url, headers, httpMethod).content;

		logger.debug("Get Documents REST response data: {}", new String(responseData));

		try {
			Object responseParsed = parseJSONResponse(responseData);
			
			// Check if REST API didn't return indexing pause parameter. 
			// In this case we'll have to wait with the next call.
			if (forcedIndexingPauseField != null) {
				Object forceIndexingPauseObj = XContentMapValues.extractValue(forcedIndexingPauseField, (Map) responseParsed);
				Integer forceIndexingPauseVal = null;
				if (forceIndexingPauseObj != null) {
					try {
						if (forceIndexingPauseObj instanceof Integer) {
							forceIndexingPauseVal = (Integer) forceIndexingPauseObj;
						} else {
							forceIndexingPauseVal = Integer.parseInt(forceIndexingPauseObj.toString());
						}
						
						// We increase waiting time by 10% just to be absolutely sure to obey the limit.
						blockHttpCallsTill.set( System.currentTimeMillis()
								+ (long)(forcedIndexingPauseFieldTimeUnit.toMillis(forceIndexingPauseVal)*1.1) );
                        
					} catch (NumberFormatException e) {
						logger.warn("Value from configured "+CFG_FORCED_INDEXING_PAUSE_FIELD+
								" field is not convertable to number: " + forceIndexingPauseObj.toString());
					}
				}
			}
			
			Integer total = null;
			if (getDocsResFieldTotalcount != null) {
				Object totalObj = XContentMapValues.extractValue(getDocsResFieldTotalcount, (Map) responseParsed);
				if (totalObj != null) {
					if (totalObj instanceof Integer)
						total = (Integer) totalObj;
					else
						try {
							total = Integer.parseInt(totalObj.toString());
						} catch (NumberFormatException e) {
							throw new Exception(
									"Value from configured getDocsResFieldTotalcount field is not convertable to number: " + totalObj);
						}
				} else {
					throw new Exception("Configured getDocsResFieldTotalcount field has no value");
				}
			}
			
			// Getting fields located outside of getDocsResFieldDocuments collection and translating their names.
			Map<String,Object> additionalFieldsForEntries = new HashMap<String,Object>();
			if( getRootResFieldsMapping!=null ) {
				
				for( String endFieldName : getRootResFieldsMapping.keySet() ) {
					String sourceFieldName = getRootResFieldsMapping.get(endFieldName).toString();
					Object fieldValue = XContentMapValues.extractValue(sourceFieldName.toString(),(Map)responseParsed);
			    	additionalFieldsForEntries.put( endFieldName , fieldValue );
			    }
				
			}

			List<Map<String, Object>> documents = null;
			if (getDocsResFieldDocuments != null) {
				documents = (List<Map<String, Object>>) XContentMapValues.extractValue(getDocsResFieldDocuments,
						(Map) responseParsed);
			} else {
				documents = (List<Map<String, Object>>) responseParsed;
			}
			
			// Adding all additional root fields to each collection entry.
			for( Map<String,Object> values : documents ) {
				values.putAll(additionalFieldsForEntries);
			}

			return new ChangedDocumentsResults(documents, startAt, total);
		} catch (ClassCastException e) {
			throw new Exception("Get Documents REST response structure is invalid " + responseData);
		}
	}

	protected static String enhanceUrlGetDocuments(String url, String spaceKey, Date updatedAfter, String updatedAfterFormat,
	        Long updatedAfterInitialValue, Long updatedBeforeTimeSpan, int startAt, boolean fullUpdate) throws UnsupportedEncodingException {
	    
	    String dateFormatToUse = updatedAfterFormat!=null && updatedAfterFormat.length()!=0
	            ? updatedAfterFormat
	            : DateTimeUtils.CUSTOM_MILLISEC_EPOCH_DATETIME_FORMAT;
	    
	    updatedAfter = updatedAfter!=null ? updatedAfter : ( updatedAfterInitialValue!=null ? new Date(updatedAfterInitialValue) : null ) ;
	    
		url = url.replaceAll("\\{space\\}", URLEncoder.encode(spaceKey, "UTF-8"));
		url = url.replaceAll("\\{updatedAfter\\}", updatedAfter != null ? URLEncoder.encode(DateTimeUtils.formatDateTime(updatedAfter, dateFormatToUse), "UTF-8") : "");
		url = url.replaceAll("\\{updatedBefore\\}", updatedBeforeTimeSpan != null && updatedAfter!=null 
		        ? URLEncoder.encode(DateTimeUtils.formatDateTime(new Date(updatedAfter.getTime()+updatedBeforeTimeSpan), dateFormatToUse), "UTF-8") : "");
		url = url.replaceAll("\\{startAtIndex\\}", startAt + "");
		url = url.replaceAll("\\{indexingType\\}", fullUpdate ? "full" : "inc");
		return url;
	}

	@Override
	public void setIndexStructureBuilder(IDocumentIndexStructureBuilder indexStructureBuilder) {
		this.indexStructureBuilder = indexStructureBuilder;
	}

	@Override
	public IDocumentIndexStructureBuilder getIndexStructureBuilder() {
		return indexStructureBuilder;
	}

}

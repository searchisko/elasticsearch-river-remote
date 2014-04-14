/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.content;

import java.util.Date;
import java.util.Map;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.jboss.elasticsearch.river.remote.DateTimeUtils;
import org.jboss.elasticsearch.tools.content.PreprocessChainContext;
import org.jboss.elasticsearch.tools.content.StructureUtils;
import org.jboss.elasticsearch.tools.content.StructuredContentPreprocessorFactory;
import org.jboss.elasticsearch.tools.content.StructuredContentPreprocessorWithSourceBasesBase;

/**
 * Content preprocessor which allows to transform {@link Long} value from source field to the ISO formatted timestamp
 * value and store it to another or same target field. Example of configuration for this preprocessor:
 * 
 * <pre>
 * { 
 *     "name"     : "Timestamp transformer",
 *     "class"    : "org.jboss.elasticsearch.tools.content.LongToTimestampValuePreprocessor",
 *     "settings" : {
 *         "source_field"  : "fields.updated",
 *         "target_field"  : "dcp_updated"
 *     } 
 * }
 * </pre>
 * 
 * Options are:
 * <ul>
 * <li><code>source_field</code> - source field in input data. Dot notation for nested values can be used here (see
 * {@link XContentMapValues#extractValue(String, Map)}).
 * <li><code>target_field</code> - target field in data to store transformed value into. Can be same as input field. Dot
 * notation can be used here for structure nesting.
 * <li><code>source_bases</code> - list of fields in source data which are used as bases for conversion. If defined then
 * conversion is performed for each of this fields, <code>source_field</code> and <code>target_field</code> are resolved
 * relatively against this base. Base must provide object or list of objects.
 * </ul>
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 */
public class LongToTimestampValuePreprocessor extends StructuredContentPreprocessorWithSourceBasesBase<Object> {

	protected static final String CFG_SOURCE_FIELD = "source_field";
	protected static final String CFG_TARGET_FIELD = "target_field";

	protected String fieldSource;
	protected String fieldTarget;

	@Override
	public void init(Map<String, Object> settings) throws SettingsException {
		super.init(settings);
		fieldSource = XContentMapValues.nodeStringValue(settings.get(CFG_SOURCE_FIELD), null);
		validateConfigurationStringNotEmpty(fieldSource, CFG_SOURCE_FIELD);
		fieldTarget = XContentMapValues.nodeStringValue(settings.get(CFG_TARGET_FIELD), null);
		validateConfigurationStringNotEmpty(fieldTarget, CFG_TARGET_FIELD);
	}

	@Override
	protected void processOneSourceValue(Map<String, Object> data, Object context, String base,
			PreprocessChainContext chainContext) {
		Object v = null;
		if (fieldSource.contains(".")) {
			v = XContentMapValues.extractValue(fieldSource, data);
		} else {
			v = data.get(fieldSource);
		}

		if (v != null) {
			if (v instanceof Long) {
				putTargetValue(data, DateTimeUtils.formatISODateTime(new Date((Long) v)));
			} else if (v instanceof String) {
				putTargetValue(data, DateTimeUtils.formatISODateTime(new Date(new Long((String) v))));
			} else {
				String warningMessage = "value for field '" + fieldSource + "' is not Long but is " + v.getClass().getName()
						+ ", so can't be processed";
				addDataWarning(chainContext, warningMessage);
				logger.debug(warningMessage);
			}
		}
	}

	@Override
	protected Object createContext() {
		return null;
	}

	protected void putTargetValue(Map<String, Object> data, Object value) {
		StructureUtils.putValueIntoMapOfMaps(data, fieldTarget, value);
	}

	public String getFieldSource() {
		return fieldSource;
	}

	public String getFieldTarget() {
		return fieldTarget;
	}

}

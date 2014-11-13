/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import org.elasticsearch.common.settings.SettingsException;

/**
 * Constants defining space indexing moce.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see SpaceIndexerCoordinator#prepareSpaceIndexer(String, boolean)
 */
public enum SpaceIndexingMode {
	SIMPLE("simple", false), PAGINATION("pagination", false), UPDATE_TIMESTAMP("updateTimestamp", true);

	private String configValue;
	private boolean updateDateMandatory;

	private SpaceIndexingMode(String configValue, boolean updateDateMandatory) {
		this.configValue = configValue;
		this.updateDateMandatory = updateDateMandatory;
	}

	/**
	 * Get value used to represent this value in configuration.
	 * 
	 * @return configuration value
	 */
	public String getConfigValue() {
		return configValue;
	}

	/**
	 * Check if date of last update in data from remote system is mandatory for this mode.
	 * 
	 * @return true if field is mandatory
	 */
	public boolean isUpdateDateMandatory() {
		return updateDateMandatory;
	}

	/**
	 * Parse mode constant from configuration.
	 * 
	 * @param value to parse
	 * @return null or relevant mode constant
	 */
	public static SpaceIndexingMode parseConfiguration(String value) {
		value = Utils.trimToNull(value);
		if (value == null)
			return null;

		if (SIMPLE.getConfigValue().equalsIgnoreCase(value)) {
			return SIMPLE;
		} else if (PAGINATION.getConfigValue().equalsIgnoreCase(value)) {
			return PAGINATION;
		} else if (UPDATE_TIMESTAMP.getConfigValue().equalsIgnoreCase(value)) {
			return UPDATE_TIMESTAMP;
		} else {
			throw new SettingsException("unsupported value for space indexing mode: " + value);
		}

	}
}

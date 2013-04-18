/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import org.elasticsearch.common.settings.SettingsException;

/**
 * Mode of comments indexing. Used to configure {@link IDocumentIndexStructureBuilder} implementations.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public enum CommentIndexingMode {

	/**
	 * Comments are not placed into search index.
	 */
	NONE("none", false),

	/**
	 * Comments are placed into search index as array in same ES document where document is stored.
	 */
	EMBEDDED("embedded", false),

	/**
	 * Comments are placed into search index in separate ES document with parent-child relation to document.
	 */
	CHILD("child", true),

	/**
	 * Comments are placed into search index in separate ES document with parent document id only in one field (no any
	 * other link to parent document).
	 */
	STANDALONE("standalone", true);

	private String configValue;

	private boolean extraDocumentIndexed;

	private CommentIndexingMode(String configValue, boolean extraDocumentIndexed) {
		this.configValue = configValue;
		this.extraDocumentIndexed = extraDocumentIndexed;
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
	 * Check if extra document is placed into search index for comment in this mode.
	 * 
	 * @return true if extra document is placed into search index
	 */
	public boolean isExtraDocumentIndexed() {
		return extraDocumentIndexed;
	}

	/**
	 * Get enum value based on String value read from configuration file.
	 * 
	 * @param value to be parsed
	 * @return Enum value, never null, default is used if value is null or empty.
	 * @throws SettingsException for bad value
	 */
	public static CommentIndexingMode parseConfiguration(String value) throws SettingsException {
		if (Utils.isEmpty(value)) {
			return EMBEDDED;
		}

		if (NONE.getConfigValue().equalsIgnoreCase(value)) {
			return NONE;
		} else if (CHILD.getConfigValue().equalsIgnoreCase(value)) {
			return CHILD;
		} else if (STANDALONE.getConfigValue().equalsIgnoreCase(value)) {
			return STANDALONE;
		} else if (EMBEDDED.getConfigValue().equalsIgnoreCase(value)) {
			return EMBEDDED;
		} else {
			throw new SettingsException("unsupported value for comments indexing mode: " + value);
		}
	}

}

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.content;

import org.jboss.elasticsearch.tools.content.StructuredContentPreprocessorFactory;

/**
 * Content preprocessor which allows to transform {@link Long} value from source field to the ISO formatted timestamp
 * value and store it to another or same target field. See documentation for
 * <code>org.jboss.elasticsearch.tools.content.LongToTimestampValuePreprocessor</code> as it is moved there now.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 * @see StructuredContentPreprocessorFactory
 * @deprecated as it is part of structured-content-tools now
 */
public class LongToTimestampValuePreprocessor extends
		org.jboss.elasticsearch.tools.content.LongToTimestampValuePreprocessor {
}

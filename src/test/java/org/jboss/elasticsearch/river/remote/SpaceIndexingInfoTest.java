/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.io.IOException;

import junit.framework.Assert;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.jboss.elasticsearch.river.remote.testtools.TestUtils;
import org.junit.Test;

/**
 * Unit test for {@link SpaceIndexingInfo}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceIndexingInfoTest {

	@Test
	public void buildDocument() throws Exception {

		TestUtils.assertStringFromClasspathFile("/asserts/SpaceIndexingInfoTest_1.json",
				new SpaceIndexingInfo("ORG", true, 10, 1, 1, DateTimeUtils.parseISODateTime("2012-09-10T12:55:58Z"), true,
						1250, null).buildDocument(XContentFactory.jsonBuilder(), true, true).string());

		TestUtils.assertStringFromClasspathFile("/asserts/SpaceIndexingInfoTest_2.json",
				new SpaceIndexingInfo("ORG", true, 10, 1, 1, DateTimeUtils.parseISODateTime("2012-09-10T12:56:50Z"), false,
						125, "Error message").buildDocument(XContentFactory.jsonBuilder(), true, true).string());

		TestUtils.assertStringFromClasspathFile("/asserts/SpaceIndexingInfoTest_3.json",
				new SpaceIndexingInfo("ORG", true, 10, 1, 1, DateTimeUtils.parseISODateTime("2012-09-10T12:55:58Z"), true,
						1250, null).buildDocument(XContentFactory.jsonBuilder(), false, true).string());

		TestUtils.assertStringFromClasspathFile("/asserts/SpaceIndexingInfoTest_4.json",
				new SpaceIndexingInfo("ORG", true, 10, 1, 1, DateTimeUtils.parseISODateTime("2012-09-10T12:55:58Z"), true,
						1250, null).buildDocument(XContentFactory.jsonBuilder(), false, false).string());
	}

	@Test
	public void readFromDocument() throws IOException {
		readFromDocumentInternalTest(new SpaceIndexingInfo("ORG", true, 10, 1, 1,
				DateTimeUtils.parseISODateTime("2012-09-10T12:55:58Z"), true, 1250, null));
		readFromDocumentInternalTest(new SpaceIndexingInfo("ORGA", false, 10, 0, 1,
				DateTimeUtils.parseISODateTime("2012-09-11T02:55:58Z"), false, 125, "Error"));
	}

	private void readFromDocumentInternalTest(SpaceIndexingInfo src) throws IOException {
		SpaceIndexingInfo result = SpaceIndexingInfo.readFromDocument(XContentFactory.xContent(XContentType.JSON)
				.createParser(src.buildDocument(XContentFactory.jsonBuilder(), true, true).string()).mapAndClose());

		Assert.assertEquals(src.spaceKey, result.spaceKey);
		Assert.assertEquals(src.fullUpdate, result.fullUpdate);
		Assert.assertEquals(src.documentsUpdated, result.documentsUpdated);
		Assert.assertEquals(src.documentsDeleted, result.documentsDeleted);
		// not stored and read for now!
		Assert.assertEquals(0, result.commentsDeleted);
		Assert.assertEquals(src.startDate, result.startDate);
		Assert.assertEquals(src.finishedOK, result.finishedOK);
		Assert.assertEquals(src.timeElapsed, result.timeElapsed);
		Assert.assertEquals(src.errorMessage, result.errorMessage);
	}

}

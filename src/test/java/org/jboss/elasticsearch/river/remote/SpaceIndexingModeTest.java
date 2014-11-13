/*
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import org.elasticsearch.common.settings.SettingsException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link SpaceIndexingMode}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class SpaceIndexingModeTest {

	@Test
	public void parseConfiguration() {
		Assert.assertEquals(SpaceIndexingMode.SIMPLE, SpaceIndexingMode.parseConfiguration("simple"));
		Assert.assertEquals(SpaceIndexingMode.SIMPLE, SpaceIndexingMode.parseConfiguration("SIMPLE"));
		Assert.assertEquals(SpaceIndexingMode.PAGINATION, SpaceIndexingMode.parseConfiguration("pagination"));
		Assert.assertEquals(SpaceIndexingMode.PAGINATION, SpaceIndexingMode.parseConfiguration("Pagination"));
		Assert.assertEquals(SpaceIndexingMode.UPDATE_TIMESTAMP, SpaceIndexingMode.parseConfiguration("updatetimestamp"));
		Assert.assertEquals(SpaceIndexingMode.UPDATE_TIMESTAMP, SpaceIndexingMode.parseConfiguration("UpdateTimestamp"));
		Assert.assertNull(SpaceIndexingMode.parseConfiguration(null));
		Assert.assertNull(SpaceIndexingMode.parseConfiguration("  "));

		try {
			SpaceIndexingMode.parseConfiguration("nonsense");
			Assert.fail("SettingsException must be thrown");
		} catch (SettingsException e) {
			// OK
		}
	}

	@Test
	public void isUpdateDateMandatory() {
		Assert.assertFalse(SpaceIndexingMode.SIMPLE.isUpdateDateMandatory());
		Assert.assertFalse(SpaceIndexingMode.PAGINATION.isUpdateDateMandatory());
		Assert.assertTrue(SpaceIndexingMode.UPDATE_TIMESTAMP.isUpdateDateMandatory());
	}

}

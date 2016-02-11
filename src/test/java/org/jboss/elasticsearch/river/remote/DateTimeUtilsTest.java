/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link DateTimeUtils}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class DateTimeUtilsTest {

	@Test
	public void formatISODateTime() {
		Assert.assertNull(DateTimeUtils.formatISODateTime(null));
		Assert.assertEquals("2012-08-14T12:00:00.0+0000",
				DateTimeUtils.formatISODateTime(DateTimeUtils.parseISODateTime("2012-08-14T08:00:00.000-0400")));
	}

	@Test
	public void parseISODateTime() {
		Assert.assertNull(DateTimeUtils.parseISODateTime(null));
		Assert.assertNull(DateTimeUtils.parseISODateTime(""));
		Assert.assertNull(DateTimeUtils.parseISODateTime("  "));
	}

	@Test
	public void parseISODateTimeWithMinutePrecise() {
		Assert.assertNull(DateTimeUtils.parseISODateTimeWithMinutePrecise(null));
		Assert.assertNull(DateTimeUtils.parseISODateTimeWithMinutePrecise(""));
		Assert.assertNull(DateTimeUtils.parseISODateTimeWithMinutePrecise("   "));
		try {
			Assert.assertNull(DateTimeUtils.parseISODateTimeWithMinutePrecise("nonsense date"));
			Assert.fail("IllegalArgumentException must be thrown");
		} catch (IllegalArgumentException e) {
			// OK
		}

		Date expected = DateTimeUtils.parseISODateTime("2012-08-14T08:00:00.000-0400");

		Assert.assertEquals(expected, DateTimeUtils.parseISODateTimeWithMinutePrecise("2012-08-14T08:00:00.000-0400"));
		Assert.assertEquals(expected, DateTimeUtils.parseISODateTimeWithMinutePrecise("2012-08-14T08:00:00.001-0400"));
		Assert.assertEquals(expected, DateTimeUtils.parseISODateTimeWithMinutePrecise("2012-08-14T08:00:10.000-0400"));
		Assert.assertEquals(expected, DateTimeUtils.parseISODateTimeWithMinutePrecise("2012-08-14T08:00:10.545-0400"));
		Assert.assertEquals(expected, DateTimeUtils.parseISODateTimeWithMinutePrecise("2012-08-14T08:00:59.999-0400"));
	}

	@Test
	public void roundDateTimeToMinutePrecise() {
		Assert.assertNull(DateTimeUtils.roundDateTimeToMinutePrecise(null));

		Date expected = DateTimeUtils.parseISODateTime("2012-08-14T08:00:00.000-0400");

		Assert.assertEquals(expected,
				DateTimeUtils.roundDateTimeToMinutePrecise(DateTimeUtils.parseISODateTime("2012-08-14T08:00:00.000-0400")));
		Assert.assertEquals(expected,
				DateTimeUtils.roundDateTimeToMinutePrecise(DateTimeUtils.parseISODateTime("2012-08-14T08:00:00.001-0400")));
		Assert.assertEquals(expected,
				DateTimeUtils.roundDateTimeToMinutePrecise(DateTimeUtils.parseISODateTime("2012-08-14T08:00:10.000-0400")));
		Assert.assertEquals(expected,
				DateTimeUtils.roundDateTimeToMinutePrecise(DateTimeUtils.parseISODateTime("2012-08-14T08:00:10.545-0400")));
		Assert.assertEquals(expected,
				DateTimeUtils.roundDateTimeToMinutePrecise(DateTimeUtils.parseISODateTime("2012-08-14T08:00:59.999-0400")));

	}
	
	@Test
	public void formatDateTime() {
	    
	    Assert.assertNull(DateTimeUtils.formatDateTime(null,""));
	    
	    Date exampleDate = DateTimeUtils.parseISODateTime("2012-08-14T08:00:00.000-0400");
	    
	    Assert.assertEquals(DateTimeUtils.formatDateTime(exampleDate, ""),DateTimeUtils.formatISODateTime(exampleDate));
	    
	    Assert.assertEquals(DateTimeUtils.formatDateTime(exampleDate, DateTimeUtils.CUSTOM_MILLISEC_EPOCH_DATETIME_FORMAT),
	            String.valueOf(exampleDate.getTime()));
	    Assert.assertEquals(DateTimeUtils.formatDateTime(exampleDate, DateTimeUtils.CUSTOM_UNIX_EPOCH_DATETIME_FORMAT),
                String.valueOf(exampleDate.getTime()).replaceFirst("\\d\\d\\d$", ""));
        
	    Assert.assertEquals(DateTimeUtils.formatDateTime(exampleDate, "YYYY-MM-dd"),"2012-08-14");
	    Assert.assertEquals(DateTimeUtils.formatDateTime(exampleDate, "YYYY-MM-dd"),"2012-08-14");
        
	    
	}
	
	@Test
	public void parseDate() {
		
		Assert.assertNull(DateTimeUtils.formatDateTime(null,""));
		
		Date exampleDate = DateTimeUtils.parseISODateTime("2012-08-14T00:00:00.000-0000");
		
		Assert.assertEquals( exampleDate , DateTimeUtils.parseDate("1344902400", DateTimeUtils.CUSTOM_UNIX_EPOCH_DATETIME_FORMAT) );
		Assert.assertEquals( exampleDate , DateTimeUtils.parseDate("1344902400000", DateTimeUtils.CUSTOM_MILLISEC_EPOCH_DATETIME_FORMAT) );
		Assert.assertTrue( exampleDate.equals(DateTimeUtils.parseDate("2012-08-14", "YYYY-MM-dd")) );
		
	}

}

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.elasticsearch.river.remote.ChangedDocumentsResults;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link ChangedDocumentsResults}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class ChangedDocumentsResultsTest {

  @Test
  public void constructorAndGetters() {
    try {
      new ChangedDocumentsResults(null, null, 1, 2);
      Assert.fail("IllegalArgumentException not thrown");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      new ChangedDocumentsResults(null, 1, null, 2);
      Assert.fail("IllegalArgumentException not thrown");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      new ChangedDocumentsResults(null, 1, 2, null);
      Assert.fail("IllegalArgumentException not thrown");
    } catch (IllegalArgumentException e) {
      // OK
    }

    ChangedDocumentsResults tested = new ChangedDocumentsResults(null, 1, 2, 3);
    Assert.assertEquals(1, tested.getStartAt());
    Assert.assertEquals(2, tested.getMaxResults());
    Assert.assertEquals(3, tested.getTotal());
    Assert.assertNull(tested.getDocuments());
    Assert.assertEquals(0, tested.getDocumentsCount());

    List<Map<String, Object>> issues = new ArrayList<Map<String, Object>>();
    tested = new ChangedDocumentsResults(issues, 1, 2, 3);
    Assert.assertEquals(1, tested.getStartAt());
    Assert.assertEquals(2, tested.getMaxResults());
    Assert.assertEquals(3, tested.getTotal());
    Assert.assertNotNull(tested.getDocuments());
    Assert.assertEquals(0, tested.getDocumentsCount());

    issues.add(new HashMap<String, Object>());
    tested = new ChangedDocumentsResults(issues, 10, 20, 300);
    Assert.assertEquals(10, tested.getStartAt());
    Assert.assertEquals(20, tested.getMaxResults());
    Assert.assertEquals(300, tested.getTotal());
    Assert.assertNotNull(tested.getDocuments());
    Assert.assertEquals(1, tested.getDocumentsCount());
  }

  @Test
  public void toStringTest() {
    ChangedDocumentsResults tested = new ChangedDocumentsResults(null, 1, 2, 3);
    Assert.assertNotNull(tested.toString());
  }
}

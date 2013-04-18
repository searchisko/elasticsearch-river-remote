/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import org.elasticsearch.common.settings.SettingsException;
import org.jboss.elasticsearch.river.remote.CommentIndexingMode;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link CommentIndexingMode}.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class CommentIndexingModeTest {

  @Test
  public void parseConfiguration() {
    Assert.assertEquals(CommentIndexingMode.NONE, CommentIndexingMode.parseConfiguration("none"));
    Assert.assertEquals(CommentIndexingMode.NONE, CommentIndexingMode.parseConfiguration("None"));
    Assert.assertEquals(CommentIndexingMode.CHILD, CommentIndexingMode.parseConfiguration("child"));
    Assert.assertEquals(CommentIndexingMode.CHILD, CommentIndexingMode.parseConfiguration("Child"));
    Assert.assertEquals(CommentIndexingMode.STANDALONE, CommentIndexingMode.parseConfiguration("standalone"));
    Assert.assertEquals(CommentIndexingMode.STANDALONE, CommentIndexingMode.parseConfiguration("Standalone"));
    Assert.assertEquals(CommentIndexingMode.EMBEDDED, CommentIndexingMode.parseConfiguration("embedded"));
    Assert.assertEquals(CommentIndexingMode.EMBEDDED, CommentIndexingMode.parseConfiguration("Embedded"));
    Assert.assertEquals(CommentIndexingMode.EMBEDDED, CommentIndexingMode.parseConfiguration(null));
    Assert.assertEquals(CommentIndexingMode.EMBEDDED, CommentIndexingMode.parseConfiguration("  "));

    try {
      CommentIndexingMode.parseConfiguration("nonsense");
      Assert.fail("SettingsException must be thrown");
    } catch (SettingsException e) {
      // OK
    }
  }

  @Test
  public void isExtraDocumentIndexed() {
    Assert.assertFalse(CommentIndexingMode.NONE.isExtraDocumentIndexed());
    Assert.assertFalse(CommentIndexingMode.EMBEDDED.isExtraDocumentIndexed());
    Assert.assertTrue(CommentIndexingMode.STANDALONE.isExtraDocumentIndexed());
    Assert.assertTrue(CommentIndexingMode.CHILD.isExtraDocumentIndexed());
  }

}

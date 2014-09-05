package org.jboss.elasticsearch.river.remote.testtools;

import junit.framework.Assert;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jboss.elasticsearch.river.remote.SpaceIndexingInfo;

public class ProjectInfoMatcher extends BaseMatcher<SpaceIndexingInfo> {

	String space;
	boolean fullUpdate;
	boolean finishedOK;
	int documentsUpdated;
	int documentsDeleted;
	String errorMessage;

	/**
	 * @param space
	 * @param fullUpdate
	 * @param finishedOK
	 * @param documentsUpdated
	 * @param documentsDeleted
	 * @param errorMessage
	 */
	public ProjectInfoMatcher(String space, boolean fullUpdate, boolean finishedOK, int documentsUpdated,
			int documentsDeleted, String errorMessage) {
		super();
		this.space = space;
		this.fullUpdate = fullUpdate;
		this.finishedOK = finishedOK;
		this.documentsUpdated = documentsUpdated;
		this.documentsDeleted = documentsDeleted;
		this.errorMessage = errorMessage;
	}

	@Override
	public boolean matches(Object arg0) {
		SpaceIndexingInfo info = (SpaceIndexingInfo) arg0;
		Assert.assertEquals(space, info.spaceKey);
		Assert.assertEquals(fullUpdate, info.fullUpdate);
		Assert.assertEquals(finishedOK, info.finishedOK);
		Assert.assertEquals(documentsUpdated, info.documentsUpdated);
		Assert.assertEquals(documentsDeleted, info.documentsDeleted);
		Assert.assertEquals(errorMessage, info.getErrorMessage());
		Assert.assertNotNull(info.startDate);
		return true;
	}

	@Override
	public void describeTo(Description arg0) {
	}

}
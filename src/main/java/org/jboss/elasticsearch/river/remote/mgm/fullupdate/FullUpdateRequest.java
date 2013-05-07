/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote.mgm.fullupdate;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jboss.elasticsearch.river.remote.Utils;
import org.jboss.elasticsearch.river.remote.mgm.JRMgmBaseRequest;

/**
 * Request for Full reindex.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class FullUpdateRequest extends JRMgmBaseRequest {

	/**
	 * Key of Space to request full reindex for. Null or Empty means full reindex for all Spaces.
	 */
	private String spaceKey;

	FullUpdateRequest() {

	}

	/**
	 * Construct request.
	 * 
	 * @param riverName for request
	 * @param spaceKey for request, optional
	 */
	public FullUpdateRequest(String riverName, String spaceKey) {
		super(riverName);
		this.spaceKey = spaceKey;
	}

	public String getSpaceKey() {
		return spaceKey;
	}

	public void setSpaceKey(String spaceKey) {
		this.spaceKey = spaceKey;
	}

	public boolean isSpaceKeyRequest() {
		return !Utils.isEmpty(spaceKey);
	}

	@Override
	public void readFrom(StreamInput in) throws IOException {
		super.readFrom(in);
		spaceKey = in.readOptionalString();
	}

	@Override
	public void writeTo(StreamOutput out) throws IOException {
		super.writeTo(out);
		out.writeOptionalString(spaceKey);
	}

	@Override
	public String toString() {
		return "FullUpdateRequest [spaceKey=" + spaceKey + ", riverName=" + riverName + "]";
	}

}

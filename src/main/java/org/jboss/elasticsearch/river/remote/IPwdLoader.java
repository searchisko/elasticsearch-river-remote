/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 */
package org.jboss.elasticsearch.river.remote;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Interface for password loader component.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public interface IPwdLoader {

	/**
	 * @param username to load password for
	 * @return password or null if not found
	 */
	public HashMap<String, String> loadKey(String username);

}

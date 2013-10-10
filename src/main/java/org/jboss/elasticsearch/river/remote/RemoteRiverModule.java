package org.jboss.elasticsearch.river.remote;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.river.River;

/**
 * Remote River ElasticSearch Module class.
 * 
 * @author Vlastimil Elias (velias at redhat dot com)
 */
public class RemoteRiverModule extends ActionModule {

	public RemoteRiverModule() {
		super(true);
	}

	@Override
	protected void configure() {
		bind(River.class).to(RemoteRiver.class).asEagerSingleton();
	}
}

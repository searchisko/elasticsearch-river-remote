package org.jboss.elasticsearch.river.remote;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.river.River;
import org.jboss.elasticsearch.river.remote.mgm.fullupdate.FullUpdateAction;
import org.jboss.elasticsearch.river.remote.mgm.fullupdate.TransportFullUpdateAction;
import org.jboss.elasticsearch.river.remote.mgm.lifecycle.JRLifecycleAction;
import org.jboss.elasticsearch.river.remote.mgm.lifecycle.TransportJRLifecycleAction;
import org.jboss.elasticsearch.river.remote.mgm.riverslist.ListRiversAction;
import org.jboss.elasticsearch.river.remote.mgm.riverslist.TransportListRiversAction;
import org.jboss.elasticsearch.river.remote.mgm.state.JRStateAction;
import org.jboss.elasticsearch.river.remote.mgm.state.TransportJRStateAction;

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

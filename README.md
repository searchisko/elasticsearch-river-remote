Universal remote system indexing River Plugin for ElasticSearch
===============================================================

The Universal remote system indexing River Plugin allows index documents from remotely accesible systems into [ElasticSearch](http://www.elasticsearch.org). It's implemented as ElasticSearch [river](http://www.elasticsearch.org/guide/reference/river/) [plugin](http://www.elasticsearch.org/guide/reference/modules/plugins.html) and uses remote APIs (REST with JSON or XML, SOAP etc.) to obtain documents from remote systems.

In order to install the plugin into ElasticSearch, simply run: `bin/plugin -url https://repository.jboss.org/nexus/content/groups/public-jboss/org/jboss/elasticsearch/elasticsearch-river-remote/1.0.0/elasticsearch-river-remote-1.0.0.zip -install elasticsearch-river-remote`.

    --------------------------------------------------
    | Remote River | ElasticSearch    | Release date |
    --------------------------------------------------
    | master       | 0.19.12          |              |
    --------------------------------------------------

For changelog, planned milestones/enhancements and known bugs see [github issue tracker](https://github.com/jbossorg/elasticsearch-river-remote/issues) please.

The river indexes documents with comments from remote system, and makes them searchable by ElasticSearch. Remote system is pooled periodically to detect changed documents to update search index in incremental update mode. 
Periodical full update may be configured too to completely refresh search index and remove documents deleted in remote system (deletes are not catched by incremental updates).

TODO structure of supported remote system API.

Creating the river can be done using:

	curl -XPUT localhost:9200/_river/my_remote_river/_meta -d '
	{
	    "type" : "remote",
	    "remote" : {
	        "urlGetDocuments"       : "https://system.org/rest/document?docSpace={space}&docUpdatedAfter={updatedAfter}",
	        "username"              : "remote_username",
          "pwd"                   : "remote_user_password",
	        "timeout"               : "5s",
	        "spacesIndexed"         : "ORG,AS7",
	        "spaceKeysExcluded"     : "",
	        "indexUpdatePeriod"     : "5m",
	        "indexFullUpdatePeriod" : "1h",
	        "maxIndexingThreads"    : 2,
	        "urlGetSpaces"          : "https://system.org/rest/space",
	        "getSpacesResField"     : "spaces"
	        "getDocsResFieldDocuments"  : "items"
	        "getDocsResFieldTotalcount" : "total"
	    },
	    "index" : {
	        "index" : "my_remote_index",
	        "type"  : "remote_document"
	    },
	    "activity_log": {
	        "index" : "remote_river_activity",
	        "type"  : "remote_river_indexupdate"
	    }
	}
	'

TODO detailed doc for river configuration, remote system API requirements, etc.

Management REST API
-------------------
Remote river supports next REST commands for management purposes. Note 
`my_remote_river` in examples is name of the remote river you can call operation
for, so replace it with real name for your calls.

Get [state info](/src/main/resources/examples/mgm/rest_river_info.json) about
the river operation:

	curl -XGET localhost:9200/_river/my_remote_river/_mgm/state

Stop remote river indexing process. Process is stopped permanently, so even
after complete elasticsearch cluster restart or river migration to another 
node. You need to `restart` it over management REST API (see next command):

	curl -XPOST localhost:9200/_river/my_remote_river/_mgm/stop

Restart remote river indexing process. Configuration of river is reloaded during 
restart. You can restart running indexing, or stopped indexing (see previous command):

	curl -XPOST localhost:9200/_river/my_remote_river/_mgm/restart

Force full index update for all document spaces:

	curl -XPOST localhost:9200/_river/my_remote_river/_mgm/fullupdate

Force full index update for document space with key `spaceKey`:

	curl -XPOST localhost:9200/_river/my_remote_river/_mgm/fullupdate/spaceKey

List names of all Remote Rivers running in ES cluster:

	curl -XGET localhost:9200/_remote_river/list


License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors as indicated by the @authors tag. 
    All rights reserved.

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.

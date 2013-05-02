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
	        "getDocsResFieldDocuments"  : "items"
	        "getDocsResFieldTotalcount" : "total"
	        "username"              : "remote_username",
          "pwd"                   : "remote_user_password",
	        "timeout"               : "5s",
	        "spacesIndexed"         : "ORG,AS7",
	        "spaceKeysExcluded"     : "",
	        "indexUpdatePeriod"     : "5m",
	        "indexFullUpdatePeriod" : "1h",
	        "maxIndexingThreads"    : 2,
	    },
	    "index" : {
	        "index" : "my_remote_index",
	        "type"  : "remote_document",
	        "remote_field_document_id" : "id",
	        "remote_field_updated" : "updated",
	    },
	    "activity_log": {
	        "index" : "remote_river_activity",
	        "type"  : "remote_river_indexupdate"
	    }
	}
	'

The example above lists all the main options controlling the creation and behavior of a Remote river. Full list of options with description is here:

* `remote/urlGetDocuments` is URL used to list documents from remote system.
* `remote/username` and `remote/pwd` are optional login credentials to access documents in remote system.
* `remote/timeout` time value, defines timeout for http/s REST request to the remote system. Optional, 5s is default if not provided.
* `remote/spacesIndexed` comma separated list of keys for remote system spaces to be indexed. Optional, list of spaces is obtained from remote system if ommited (so new spaces are indexed automatically).
* `remote/urlGetSpaces` is URL used to list spaces from remote system. Necessary if `remote/spacesIndexed`is not provided.
* `remote/spaceKeysExcluded` comma separated list of keys for remote system spaces to be excluded from indexing if list is obtained from remote system (so used only if no `jira/spacesIndexed` is defined). Optional.
* `remote/indexUpdatePeriod`  time value, defines how often is search index updated from remote system. Optional, default 5 minutes.
* `remote/indexFullUpdatePeriod` time value, defines how often is search index updated from remote system in full update mode. Optional, default 12 hours. You can use `0` to disable automatic full updates. Full update updates all documents in search index from remote system, and removes documents deleted in remote system from search index also. This brings more load to both remote system and ElasticSearch servers, and may run for long time in case of remote systems with many documents. Incremental updates are performed between full updates as defined by `indexUpdatePeriod` parameter.
* `remote/maxIndexingThreads` defines maximal number of parallel indexing threads running for this river. Optional, default 1. This setting influences load on both JIRA and ElasticSearch servers during indexing. Threads are started per JIRA project update. If there is more threads allowed, then one is always dedicated for incremental updates only (so full updates do not block incremental updates for another projects).
* `index/index` defines name of search [index](http://www.elasticsearch.org/guide/appendix/glossary.html#index) where documents from remote system are stored. Parameter is optional, name of river is used if omitted. See related notes later!
* `index/type` defines [type](http://www.elasticsearch.org/guide/appendix/glossary.html#type) used when document from remote system is stored into search index. Parameter is optional, `remote_document` is used if omitted. See related notes later!
* `index/field_river_name`, `index/field_space_key`, `index/field_document_id`, `index/fields`, `index/value_filters` can be used to change structure of indexed document. See 'Index document structure' chapter.
* `index/remote_field_document_id` is used to define field in remote system document data where unique document identifier is stored
* `index/remote_field_updated` is used to define field in remote system document data where timestamp of last update is stored - timestamp may be formatted by ISO format or number representing millis from 1.1.1970 
* `index/comment_mode` defines mode of issue comments indexing: `none` - no comments indexed, `embedded` - comments indexed as array in document, `child` - comment indexed as separate document with [parent-child relation](http://www.elasticsearch.org/guide/reference/mapping/parent-field.html) to the document, `standalone` - comment indexed as separate document. Setting is optional, `none` value is default if not provided.
* `index/comment_type` defines [type](http://www.elasticsearch.org/guide/appendix/glossary.html#type) used when issue comment is stored into search index in `child` or `standalone` mode. See related notes later!
* `index/field_comments`, `index/comment_fields` can be used to change structure comment informations in indexed documents. See 'index document structure' chapter.
* `index/remote_field_comments` is used to define field in remote system document data where array of comments is stored
* `index/remote_field_comment_id` is used to define field in remote system's comment data where unique comment identifier is stored. Used if `comment_mode` is  `child` or `standalone`
* `index/preprocessors` optional parameter. Defines chain of preprocessors applied to document data read from remote system before stored into index. See related notes later!
* `activity_log` part defines where information about remote river index update activity are stored. If omitted then no activity informations are stored.
* `activity_log/index` defines name of index where information about remote river activity are stored.
* `activity_log/type` defines [type](http://www.elasticsearch.org/guide/appendix/glossary.html#type) used to store information about remote river activity. Parameter is optional, `remote_river_indexupdate` is used if omitted.

Time value in configuration is number representing milliseconds, but you can use these postfixes appended to the number to define units: `s` for seconds, `m` for minutes, `h` for hours, `d` for days and `w` for weeks. So for example value `5h` means five fours, `2w` means two weeks.
 
To get rid of some unwanted WARN log messages add next line to the [logging configuration file](http://www.elasticsearch.org/guide/reference/setup/configuration.html) of your ElasticSearch instance which is `config/logging.yml`:

	org.apache.commons.httpclient: ERROR

And to get rid of extensive INFO messages from index update runs use:

	org.jboss.elasticsearch.river.remote.SpaceByLastUpdateTimestampIndexer: WARN


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

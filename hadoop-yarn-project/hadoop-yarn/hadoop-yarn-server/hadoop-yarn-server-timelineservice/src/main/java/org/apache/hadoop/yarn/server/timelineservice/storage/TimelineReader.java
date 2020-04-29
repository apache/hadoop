/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.hadoop.yarn.server.timelineservice.storage;

import java.io.IOException;

import java.util.Set;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;

/** ATSv2 reader interface. */
@Private
@Unstable
public interface TimelineReader extends Service {

  /**
   * Possible fields to retrieve for {@link #getEntities} and
   * {@link #getEntity}.
   */
  public enum Field {
    ALL,
    EVENTS,
    INFO,
    METRICS,
    CONFIGS,
    RELATES_TO,
    IS_RELATED_TO
  }

  /**
   * <p>The API to fetch the single entity given the identifier(depending on
   * the entity type) in the scope of the given context.</p>
   * @param context Context which defines the scope in which query has to be
   *    made. Use getters of {@link TimelineReaderContext} to fetch context
   *    fields. Context contains the following :<br>
   *    <ul>
   *    <li><b>entityType</b> - Entity type(mandatory).</li>
   *    <li><b>clusterId</b> - Identifies the cluster(mandatory).</li>
   *    <li><b>userId</b> - Identifies the user.</li>
   *    <li><b>flowName</b> - Context flow name.</li>
   *    <li><b>flowRunId</b> - Context flow run id.</li>
   *    <li><b>appId</b> - Context app id.</li>
   *    <li><b>entityId</b> - Entity id.</li>
   *    </ul>
   *    Fields in context which are mandatory depends on entity type. Entity
   *    type is always mandatory. In addition to entity type, below is the list
   *    of context fields which are mandatory, based on entity type.<br>
   *    <ul>
   *    <li>If entity type is YARN_FLOW_RUN (i.e. query to fetch a specific flow
   *    run), clusterId, userId, flowName and flowRunId are mandatory.</li>
   *    <li>If entity type is YARN_APPLICATION (i.e. query to fetch a specific
   *    app), query is within the scope of clusterId, userId, flowName,
   *    flowRunId and appId. But out of this, only clusterId and appId are
   *    mandatory. If only clusterId and appId are supplied, backend storage
   *    must fetch the flow context information i.e. userId, flowName and
   *    flowRunId first and based on that, fetch the app. If flow context
   *    information is also given, app can be directly fetched.
   *    </li>
   *    <li>For other entity types (i.e. query to fetch generic entity), query
   *    is within the scope of clusterId, userId, flowName, flowRunId, appId,
   *    entityType and entityId. But out of this, only clusterId, appId,
   *    entityType and entityId are mandatory. If flow context information is
   *    not supplied, backend storage must fetch the flow context information
   *    i.e. userId, flowName and flowRunId first and based on that, fetch the
   *    entity. If flow context information is also given, entity can be
   *    directly queried.
   *    </li>
   *    </ul>
   * @param dataToRetrieve Specifies which data to retrieve for the entity. Use
   *    getters of TimelineDataToRetrieve class to fetch dataToRetrieve
   *    fields. All the dataToRetrieve fields are optional. Refer to
   *    {@link TimelineDataToRetrieve} for details.
   * @return A <cite>TimelineEntity</cite> instance or null. The entity will
   *    contain the metadata plus the given fields to retrieve.<br>
   *    If entityType is YARN_FLOW_RUN, entity returned is of type
   *    <cite>FlowRunEntity</cite>.<br>
   *    For all other entity types, entity returned is of type
   *    <cite>TimelineEntity</cite>.
   * @throws IOException if there is an exception encountered while fetching
   *    entity from backend storage.
   */
  TimelineEntity getEntity(TimelineReaderContext context,
      TimelineDataToRetrieve dataToRetrieve) throws IOException;

  /**
   * <p>The API to search for a set of entities of the given entity type in
   * the scope of the given context which matches the given predicates. The
   * predicates include the created time window, limit to number of entities to
   * be returned, and the entities can be filtered by checking whether they
   * contain the given info/configs entries in the form of key/value pairs,
   * given metrics in the form of metricsIds and its relation with metric
   * values, given events in the form of the Ids, and whether they relate to/are
   * related to other entities. For those parameters which have multiple
   * entries, the qualified entity needs to meet all or them.</p>
   *
   * @param context Context which defines the scope in which query has to be
   *    made. Use getters of {@link TimelineReaderContext} to fetch context
   *    fields. Context contains the following :<br>
   *    <ul>
   *    <li><b>entityType</b> - Entity type(mandatory).</li>
   *    <li><b>clusterId</b> - Identifies the cluster(mandatory).</li>
   *    <li><b>userId</b> - Identifies the user.</li>
   *    <li><b>flowName</b> - Context flow name.</li>
   *    <li><b>flowRunId</b> - Context flow run id.</li>
   *    <li><b>appId</b> - Context app id.</li>
   *    </ul>
   *    Although entityIdPrefix and entityId are also part of context,
   *    it has no meaning for getEntities.<br>
   *    Fields in context which are mandatory depends on entity type. Entity
   *    type is always mandatory. In addition to entity type, below is the list
   *    of context fields which are mandatory, based on entity type.<br>
   *    <ul>
   *    <li>If entity type is YARN_FLOW_ACTIVITY (i.e. query to fetch flows),
   *    only clusterId is mandatory.
   *    </li>
   *    <li>If entity type is YARN_FLOW_RUN (i.e. query to fetch flow runs),
   *    clusterId, userId and flowName are mandatory.</li>
   *    <li>If entity type is YARN_APPLICATION (i.e. query to fetch apps), we
   *    can either get all apps within the context of flow name or within the
   *    context of flow run. If apps are queried within the scope of flow name,
   *    clusterId, userId and flowName are supplied. If they are queried within
   *    the scope of flow run, clusterId, userId, flowName and flowRunId are
   *    supplied.</li>
   *    <li>For other entity types (i.e. query to fetch generic entities), query
   *    is within the scope of clusterId, userId, flowName, flowRunId, appId and
   *    entityType. But out of this, only clusterId, appId and entityType are
   *    mandatory. If flow context information is not supplied, backend storage
   *    must fetch the flow context information i.e. userId, flowName and
   *    flowRunId first and based on that, fetch the entities. If flow context
   *    information is also given, entities can be directly queried.
   *    </li>
   *    </ul>
   * @param filters Specifies filters which restrict the number of entities
   *    to return. Use getters of TimelineEntityFilters class to fetch
   *    various filters. All the filters are optional. Refer to
   *    {@link TimelineEntityFilters} for details.
   * @param dataToRetrieve Specifies which data to retrieve for each entity. Use
   *    getters of TimelineDataToRetrieve class to fetch dataToRetrieve
   *    fields. All the dataToRetrieve fields are optional. Refer to
   *    {@link TimelineDataToRetrieve} for details.
   * @return A set of <cite>TimelineEntity</cite> instances of the given entity
   *    type in the given context scope which matches the given predicates
   *    ordered by enitityIdPrefix(for generic entities only).
   *    Each entity will only contain
   *    the metadata(id, type , idPrefix and created time) plus the given
   *    fields to retrieve.
   *    <br>
   *    If entityType is YARN_FLOW_ACTIVITY, entities returned are of type
   *    <cite>FlowActivityEntity</cite>.<br>
   *    If entityType is YARN_FLOW_RUN, entities returned are of type
   *    <cite>FlowRunEntity</cite>.<br>
   *    For all other entity types, entities returned are of type
   *    <cite>TimelineEntity</cite>.
   * @throws IOException if there is an exception encountered while fetching
   *    entity from backend storage.
   */
  Set<TimelineEntity> getEntities(
      TimelineReaderContext context,
      TimelineEntityFilters filters,
      TimelineDataToRetrieve dataToRetrieve) throws IOException;

  /**
   * The API to list all available entity types of the given context.
   *
   * @param context A context defines the scope of this query. The incoming
   * context should contain at least the cluster id and application id.
   *
   * @return A set of entity types available in the given context.
   *
   * @throws IOException if an exception occurred while listing from backend
   * storage.
   */
  Set<String> getEntityTypes(TimelineReaderContext context) throws IOException;

  /**
   * Check if reader connection is working properly.
   *
   * @return True if reader connection works as expected, false otherwise.
   */
  TimelineHealth getHealthStatus();
}
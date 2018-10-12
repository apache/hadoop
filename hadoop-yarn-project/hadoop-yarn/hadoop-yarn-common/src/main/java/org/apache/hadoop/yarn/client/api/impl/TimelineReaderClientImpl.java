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
package org.apache.hadoop.yarn.client.api.impl;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineReaderClient;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType.YARN_APPLICATION_ATTEMPT;
import static org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType.YARN_CONTAINER;
import static org.apache.hadoop.yarn.util.StringHelper.PATH_JOINER;

/**
 * Implementation of TimelineReaderClient interface.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TimelineReaderClientImpl extends TimelineReaderClient {
  private static final Log LOG =
      LogFactory.getLog(TimelineReaderClientImpl.class);

  private static final String RESOURCE_URI_STR_V2 = "/ws/v2/timeline/";

  private TimelineConnector connector;
  private URI baseUri;
  private String clusterId;

  public TimelineReaderClientImpl() {
    super(TimelineReaderClientImpl.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (!YarnConfiguration.timelineServiceV2Enabled(conf)) {
      throw new IOException("Timeline V2 client is not properly configured. "
          + "Either timeline service is not enabled or version is not set to"
          + " 2");
    }
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation realUgi = ugi.getRealUser();
    String doAsUser;
    UserGroupInformation authUgi;
    if (realUgi != null) {
      authUgi = realUgi;
      doAsUser = ugi.getShortUserName();
    } else {
      authUgi = ugi;
      doAsUser = null;
    }
    DelegationTokenAuthenticatedURL.Token token =
        new DelegationTokenAuthenticatedURL.Token();
    connector = new TimelineConnector(false, authUgi, doAsUser, token);
    addIfService(connector);
    String timelineReaderWebAppAddress =
        WebAppUtils.getTimelineReaderWebAppURLWithoutScheme(conf);
    baseUri = TimelineConnector.constructResURI(
        conf, timelineReaderWebAppAddress, RESOURCE_URI_STR_V2);
    clusterId = conf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
    super.serviceInit(conf);
  }

  @Override
  public TimelineEntity getApplicationEntity(ApplicationId appId, String fields,
      Map<String, String> filters)
      throws IOException {
    String path = PATH_JOINER.join("clusters", clusterId, "apps", appId);

    if (fields == null || fields.isEmpty()) {
      fields = "INFO";
    }
    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("fields", fields);
    mergeFilters(params, filters);

    ClientResponse response = doGetUri(baseUri, path, params);
    TimelineEntity entity = response.getEntity(TimelineEntity.class);
    return entity;
  }

  @Override
  public TimelineEntity getApplicationAttemptEntity(
      ApplicationAttemptId appAttemptId,
      String fields, Map<String, String> filters) throws IOException {
    ApplicationId appId = appAttemptId.getApplicationId();
    String path = PATH_JOINER.join("clusters", clusterId, "apps",
        appId, "entities", YARN_APPLICATION_ATTEMPT, appAttemptId);

    if (fields == null || fields.isEmpty()) {
      fields = "INFO";
    }
    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("fields", fields);
    mergeFilters(params, filters);

    ClientResponse response = doGetUri(baseUri, path, params);
    TimelineEntity entity = response.getEntity(TimelineEntity.class);
    return entity;
  }

  @Override
  public List<TimelineEntity> getApplicationAttemptEntities(
      ApplicationId appId, String fields, Map<String, String> filters,
      long limit, String fromId) throws IOException {
    String path = PATH_JOINER.join("clusters", clusterId, "apps",
        appId, "entities", YARN_APPLICATION_ATTEMPT);

    if (fields == null || fields.isEmpty()) {
      fields = "INFO";
    }
    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("fields", fields);
    if (limit > 0) {
      params.add("limit", Long.toString(limit));
    }
    if (fromId != null && !fromId.isEmpty()) {
      params.add("fromid", fromId);
    }
    mergeFilters(params, filters);

    ClientResponse response = doGetUri(baseUri, path, params);
    TimelineEntity[] entities = response.getEntity(TimelineEntity[].class);
    return Arrays.asList(entities);
  }

  @Override
  public TimelineEntity getContainerEntity(ContainerId containerId,
      String fields, Map<String, String> filters) throws IOException {
    ApplicationId appId = containerId.getApplicationAttemptId().
        getApplicationId();
    String path = PATH_JOINER.join("clusters", clusterId, "apps",
        appId, "entities", YARN_CONTAINER, containerId);

    if (fields == null || fields.isEmpty()) {
      fields = "INFO";
    }
    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("fields", fields);
    mergeFilters(params, filters);

    ClientResponse response = doGetUri(baseUri, path, params);
    TimelineEntity entity = response.getEntity(TimelineEntity.class);
    return entity;
  }

  @Override
  public List<TimelineEntity> getContainerEntities(
      ApplicationId appId, String fields,
      Map<String, String> filters,
      long limit, String fromId) throws IOException {
    String path = PATH_JOINER.join("clusters", clusterId, "apps",
        appId, "entities", YARN_CONTAINER);

    if (fields == null || fields.isEmpty()) {
      fields = "INFO";
    }
    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("fields", fields);
    if (limit > 0) {
      params.add("limit", Long.toString(limit));
    }
    if (fromId != null && !fromId.isEmpty()) {
      params.add("fromid", fromId);
    }
    mergeFilters(params, filters);

    ClientResponse response = doGetUri(baseUri, path, params);
    TimelineEntity[] entity = response.getEntity(TimelineEntity[].class);
    return Arrays.asList(entity);
  }

  private void mergeFilters(MultivaluedMap<String, String> defaults,
      Map<String, String> filters) {
    if (filters != null && !filters.isEmpty()) {
      for (Map.Entry<String, String> entry : filters.entrySet()) {
        if (!defaults.containsKey(entry.getKey())) {
          defaults.add(entry.getKey(), filters.get(entry.getValue()));
        }
      }
    }
  }

  @VisibleForTesting
  protected ClientResponse doGetUri(URI base, String path,
      MultivaluedMap<String, String> params) throws IOException {
    ClientResponse resp = connector.getClient().resource(base).path(path)
        .queryParams(params).accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    if (resp == null ||
        resp.getStatusInfo().getStatusCode() != ClientResponse.Status.OK
        .getStatusCode()) {
      String msg =
          "Response from the timeline reader server is " +
              ((resp == null) ? "null" : "not successful," +
                  " HTTP error code: " + resp.getStatus() +
                  ", Server response:\n" + resp.getEntity(String.class));
      LOG.error(msg);
      throw new IOException(msg);
    }
    return resp;
  }
}

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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

@Private
@Unstable
public class TimelineClientImpl extends TimelineClient {

  private static final Log LOG = LogFactory.getLog(TimelineClientImpl.class);
  private static final String RESOURCE_URI_STR = "/ws/v1/timeline/";
  private static final Joiner JOINER = Joiner.on("");

  private Client client;
  private URI resURI;
  private boolean isEnabled;

  public TimelineClientImpl() {
    super(TimelineClientImpl.class.getName());
    ClientConfig cc = new DefaultClientConfig();
    cc.getClasses().add(YarnJacksonJaxbJsonProvider.class);
    client = Client.create(cc);
  }

  protected void serviceInit(Configuration conf) throws Exception {
    isEnabled = conf.getBoolean(
        YarnConfiguration.TIMELINE_SERVICE_ENABLED,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED);
    if (!isEnabled) {
      LOG.info("Timeline service is not enabled");
    } else {
      if (YarnConfiguration.useHttps(conf)) {
        resURI = URI
            .create(JOINER.join("https://", conf.get(
                YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
                YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS),
                RESOURCE_URI_STR));
      } else {
        resURI = URI.create(JOINER.join("http://", conf.get(
            YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS),
            RESOURCE_URI_STR));
      }
      LOG.info("Timeline service address: " + resURI);
    }
    super.serviceInit(conf);
  }

  @Override
  public TimelinePutResponse putEntities(
      TimelineEntity... entities) throws IOException, YarnException {
    if (!isEnabled) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Nothing will be put because timeline service is not enabled");
      }
      return new TimelinePutResponse();
    }
    TimelineEntities entitiesContainer = new TimelineEntities();
    entitiesContainer.addEntities(Arrays.asList(entities));
    ClientResponse resp;
    try {
      resp = doPostingEntities(entitiesContainer);
    } catch (RuntimeException re) {
      // runtime exception is expected if the client cannot connect the server
      String msg =
          "Failed to get the response from the timeline server.";
      LOG.error(msg, re);
      throw re;
    }
    if (resp == null ||
        resp.getClientResponseStatus() != ClientResponse.Status.OK) {
      String msg =
          "Failed to get the response from the timeline server.";
      LOG.error(msg);
      if (LOG.isDebugEnabled() && resp != null) {
        String output = resp.getEntity(String.class);
        LOG.debug("HTTP error code: " + resp.getStatus()
            + " Server response : \n" + output);
      }
      throw new YarnException(msg);
    }
    return resp.getEntity(TimelinePutResponse.class);
  }

  @Private
  @VisibleForTesting
  public ClientResponse doPostingEntities(TimelineEntities entities) {
    WebResource webResource = client.resource(resURI);
    return webResource.accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON)
        .post(ClientResponse.class, entities);
  }

}

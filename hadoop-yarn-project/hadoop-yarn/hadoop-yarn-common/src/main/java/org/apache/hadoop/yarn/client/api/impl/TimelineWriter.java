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

import java.io.Flushable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.hadoop.yarn.api.records.writer.TimelineDomainWriter;
import org.apache.hadoop.yarn.api.records.writer.TimelineEntitiesWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Base writer class to write the Timeline data.
 */
@Private
@Unstable
public abstract class TimelineWriter implements Flushable {

  private static final Logger LOG = LoggerFactory
      .getLogger(TimelineWriter.class);

  private final UserGroupInformation authUgi;
  private final Client client;
  private final URI resURI;
  private final RetryPolicy<Object> retryPolicy;

  public TimelineWriter(UserGroupInformation authUgi, Client client,
      URI resURI, RetryPolicy<Object> retryPolicy) {
    this.authUgi = authUgi;
    this.client = client;
    this.resURI = resURI;
    this.retryPolicy = retryPolicy;
  }

  public void close() throws Exception {
    // DO NOTHING
  }

  @Override
  public void flush() throws IOException {
    // DO NOTHING
  }

  @Override
  public String toString() {
    return "Timeline writer posting to " + resURI;
  }

  public TimelinePutResponse putEntities(
      TimelineEntity... entities) throws IOException, YarnException {
    TimelineEntities entitiesContainer = new TimelineEntities();
    for (TimelineEntity entity : entities) {
      if (entity.getEntityId() == null || entity.getEntityType() == null) {
        throw new YarnException("Incomplete entity without entity id/type");
      }
      entitiesContainer.addEntity(entity);
    }
    Response resp = doPosting(entitiesContainer, null);
    return resp.readEntity(TimelinePutResponse.class);
  }

  public void putDomain(TimelineDomain domain) throws IOException,
      YarnException {
    doPosting(domain, "domain");
  }

  public abstract TimelinePutResponse putEntities(
      ApplicationAttemptId appAttemptId, TimelineEntityGroupId groupId,
      TimelineEntity... entities) throws IOException, YarnException;

  public abstract void putDomain(ApplicationAttemptId appAttemptId,
      TimelineDomain domain) throws IOException, YarnException;

  private Response doPosting(final Object obj, final String path)
      throws IOException, YarnException {
    Response resp;
    try {
      resp = authUgi.doAs((PrivilegedExceptionAction<Response>)
          () -> doPostingObject(obj, path));
    } catch (UndeclaredThrowableException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException)cause;
      } else {
        throw new IOException(cause);
      }
    } catch (InterruptedException ie) {
      throw (IOException)new InterruptedIOException().initCause(ie);
    }
    if (resp == null ||
        resp.getStatusInfo().getStatusCode()
            != Response.Status.OK.getStatusCode()) {
      String msg =
          "Failed to get the response from the timeline server.";
      LOG.error(msg);
      if (resp != null) {
        msg += " HTTP error code: " + resp.getStatus();
        LOG.debug("HTTP error code: {} Server response : \n{}",
            resp.getStatus(), resp.readEntity(String.class));
      }
      throw new YarnException(msg);
    }
    return resp;
  }

  @Private
  @VisibleForTesting
  public Response doPostingObject(Object object, String path) throws JsonProcessingException {
    final WebTarget webTarget = client
        .register(TimelineEntitiesWriter.class)
        .register(TimelineDomainWriter.class)
        .target(resURI);

    if (path == null) {
      LOG.debug("POST to {}", resURI);
      Response r = Failsafe.with(retryPolicy).get(
          () -> webTarget.request(MediaType.APPLICATION_JSON)
          .post(Entity.entity(object, MediaType.APPLICATION_JSON), Response.class));
      r.bufferEntity();
      return r;
    } else if (path.equals("domain")) {
      LOG.debug("PUT to {}/{}", resURI, path);
      Response r = Failsafe.with(retryPolicy).get(
          () -> webTarget.path(path).request(MediaType.APPLICATION_JSON)
          .put(Entity.entity(object, MediaType.APPLICATION_JSON), Response.class));
      r.bufferEntity();
      return r;
    } else {
      throw new YarnRuntimeException("Unknown resource type");
    }
  }
}

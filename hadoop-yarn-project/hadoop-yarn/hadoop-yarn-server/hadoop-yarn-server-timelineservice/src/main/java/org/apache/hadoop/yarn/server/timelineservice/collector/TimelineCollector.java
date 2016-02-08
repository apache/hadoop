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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;

/**
 * Service that handles writes to the timeline service and writes them to the
 * backing storage.
 *
 * Classes that extend this can add their own lifecycle management or
 * customization of request handling.
 */
@Private
@Unstable
public abstract class TimelineCollector extends CompositeService {
  private static final Log LOG = LogFactory.getLog(TimelineCollector.class);

  private TimelineWriter writer;

  public TimelineCollector(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  protected void setWriter(TimelineWriter w) {
    this.writer = w;
  }

  /**
   * Handles entity writes. These writes are synchronous and are written to the
   * backing storage without buffering/batching. If any entity already exists,
   * it results in an update of the entity.
   *
   * This method should be reserved for selected critical entities and events.
   * For normal voluminous writes one should use the async method
   * {@link #putEntitiesAsync(TimelineEntities, UserGroupInformation)}.
   *
   * @param entities entities to post
   * @param callerUgi the caller UGI
   * @return the response that contains the result of the post.
   * @throws IOException if there is any exception encountered while putting
   *     entities.
   */
  public TimelineWriteResponse putEntities(TimelineEntities entities,
      UserGroupInformation callerUgi) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("SUCCESS - TIMELINE V2 PROTOTYPE");
      LOG.debug("putEntities(entities=" + entities + ", callerUgi="
          + callerUgi + ")");
    }

    TimelineCollectorContext context = getTimelineEntityContext();
    return writer.write(context.getClusterId(), context.getUserId(),
        context.getFlowName(), context.getFlowVersion(), context.getFlowRunId(),
        context.getAppId(), entities);
  }

  /**
   * Handles entity writes in an asynchronous manner. The method returns as soon
   * as validation is done. No promises are made on how quickly it will be
   * written to the backing storage or if it will always be written to the
   * backing storage. Multiple writes to the same entities may be batched and
   * appropriate values updated and result in fewer writes to the backing
   * storage.
   *
   * @param entities entities to post
   * @param callerUgi the caller UGI
   */
  public void putEntitiesAsync(TimelineEntities entities,
      UserGroupInformation callerUgi) {
    // TODO implement
    if (LOG.isDebugEnabled()) {
      LOG.debug("putEntitiesAsync(entities=" + entities + ", callerUgi=" +
          callerUgi + ")");
    }
  }

  public abstract TimelineCollectorContext getTimelineEntityContext();

}

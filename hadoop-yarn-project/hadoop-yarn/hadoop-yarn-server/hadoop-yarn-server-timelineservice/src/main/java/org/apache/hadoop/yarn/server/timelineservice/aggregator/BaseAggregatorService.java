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

package org.apache.hadoop.yarn.server.timelineservice.aggregator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;

/**
 * Service that handles writes to the timeline service and writes them to the
 * backing storage.
 *
 * Classes that extend this can add their own lifecycle management or
 * customization of request handling.
 */
@Private
@Unstable
public class BaseAggregatorService extends CompositeService {
  private static final Log LOG = LogFactory.getLog(BaseAggregatorService.class);

  public BaseAggregatorService(String name) {
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

  /**
   * Handles entity writes. These writes are synchronous and are written to the
   * backing storage without buffering/batching. If any entity already exists,
   * it results in an update of the entity.
   *
   * This method should be reserved for selected critical entities and events.
   * For normal voluminous writes one should use the async method
   * {@link #postEntitiesAsync(TimelineEntities, UserGroupInformation)}.
   *
   * @param entities entities to post
   * @param callerUgi the caller UGI
   */
  public void postEntities(TimelineEntities entities,
      UserGroupInformation callerUgi) {
    // Add this output temporarily for our prototype
    // TODO remove this after we have an actual implementation
    LOG.info("SUCCESS - TIMELINE V2 PROTOTYPE");
    LOG.info("postEntities(entities=" + entities + ", callerUgi=" +
        callerUgi + ")");

    // TODO implement
    if (LOG.isDebugEnabled()) {
      LOG.debug("postEntities(entities=" + entities + ", callerUgi=" +
          callerUgi + ")");
    }
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
  public void postEntitiesAsync(TimelineEntities entities,
      UserGroupInformation callerUgi) {
    // TODO implement
    if (LOG.isDebugEnabled()) {
      LOG.debug("postEntitiesAsync(entities=" + entities + ", callerUgi=" +
          callerUgi + ")");
    }
  }
}
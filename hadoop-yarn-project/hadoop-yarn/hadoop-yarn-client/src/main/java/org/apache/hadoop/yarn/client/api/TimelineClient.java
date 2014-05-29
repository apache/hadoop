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

package org.apache.hadoop.yarn.client.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

/**
 * A client library that can be used to post some information in terms of a
 * number of conceptual entities.
 */
@Public
@Unstable
public abstract class TimelineClient extends AbstractService {

  @Public
  public static TimelineClient createTimelineClient() {
    TimelineClient client = new TimelineClientImpl();
    return client;
  }

  @Private
  protected TimelineClient(String name) {
    super(name);
  }

  /**
   * <p>
   * Send the information of a number of conceptual entities to the timeline
   * server. It is a blocking API. The method will not return until it gets the
   * response from the timeline server.
   * </p>
   * 
   * @param entities
   *          the collection of {@link TimelineEntity}
   * @return the error information if the sent entities are not correctly stored
   * @throws IOException
   * @throws YarnException
   */
  @Public
  public abstract TimelinePutResponse putEntities(
      TimelineEntity... entities) throws IOException, YarnException;

  /**
   * <p>
   * Get a delegation token so as to be able to talk to the timeline server in a
   * secure way.
   * </p>
   * 
   * @param renewer
   *          Address of the renewer who can renew these tokens when needed by
   *          securely talking to the timeline server
   * @return a delegation token ({@link Token}) that can be used to talk to the
   *         timeline server
   * @throws IOException
   * @throws YarnException
   */
  @Public
  public abstract Token<TimelineDelegationTokenIdentifier> getDelegationToken(
      String renewer) throws IOException, YarnException;

}

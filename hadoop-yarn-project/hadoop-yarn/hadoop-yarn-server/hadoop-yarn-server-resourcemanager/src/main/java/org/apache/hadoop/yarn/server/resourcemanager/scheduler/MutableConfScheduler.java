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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;

/**
 * Interface for a scheduler that supports changing configuration at runtime.
 *
 */
public interface MutableConfScheduler extends ResourceScheduler {

  /**
   * Update the scheduler's configuration.
   * @param user Caller of this update
   * @param confUpdate configuration update
   * @throws IOException if scheduler could not be reinitialized
   * @throws YarnException if reservation system could not be reinitialized
   */
  void updateConfiguration(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) throws IOException, YarnException;

  /**
   * Get the scheduler configuration.
   * @return the scheduler configuration
   */
  Configuration getConfiguration();

  /**
   * Get queue object based on queue name.
   * @param queueName the queue name
   * @return the queue object
   */
  Queue getQueue(String queueName);

  /**
   * Return whether the scheduler configuration is mutable.
   * @return whether scheduler configuration is mutable or not.
   */
  boolean isConfigurationMutable();
}

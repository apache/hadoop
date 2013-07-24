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

package org.apache.hadoop.mapred;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This interface is implemented by objects that are able to answer shuffle requests which are
 * sent from a matching Shuffle Consumer that lives in context of a ReduceTask object.
 *
 * ShuffleProviderPlugin object will be notified on the following events:
 * initialize, destroy.
 *
 * NOTE: This interface is also used when loading 3rd party plugins at runtime
 *
 */
@InterfaceAudience.LimitedPrivate("MapReduce")
@InterfaceStability.Unstable
public interface ShuffleProviderPlugin {
  /**
   * Do constructor work here.
   * This method is invoked by the TaskTracker Constructor
   */
  public void initialize(TaskTracker taskTracker);

  /**
   * close and cleanup any resource, including threads and disk space.
   * This method is invoked by TaskTracker.shutdown
   */
  public void destroy();
}

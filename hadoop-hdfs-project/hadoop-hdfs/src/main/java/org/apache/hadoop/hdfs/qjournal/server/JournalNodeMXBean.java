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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;

/**
 * This is the JMX management interface for JournalNode information
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface JournalNodeMXBean {
  
  /**
   * Get status information (e.g., whether formatted) of JournalNode's journals.
   * 
   * @return A string presenting status for each journal
   */
  String getJournalsStatus();

  /**
   * Get host and port of JournalNode.
   *
   * @return colon separated host and port.
   */
  String getHostAndPort();

  /**
   * Get list of the clusters of JournalNode's journals
   * as one JournalNode may support multiple clusters.
   *
   * @return list of clusters.
   */
  List<String> getClusterIds();

  /**
   * Gets the version of Hadoop.
   *
   * @return the version of Hadoop.
   */
  String getVersion();

  /**
   * Get the start time of the JournalNode.
   *
   * @return the start time of the JournalNode.
   */
  long getJNStartedTimeInMillis();

  /**
   * Get the list of the storage infos of JournalNode's journals. Storage infos
   * include layout version, namespace id, cluster id and creation time of the
   * File system state.
   *
   * @return the list of storage infos associated with journals.
   */
  List<String> getStorageInfos();
}

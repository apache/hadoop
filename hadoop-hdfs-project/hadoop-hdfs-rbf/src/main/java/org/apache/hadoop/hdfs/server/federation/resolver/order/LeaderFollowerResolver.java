/*
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
package org.apache.hadoop.hdfs.server.federation.resolver.order;

import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LEADER_FOLLOWER can be used in cross-cluster disaster tolerance,
 * and the order of namespaces is always "leader,follower,follower...".
 * Write data in leader sub-cluster as many as possible. If leader
 * sub-cluster failed, try followers then, the same goes for reading data.
 */
public class LeaderFollowerResolver implements OrderedResolver {
  protected static final Logger LOG =
      LoggerFactory.getLogger(LeaderFollowerResolver.class);

  @Override
  public String getFirstNamespace(String path, PathLocation loc) {
    try {
      // Always return first destination.
      // In leader/follower mode, admin add sub-clusters
      // by the order of leader,follower,follower...
      // The first element is always the leader sub-cluster,
      // so invoking getDefaultLocation is suitable here.
      RemoteLocation remoteLocation = loc.getDefaultLocation();
      return remoteLocation.getNameserviceId();
    } catch (Exception ex) {
      LOG.error("Cannot find sub-cluster for {}", loc);
      return null;
    }
  }
}

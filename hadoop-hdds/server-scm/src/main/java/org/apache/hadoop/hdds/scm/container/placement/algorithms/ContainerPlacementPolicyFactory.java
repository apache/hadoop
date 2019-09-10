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
package org.apache.hadoop.hdds.scm.container.placement.algorithms;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * A factory to create container placement instance based on configuration
 * property ozone.scm.container.placement.classname.
 */
public final class ContainerPlacementPolicyFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerPlacementPolicyFactory.class);

  private static final Class<? extends PlacementPolicy>
      OZONE_SCM_CONTAINER_PLACEMENT_IMPL_DEFAULT =
      SCMContainerPlacementRandom.class;

  private ContainerPlacementPolicyFactory() {
  }


  public static PlacementPolicy getPolicy(Configuration conf,
    final NodeManager nodeManager, NetworkTopology clusterMap,
    final boolean fallback, SCMContainerPlacementMetrics metrics)
    throws SCMException{
    final Class<? extends PlacementPolicy> placementClass = conf
        .getClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
            OZONE_SCM_CONTAINER_PLACEMENT_IMPL_DEFAULT,
            PlacementPolicy.class);
    Constructor<? extends PlacementPolicy> constructor;
    try {
      constructor = placementClass.getDeclaredConstructor(NodeManager.class,
          Configuration.class, NetworkTopology.class, boolean.class,
          SCMContainerPlacementMetrics.class);
      LOG.info("Create container placement policy of type " +
          placementClass.getCanonicalName());
    } catch (NoSuchMethodException e) {
      String msg = "Failed to find constructor(NodeManager, Configuration, " +
          "NetworkTopology, boolean) for class " +
          placementClass.getCanonicalName();
      LOG.error(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_INIT_CONTAINER_PLACEMENT_POLICY);
    }

    try {
      return constructor.newInstance(nodeManager, conf, clusterMap, fallback,
          metrics);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate class " +
          placementClass.getCanonicalName() + " for " + e.getMessage());
    }
  }
}

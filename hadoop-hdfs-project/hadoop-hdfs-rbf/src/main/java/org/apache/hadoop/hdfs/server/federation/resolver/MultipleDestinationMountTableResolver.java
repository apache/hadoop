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
package org.apache.hadoop.hdfs.server.federation.resolver;

import java.io.IOException;
import java.util.EnumMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.order.AvailableSpaceResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.resolver.order.HashFirstResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.HashResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.LocalResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.OrderedResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.RandomResolver;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Mount table resolver that supports multiple locations for each mount entry.
 * The returned location contains prioritized remote paths from highest priority
 * to the lowest priority. Multiple locations for a mount point are optional.
 * When multiple locations are specified, both will be checked for the presence
 * of a file and the nameservice for a new file/dir is chosen based on the
 * results of a consistent hashing algorithm.
 * <p>
 * Does the Mount table entry for this path have multiple destinations?
 * <ul>
 * <li>No -> Return the location
 * <li>Yes -> Return all locations, prioritizing the best guess from the
 * consistent hashing algorithm.
 * </ul>
 * <p>
 * It has multiple options to order the locations: HASH (default), LOCAL,
 * RANDOM, and HASH_ALL.
 * <p>
 * The consistent hashing result is dependent on the number and combination of
 * nameservices that are registered for particular mount point. The order of
 * nameservices/locations in the mount table is not prioritized. Each consistent
 * hash calculation considers only the set of unique nameservices present for
 * the mount table location.
 */
public class MultipleDestinationMountTableResolver extends MountTableResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(MultipleDestinationMountTableResolver.class);


  /** Resolvers that use a particular order for multiple destinations. */
  private EnumMap<DestinationOrder, OrderedResolver> orderedResolvers =
      new EnumMap<>(DestinationOrder.class);


  public MultipleDestinationMountTableResolver(
      Configuration conf, Router router) {
    super(conf, router);

    // Initialize the ordered resolvers
    addResolver(DestinationOrder.HASH, new HashFirstResolver());
    addResolver(DestinationOrder.LOCAL, new LocalResolver(conf, router));
    addResolver(DestinationOrder.RANDOM, new RandomResolver());
    addResolver(DestinationOrder.HASH_ALL, new HashResolver());
    addResolver(DestinationOrder.SPACE,
        new AvailableSpaceResolver(conf, router));
  }

  @Override
  public PathLocation getDestinationForPath(String path) throws IOException {
    PathLocation mountTableResult = super.getDestinationForPath(path);
    if (mountTableResult == null) {
      LOG.error("The {} cannot find a location for {}",
          super.getClass().getSimpleName(), path);
    } else if (mountTableResult.hasMultipleDestinations()) {
      DestinationOrder order = mountTableResult.getDestinationOrder();
      OrderedResolver orderedResolver = orderedResolvers.get(order);
      if (orderedResolver == null) {
        LOG.error("Cannot find resolver for order {}", order);
      } else {
        String firstNamespace =
            orderedResolver.getFirstNamespace(path, mountTableResult);

        // Change the order of the name spaces according to the policy
        if (firstNamespace != null) {
          // This is the entity in the tree, we need to create our own copy
          mountTableResult = new PathLocation(mountTableResult, firstNamespace);
          LOG.debug("Ordered locations following {} are {}",
              order, mountTableResult);
        } else {
          LOG.error("Cannot get main namespace for path {} with order {}",
              path, order);
        }
      }
    }
    return mountTableResult;
  }

  @VisibleForTesting
  public void addResolver(DestinationOrder order, OrderedResolver resolver) {
    orderedResolvers.put(order, resolver);
  }
}
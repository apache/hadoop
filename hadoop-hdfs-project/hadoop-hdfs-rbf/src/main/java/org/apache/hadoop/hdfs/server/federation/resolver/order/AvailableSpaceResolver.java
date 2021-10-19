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
package org.apache.hadoop.hdfs.server.federation.resolver.order;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.order.AvailableSpaceResolver.SubclusterAvailableSpace;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * Order the destinations based on available space. This resolver uses a
 * higher probability (instead of "always") to choose the cluster with higher
 * available space.
 */
public class AvailableSpaceResolver
    extends RouterResolver<String, SubclusterAvailableSpace> {

  private static final Logger LOG = LoggerFactory
      .getLogger(AvailableSpaceResolver.class);

  /** Increases chance of files on subcluster with more available space. */
  public static final String BALANCER_PREFERENCE_KEY =
      RBFConfigKeys.FEDERATION_ROUTER_PREFIX
      + "available-space-resolver.balanced-space-preference-fraction";
  public static final float BALANCER_PREFERENCE_DEFAULT = 0.6f;

  /** Random instance used in the subcluster comparison. */
  private static final Random RAND = new Random();

  /** Customized comparator for SubclusterAvailableSpace. */
  private SubclusterSpaceComparator comparator;

  public AvailableSpaceResolver(final Configuration conf,
      final Router routerService) {
    super(conf, routerService);
    float balancedPreference = conf.getFloat(BALANCER_PREFERENCE_KEY,
        BALANCER_PREFERENCE_DEFAULT);
    if (balancedPreference < 0.5) {
      LOG.warn("The balancer preference value is less than 0.5. That means more"
          + " files will be allocated in cluster with lower available space.");
    }

    this.comparator = new SubclusterSpaceComparator(balancedPreference);
  }

  /**
   * Get the mapping from NamespaceId to subcluster space info. It gets this
   * mapping from the subclusters through expensive calls (e.g., RPC) and uses
   * caching to avoid too many calls. The cache might be updated asynchronously
   * to reduce latency.
   *
   * @return NamespaceId to {@link SubclusterAvailableSpace}.
   */
  @Override
  protected Map<String, SubclusterAvailableSpace> getSubclusterInfo(
      MembershipStore membershipStore) {
    Map<String, SubclusterAvailableSpace> mapping = new HashMap<>();
    try {
      // Get the Namenode's available space info from the subclusters
      // from the Membership store.
      GetNamenodeRegistrationsRequest request = GetNamenodeRegistrationsRequest
          .newInstance();
      GetNamenodeRegistrationsResponse response = membershipStore
          .getNamenodeRegistrations(request);
      final List<MembershipState> nns = response.getNamenodeMemberships();
      for (MembershipState nn : nns) {
        try {
          String nsId = nn.getNameserviceId();
          long availableSpace = nn.getStats().getAvailableSpace();
          mapping.put(nsId, new SubclusterAvailableSpace(nsId, availableSpace));
        } catch (Exception e) {
          LOG.error("Cannot get stats info for {}: {}.", nn, e.getMessage());
        }
      }
    } catch (IOException ioe) {
      LOG.error("Cannot get Namenodes from the State Store.", ioe);
    }
    return mapping;
  }

  @Override
  protected String chooseFirstNamespace(String path, PathLocation loc) {
    Map<String, SubclusterAvailableSpace> subclusterInfo =
        getSubclusterMapping();
    List<SubclusterAvailableSpace> subclusterList = new LinkedList<>(
        subclusterInfo.values());
    Collections.sort(subclusterList, comparator);

    return subclusterList.size() > 0 ? subclusterList.get(0).getNameserviceId()
        : null;
  }

  /**
   * Inner class that stores cluster available space info.
   */
  static class SubclusterAvailableSpace {
    private final String nsId;
    private final long availableSpace;

    SubclusterAvailableSpace(String nsId, long availableSpace) {
      this.nsId = nsId;
      this.availableSpace = availableSpace;
    }

    public String getNameserviceId() {
      return this.nsId;
    }

    public long getAvailableSpace() {
      return this.availableSpace;
    }
  }

  /**
   * Customized comparator for SubclusterAvailableSpace. If more available
   * space the one cluster has, the higher priority it will have. But this
   * is not absolute, there is a balanced preference to make this use a higher
   * probability (instead of "always") to compare by this way.
   */
  static final class SubclusterSpaceComparator
      implements Comparator<SubclusterAvailableSpace>, Serializable {
    private int balancedPreference;

    SubclusterSpaceComparator(float balancedPreference) {
      Preconditions.checkArgument(
          balancedPreference <= 1 && balancedPreference >= 0,
          "The balancer preference value should be in the range 0.0 - 1.0");

      this.balancedPreference = (int) (100 * balancedPreference);
    }

    @Override
    public int compare(SubclusterAvailableSpace cluster1,
        SubclusterAvailableSpace cluster2) {
      int ret = cluster1.getAvailableSpace() > cluster2.getAvailableSpace() ? -1
          : 1;

      if (ret < 0) {
        return (RAND.nextInt(100) < balancedPreference) ? -1 : 1;
      } else {
        return (RAND.nextInt(100) < balancedPreference) ? 1 : -1;
      }
    }
  }
}

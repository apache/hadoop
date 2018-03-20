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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Order the destinations randomly.
 */
public class RandomResolver implements OrderedResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(RandomResolver.class);


  /** Random number generator. */
  private static final Random RANDOM = new Random();

  /**
   * Get a random name space from the path.
   *
   * @param path Path ignored by this policy.
   * @param loc Federated location with multiple destinations.
   * @return Random name space.
   */
  public String getFirstNamespace(final String path, final PathLocation loc) {
    if (loc == null) {
      return null;
    }
    Set<String> namespaces = loc.getNamespaces();
    if (namespaces == null || namespaces.isEmpty()) {
      LOG.error("Cannot get namespaces for {}", loc);
      return null;
    }
    List<String> nssList = new ArrayList<>(namespaces);
    int index = RANDOM.nextInt(nssList.size());
    return nssList.get(index);
  }
}
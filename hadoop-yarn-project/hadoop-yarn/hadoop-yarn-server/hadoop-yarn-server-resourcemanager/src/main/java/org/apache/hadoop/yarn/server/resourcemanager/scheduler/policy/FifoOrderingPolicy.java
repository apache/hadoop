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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * An OrderingPolicy which orders SchedulableEntities by input order
 */
public class FifoOrderingPolicy<S extends SchedulableEntity> extends AbstractComparatorOrderingPolicy<S> {
  
  public FifoOrderingPolicy() {
    List<Comparator<SchedulableEntity>> comparators =
        new ArrayList<Comparator<SchedulableEntity>>();
    comparators.add(new PriorityComparator());
    comparators.add(new FifoComparator());
    this.comparator = new CompoundComparator(comparators);
    this.schedulableEntities = new ConcurrentSkipListSet<S>(comparator);

  }
  
  @Override
  public void configure(Map<String, String> conf) {
    
  }
  
  @Override
  public void containerAllocated(S schedulableEntity, 
    RMContainer r) {
    }

  @Override
  public void containerReleased(S schedulableEntity, 
    RMContainer r) {
    }

  @Override
  public void demandUpdated(S schedulableEntity) {
  }

  @Override
  public String getInfo() {
    return "FifoOrderingPolicy";
  }
  
}

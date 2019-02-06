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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;


/**
 * An OrderingPolicy which can serve as a baseclass for policies which can be
 * expressed in terms of comparators
 */
public abstract class AbstractComparatorOrderingPolicy<S extends SchedulableEntity> implements OrderingPolicy<S> {
  
  private static final Log LOG = LogFactory.getLog(OrderingPolicy.class);
                                            
  protected ConcurrentSkipListSet<S> schedulableEntities;
  protected Comparator<SchedulableEntity> comparator;
  protected Map<String, S> entitiesToReorder = new HashMap<String, S>();
  
  public AbstractComparatorOrderingPolicy() { }
  
  @Override
  public Collection<S> getSchedulableEntities() {
    return schedulableEntities;
  }
  
  @Override
  public Iterator<S> getAssignmentIterator() {
    reorderScheduleEntities();
    return schedulableEntities.iterator();
  }
  
  @Override
  public Iterator<S> getPreemptionIterator() {
    reorderScheduleEntities();
    return schedulableEntities.descendingIterator();
  }
  
  public static void updateSchedulingResourceUsage(ResourceUsage ru) {
    ru.setCachedUsed(CommonNodeLabelsManager.ANY, ru.getAllUsed());
    ru.setCachedPending(CommonNodeLabelsManager.ANY, ru.getAllPending());
  }
  
  protected void reorderSchedulableEntity(S schedulableEntity) {
    //remove, update comparable data, and reinsert to update position in order
    schedulableEntities.remove(schedulableEntity);
    updateSchedulingResourceUsage(
      schedulableEntity.getSchedulingResourceUsage());
    schedulableEntities.add(schedulableEntity);
  }
  
  protected void reorderScheduleEntities() {
    synchronized (entitiesToReorder) {
      for (Map.Entry<String, S> entry :
          entitiesToReorder.entrySet()) {
        reorderSchedulableEntity(entry.getValue());
      }
      entitiesToReorder.clear();
    }
  }

  protected void entityRequiresReordering(S schedulableEntity) {
    synchronized (entitiesToReorder) {
      entitiesToReorder.put(schedulableEntity.getId(), schedulableEntity);
    }
  }

  public Comparator<SchedulableEntity> getComparator() {
    return comparator; 
  }
  
  @Override
  public void addSchedulableEntity(S s) {
    if (null == s) {
      return;
    }
    schedulableEntities.add(s); 
  }
  
  @Override
  public boolean removeSchedulableEntity(S s) {
    if (null == s) {
      return false;
    }
    synchronized (entitiesToReorder) {
      entitiesToReorder.remove(s.getId());
    }
    return schedulableEntities.remove(s); 
  }
  
  @Override
  public void addAllSchedulableEntities(Collection<S> sc) {
    schedulableEntities.addAll(sc);
  }
  
  @Override
  public int getNumSchedulableEntities() {
    return schedulableEntities.size(); 
  }
  
  @Override
  public abstract void configure(Map<String, String> conf);
  
  @Override
  public abstract void containerAllocated(S schedulableEntity, 
    RMContainer r);
  
  @Override
  public abstract void containerReleased(S schedulableEntity, 
    RMContainer r);
  
  @Override
  public abstract void demandUpdated(S schedulableEntity);

  @Override
  public abstract String getInfo();
  
}

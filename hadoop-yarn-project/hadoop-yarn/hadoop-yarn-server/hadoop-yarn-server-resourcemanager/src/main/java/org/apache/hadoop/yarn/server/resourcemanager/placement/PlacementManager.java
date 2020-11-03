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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

public class PlacementManager {  
  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementManager.class);

  List<PlacementRule> rules;
  ReadLock readLock;
  WriteLock writeLock;

  public PlacementManager() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public void updateRules(List<PlacementRule> rules) {
    writeLock.lock();
    try {
      this.rules = rules;
    } finally {
      writeLock.unlock();
    }
  }

  public ApplicationPlacementContext placeApplication(
      ApplicationSubmissionContext asc, String user) throws YarnException {
    readLock.lock();
    try {
      if (null == rules || rules.isEmpty()) {
        return null;
      }

      ApplicationPlacementContext placement = null;
      for (PlacementRule rule : rules) {
        placement = rule.getPlacementForApp(asc, user);
        if (placement != null) {
          break;
        }
      }

      return placement;
    } finally {
      readLock.unlock();
    }
  }
  
  @VisibleForTesting
  public List<PlacementRule> getPlacementRules() {
    return rules;
  }
}

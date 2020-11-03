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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class CSQueueStore {
  //This map is the single source of truth, this will store ALL queues
  //using the queue path as the key
  private final Map<String, CSQueue> fullNameQueues = new HashMap<>();

  //this map will contain all short names and the paths they can be derived from
  //this set is required for remove operation to properly set the short name
  //mapping when the ambiguity is resolved.
  private final Map<String, Set<String>> shortNameToLongNames = new HashMap<>();

  //This map will store the result to the get calls to prevent unnecessary
  //checks, this will be updated on queue add / remove
  private final Map<String, CSQueue> getMap = new HashMap<>();

  //this lock will be used to make sure isAmbiguous can be called parallel
  //it will be only blocked during add / remove operations.
  private ReadWriteLock modificationLock = new ReentrantReadWriteLock();

  /**
   * This getter method will return an immutable map with all the queues with
   * queue path as the key.
   * @return Map containing all queues and having path as key
   */
  Map<String, CSQueue> getFullNameQueues() {
    return ImmutableMap.copyOf(fullNameQueues);
  }

  /**
   * This getter method will return an immutable map with all queues
   * which can be disambiguously referenced by short name, using short name
   * as the key.
   * @return Map containing queues and having short name as key
   */
  @VisibleForTesting
  Map<String, CSQueue> getShortNameQueues() {
    //this is not the most efficient way to create a short named list
    //but this method is only used in tests
    try {
      modificationLock.readLock().lock();
      return ImmutableMap.copyOf(
          fullNameQueues
              //getting all queues from path->queue map
              .entrySet()
              .stream()
              //filtering the list to contain only disambiguous short names
              .filter(
                  //keeping queues where get(queueShortname) == queue
                  //these are the ambigous references
                  entry -> getMap.get(entry.getValue().getQueueShortName())
                      == entry.getValue())
              //making a map from the stream
              .collect(
                  Collectors.toMap(
                      //using the queue's short name as key
                      entry->entry.getValue().getQueueShortName(),
                      //using the queue as value
                      entry->entry.getValue()))
      );
    } finally {
      modificationLock.readLock().unlock();
    }
  }

  /**
   * This method will update the getMap for the short name provided, depending
   * on how many queues are present with the same shortname.
   * @param shortName The short name of the queue to be updated
   */
  private void updateGetMapForShortName(String shortName) {
    //we protect the root, since root can be both a full path and a short name
    //we simply deny adding root as a shortname to the getMap.
    if (shortName.equals(CapacitySchedulerConfiguration.ROOT)) {
      return;
    }
    //getting all queues with the same short name
    Set<String> fullNames = this.shortNameToLongNames.get(shortName);

    //if there is only one queue we add it to the getMap
    if (fullNames != null && fullNames.size() == 1) {
      getMap.put(shortName,
          fullNameQueues.get(fullNames.iterator().next()));
    } else {
      //in all other cases using only shortName cannot disambigously identifiy
      //a queue
      getMap.remove(shortName);
    }
  }

  /**
   * Method for adding a queue to the store.
   * @param queue Queue to be added
   */
  public void add(CSQueue queue) {
    String fullName = queue.getQueuePath();
    String shortName = queue.getQueueShortName();

    try {
      modificationLock.writeLock().lock();

      fullNameQueues.put(fullName, queue);
      getMap.put(fullName, queue);

      //we only update short queue name ambiguity for non root queues
      if (!shortName.equals(CapacitySchedulerConfiguration.ROOT)) {
        //getting or creating the ambiguity set for the current queue
        Set<String> fullNamesSet =
            this.shortNameToLongNames.getOrDefault(shortName, new HashSet<>());

        //adding the full name to the queue
        fullNamesSet.add(fullName);
        this.shortNameToLongNames.put(shortName, fullNamesSet);
      }

      //updating the getMap references for the queue
      updateGetMapForShortName(shortName);
    } finally {
      modificationLock.writeLock().unlock();
    }
  }

  /**
   * Method for removing a queue from the store.
   * @param queue The queue to be removed
   */
  public void remove(CSQueue queue) {
    //if no queue is specified, we can consider it already removed,
    //also consistent with hashmap behaviour
    if (queue == null) {
      return;
    }
    try {
      modificationLock.writeLock().lock();

      String fullName = queue.getQueuePath();
      String shortName = queue.getQueueShortName();

      fullNameQueues.remove(fullName);
      getMap.remove(fullName);

      //we only update short queue name ambiguity for non root queues
      if (!shortName.equals(CapacitySchedulerConfiguration.ROOT)) {
        Set<String> fullNamesSet = this.shortNameToLongNames.get(shortName);
        fullNamesSet.remove(fullName);
        //if there are no more queues with the current short name, we simply
        //remove the set to free up some memory
        if (fullNamesSet.size() == 0) {
          this.shortNameToLongNames.remove(shortName);
        }
      }

      //updating the getMap references for the queue
      updateGetMapForShortName(shortName);

    } finally {
      modificationLock.writeLock().unlock();
    }
  }

  /**
   * Method for removing a queue from the store by name.
   * @param name A deterministic name for the queue to be removed
   */
  public void remove(String name) {
    CSQueue queue = get(name);
    if (queue != null) {
      remove(queue);
    }
  }

  /**
   * Returns a queue by looking it up by its fully qualified name.
   * @param fullName The full name/path of the queue
   * @return The queue or null if none found
   */
  CSQueue getByFullName(String fullName) {
    if (fullName == null) {
      return null;
    }

    try {
      modificationLock.readLock().lock();
      return fullNameQueues.getOrDefault(fullName, null);
    } finally {
      modificationLock.readLock().unlock();
    }
  }

  /**
   * Check for name ambiguity returns true, if there are at least two queues
   * with the same short name. Queue named "root" is protected, and it will
   * always return the root queue regardless of ambiguity.
   * @param shortName The short name to be checked for ambiguity
   * @return true if there are at least two queues found false otherwise
   */
  boolean isAmbiguous(String shortName) {
    if (shortName == null) {
      return false;
    }

    boolean ret = true;
    try {
      modificationLock.readLock().lock();
      Set<String> fullNamesSet = this.shortNameToLongNames.get(shortName);

      if (fullNamesSet == null || fullNamesSet.size() <= 1) {
        ret = false;
      }
    } finally {
      modificationLock.readLock().unlock();
    }

    return ret;
  }

  /**
   * Getter method for the queue it can find queues by both full and
   * short names.
   * @param name Full or short name of the queue
   * @return the queue
   */
  public CSQueue get(String name) {
    if (name == null) {
      return null;
    }
    try {
      modificationLock.readLock().lock();
      return getMap.getOrDefault(name, null);
    } finally {
      modificationLock.readLock().unlock();
    }
  }

  /**
   * Clears the store, removes all queue references.
   */
  public void clear() {
    try {
      modificationLock.writeLock().lock();
      fullNameQueues.clear();
      shortNameToLongNames.clear();
      getMap.clear();
    } finally {
      modificationLock.writeLock().unlock();
    }
  }

  /**
   * Returns all queues as a list.
   * @return List containing all the queues
   */
  public Collection<CSQueue> getQueues() {
    try {
      modificationLock.readLock().lock();
      return ImmutableList.copyOf(fullNameQueues.values());
    } finally {
      modificationLock.readLock().unlock();
    }
  }
}

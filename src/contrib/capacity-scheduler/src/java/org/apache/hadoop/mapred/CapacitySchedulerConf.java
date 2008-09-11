/** Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Class providing access to resource manager configuration.
 * 
 * Resource manager configuration involves setting up queues, and defining
 * various properties for the queues. These are typically read from a file 
 * called resource-manager-conf.xml that must be in the classpath of the
 * application. The class provides APIs to get/set and reload the 
 * configuration for the queues.
 */
class CapacitySchedulerConf {
  
  /** Default file name from which the resource manager configuration is read. */ 
  public static final String SCHEDULER_CONF_FILE = "capacity-scheduler.xml";

  /** Default value for guaranteed capacity of maps (as percentage).
   * The default value is set to 100, to represent the entire queue. 
   */ 
  public static final float DEFAULT_GUARANTEED_CAPACITY = 100;

  /** Default value for reclaiming redistributed resources.
   * The default value is set to <code>300</code>. 
   */ 
  public static final int DEFAULT_RECLAIM_TIME_LIMIT = 300;
  
  /** Default value for minimum resource limit per user per queue, as a 
   * percentage.
   * The default value is set to <code>100</code>, the idea
   * being that the default is suitable for organizations that do not
   * require setting up any queues.
   */ 
  public static final int DEFAULT_MIN_USER_LIMIT_PERCENT = 100;

  private static final String QUEUE_CONF_PROPERTY_NAME_PREFIX = 
    "mapred.capacity-scheduler.queue.";

  private Configuration rmConf;
  
  /**
   * Create a new ResourceManagerConf.
   * This method reads from the default configuration file mentioned in
   * {@link RM_CONF_FILE}, that must be present in the classpath of the
   * application.
   */
  public CapacitySchedulerConf() {
    rmConf = new Configuration(false);
    rmConf.addResource(SCHEDULER_CONF_FILE);
  }

  /**
   * Create a new ResourceManagerConf reading the specified configuration
   * file.
   * 
   * @param configFile {@link Path} to the configuration file containing
   * the resource manager configuration.
   */
  public CapacitySchedulerConf(Path configFile) {
    rmConf = new Configuration(false);
    rmConf.addResource(configFile);
  }
  
  /**
   * Get the guaranteed percentage of the cluster for the specified queue.
   * 
   * This method defaults to {@link #DEFAULT_GUARANTEED_CAPACITY} if
   * no value is specified in the configuration for this queue. If the queue
   * name is unknown, this method throws a {@link IllegalArgumentException}
   * @param queue name of the queue
   * @return guaranteed percent of the cluster for the queue.
   */
  public float getGuaranteedCapacity(String queue) {
    checkQueue(queue);
    float result = rmConf.getFloat(toFullPropertyName(queue, 
                                   "guaranteed-capacity"), 
                                   DEFAULT_GUARANTEED_CAPACITY);
    if (result < 0.0 || result > 100.0) {
      throw new IllegalArgumentException("Illegal capacity for queue " + queue +
                                         " of " + result);
    }
    return result;
  }
  
  /**
   * Get the amount of time before which redistributed resources must be
   * reclaimed for the specified queue.
   * 
   * The resource manager distributes spare capacity from a free queue
   * to ones which are in need for more resources. However, if a job 
   * submitted to the first queue requires back the resources, they must
   * be reclaimed within the specified configuration time limit.
   * 
   * This method defaults to {@link #DEFAULT_RECLAIM_TIME_LIMIT} if
   * no value is specified in the configuration for this queue. If the queue
   * name is unknown, this method throws a {@link IllegalArgumentException}
   * @param queue name of the queue
   * @return reclaim time limit for this queue.
   */
  public int getReclaimTimeLimit(String queue) {
    checkQueue(queue);
    return rmConf.getInt(toFullPropertyName(queue, "reclaim-time-limit"), 
                          DEFAULT_RECLAIM_TIME_LIMIT);
  }
  
  /**
   * Set the amount of time before which redistributed resources must be
   * reclaimed for the specified queue.
   * @param queue Name of the queue
   * @param value Amount of time before which the redistributed resources
   * must be retained.
   */
  public void setReclaimTimeLimit(String queue, int value) {
    checkQueue(queue);
    rmConf.setInt(toFullPropertyName(queue, "reclaim-time-limit"), value);
  }
  
  /**
   * Get whether priority is supported for this queue.
   * 
   * If this value is false, then job priorities will be ignored in 
   * scheduling decisions. This method defaults to <code>false</code> if 
   * the property is not configured for this queue. If the queue name is 
   * unknown, this method throws a {@link IllegalArgumentException}
   * @param queue name of the queue
   * @return Whether this queue supports priority or not.
   */
  public boolean isPrioritySupported(String queue) {
    checkQueue(queue);
    return rmConf.getBoolean(toFullPropertyName(queue, "supports-priority"),
                              false);  
  }
  
  /**
   * Set whether priority is supported for this queue.
   * 
   * If the queue name is unknown, this method throws a 
   * {@link IllegalArgumentException}
   * @param queue name of the queue
   * @param value true, if the queue must support priorities, false otherwise.
   */
  public void setPrioritySupported(String queue, boolean value) {
    checkQueue(queue);
    rmConf.setBoolean(toFullPropertyName(queue, "supports-priority"), value);
  }
  
  /**
   * Get the minimum limit of resources for any user submitting jobs in 
   * this queue, in percentage.
   * 
   * This method defaults to {@link #DEFAULT_MIN_USER_LIMIT_PERCENT} if
   * no value is specified in the configuration for this queue. If the queue
   * name is unknown, this method throws a {@link IllegalArgumentException}
   * @param queue name of the queue
   * @return minimum limit of resources, in percentage, that will be 
   * available for a user.
   * 
   */
  public int getMinimumUserLimitPercent(String queue) {
    checkQueue(queue);
    return rmConf.getInt(toFullPropertyName(queue, 
                          "minimum-user-limit-percent"), 
                          DEFAULT_MIN_USER_LIMIT_PERCENT);
  }
  
  /**
   * Set the minimum limit of resources for any user submitting jobs in
   * this queue, in percentage.
   * 
   * If the queue name is unknown, this method throws a 
   * {@link IllegalArgumentException}
   * @param queue name of the queue
   * @param value minimum limit of resources for any user submitting jobs
   * in this queue
   */
  public void setMinimumUserLimitPercent(String queue, int value) {
    checkQueue(queue);
    rmConf.setInt(toFullPropertyName(queue, "minimum-user-limit-percent"), 
                    value);
  }
  
  /**
   * Reload configuration by clearing the information read from the 
   * underlying configuration file.
   */
  public synchronized void reloadConfiguration() {
    rmConf.reloadConfiguration();
  }
  
  private synchronized void checkQueue(String queue) {
    /*if (queues == null) {
      queues = getQueues();
    }
    if (!queues.contains(queue)) {
      throw new IllegalArgumentException("Queue " + queue + " is undefined.");
    }*/
  }

  private static final String toFullPropertyName(String queue, 
                                                  String property) {
      return QUEUE_CONF_PROPERTY_NAME_PREFIX + queue + "." + property;
  }
}

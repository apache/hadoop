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

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

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
class ResourceManagerConf {
  
  /** Default file name from which the resource manager configuration is read. */ 
  public static final String RM_CONF_FILE = "resource-manager-conf.xml";

  /** Default value for guaranteed capacity of maps.
   * The default value is set to <code>Integer.MAX_VALUE</code>, the idea
   * being that the default is suitable for organizations that do not
   * require setting up any queues. 
   */ 
  public static final int DEFAULT_GUARANTEED_CAPACITY_MAPS = 
                                                    Integer.MAX_VALUE;

  /** Default value for guaranteed capacity of reduces.
   * The default value is set to <code>Integer.MAX_VALUE</code>, the idea
   * being that the default is suitable for organizations that do not
   * require setting up any queues. 
   */ 
  public static final int DEFAULT_GUARANTEED_CAPACITY_REDUCES = 
                                                    Integer.MAX_VALUE;

  /** Default value for reclaiming redistributed resources.
   * The default value is set to <code>0</code>, the idea
   * being that the default is suitable for organizations that do not
   * require setting up any queues.
   */ 
  public static final int DEFAULT_RECLAIM_TIME_LIMIT = 0;
  
  /** Default value for minimum resource limit per user per queue, as a 
   * percentage.
   * The default value is set to <code>100</code>, the idea
   * being that the default is suitable for organizations that do not
   * require setting up any queues.
   */ 
  public static final int DEFAULT_MIN_USER_LIMIT_PERCENT = 100;

  private static final String QUEUE_CONF_PROPERTY_NAME_PREFIX 
                                              = "hadoop.rm.queue.";

  private Configuration rmConf;
  private Set<String> queues;
  
  /**
   * Create a new ResourceManagerConf.
   * This method reads from the default configuration file mentioned in
   * {@link RM_CONF_FILE}, that must be present in the classpath of the
   * application.
   */
  public ResourceManagerConf() {
    rmConf = new Configuration(false);
    rmConf.addResource(RM_CONF_FILE);
  }

  /**
   * Create a new ResourceManagerConf reading the specified configuration
   * file.
   * 
   * @param configFile {@link Path} to the configuration file containing
   * the resource manager configuration.
   */
  public ResourceManagerConf(Path configFile) {
    rmConf = new Configuration(false);
    rmConf.addResource(configFile);
  }
  
  /**
   * Return the set of configured queue names.
   * @return set of configured queue names.
   */
  public synchronized Set<String> getQueues() {
    if (queues == null) {
      String[] vals = rmConf.getStrings("hadoop.rm.queue.names");
      queues = new TreeSet<String>();
      for (String val : vals) {
        queues.add(val);
      }
    }
    return queues;
  }
  
  /**
   * Define the set of queues known to this configuration.
   * This will override the queue names that are read from the
   * configuration file. 
   * @param newQueues Set of queue names
   */
  public synchronized void setQueues(Set<String> newQueues) {
    StringBuffer queueSb = new StringBuffer();
    for (String queue : newQueues) {
      queueSb.append(queue).append(',');
    }
    if (newQueues.size() > 0) {
      String value = queueSb.substring(0, queueSb.length());
      rmConf.set("hadoop.rm.queue.names", value);
      queues = null;
    }
  }
  
  /**
   * Get the guaranteed number of maps for the specified queue.
   * 
   * This method defaults to {@link #DEFAULT_GUARANTEED_CAPACITY_MAPS} if
   * no value is specified in the configuration for this queue. If the queue
   * name is unknown, this method throws a {@link IllegalArgumentException}
   * @param queue name of the queue
   * @return guaranteed number of maps for this queue.
   */
  public int getGuaranteedCapacityMaps(String queue) {
    checkQueue(queue);
    return rmConf.getInt(toFullPropertyName(queue, 
                          "guaranteed-capacity-maps"), 
                          DEFAULT_GUARANTEED_CAPACITY_MAPS);
  }

  /**
   * Set the guaranteed number of maps for the specified queue
   * @param queue Name of the queue
   * @param value Guaranteed number of maps.
   */
  public void setGuaranteedCapacityMaps(String queue, int value) {
    checkQueue(queue);
    rmConf.setInt(toFullPropertyName(queue,"guaranteed-capacity-maps"), 
                    value);
  }
  
  /**
   * Get the guaranteed number of reduces for the specified queue.
   * 
   * This method defaults to {@link #DEFAULT_GUARANTEED_CAPACITY_REDUCES} if
   * no value is specified in the configuration for this queue. If the queue
   * name is unknown, this method throws a {@link IllegalArgumentException}
   * @param queue name of the queue
   * @return guaranteed number of reduces for this queue.
   */
  public int getGuaranteedCapacityReduces(String queue) {
    checkQueue(queue);
    return rmConf.getInt(toFullPropertyName(queue,
                          "guaranteed-capacity-reduces"), 
                          DEFAULT_GUARANTEED_CAPACITY_REDUCES);
  }

  /**
   * Set the guaranteed number of reduces for the specified queue
   * @param queue Name of the queue
   * @param value Guaranteed number of reduces.
   */
  public void setGuaranteedCapacityReduces(String queue, int value) {
    checkQueue(queue);
    rmConf.setInt(toFullPropertyName(queue,"guaranteed-capacity-reduces"), 
                    value);
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
   * Get the list of users who can submit jobs to this queue.
   * 
   * This method defaults to <code>null</code> if no value is specified 
   * in the configuration for this queue. If the queue
   * name is unknown, this method throws a {@link IllegalArgumentException}
   * @param queue name of the queue
   * @return list of users who can submit jobs to this queue.
   */
  public String[] getAllowedUsers(String queue) {
    checkQueue(queue);
    return rmConf.getStrings(toFullPropertyName(queue, "allowed-users"), 
                              (String[])null);
  }
  
  /**
   * Set the list of users who can submit jobs to this queue.
   * 
   * If the queue name is unknown, this method throws a 
   * {@link IllegalArgumentException}
   * @param queue name of the queue
   * @param values list of users allowed to submit jobs to this queue.
   */
  public void setAllowedUsers(String queue, String... values) {
    checkQueue(queue);
    rmConf.setStrings(toFullPropertyName(queue, "allowed-users"), values);
  }
  
  /**
   * Get the list of users who cannot submit jobs to this queue.
   * 
   * This method defaults to <code>null</code> if no value is specified 
   * in the configuration for this queue. If the queue
   * name is unknown, this method throws a {@link IllegalArgumentException}
   * @param queue name of the queue
   * @return list of users who cannot submit jobs to this queue.
   */
  public String[] getDeniedUsers(String queue) {
    checkQueue(queue);
    return rmConf.getStrings(toFullPropertyName(queue, "denied-users"), 
                              (String[])null);
  }

  /**
   * Set the list of users who cannot submit jobs to this queue.
   * 
   * If the queue name is unknown, this method throws a 
   * {@link IllegalArgumentException}
   * @param queue name of the queue
   * @param values list of users denied to submit jobs to this queue.
   */
  public void setDeniedUsers(String queue, String... values) {
    checkQueue(queue);
    rmConf.setStrings(toFullPropertyName(queue, "denied-users"), values);
  }

  /**
   * Get whether users present in allowed users list will override values
   * in the denied user list for this queue.
   * 
   * If a user name is specified in both the allowed user list and the denied
   * user list, this configuration determines whether to honor the allowed
   * user list or the denied user list. This method defaults to 
   * <code>false</code> if no value is specified in the configuration for 
   * this queue. If the queue name is unknown, this method throws a 
   * {@link IllegalArgumentException}
   * @param queue name of the queue
   * @return 
   */
  public boolean doAllowedUsersOverride(String queue) {
    checkQueue(queue);
    return rmConf.getBoolean(toFullPropertyName(queue, 
                              "allowed-users-override"), false);
  }

  /**
   * Set whether users present in allowed users list will override values
   * in the denied user list for this queue.
   * 
   * If the queue name is unknown, this method throws a 
   * {@link IllegalArgumentException}
   * @param queue name of the queue
   * @param value true if the allowed users take priority, false otherwise.
   */
  public void setAllowedUsersOverride(String queue, boolean value) {
    checkQueue(queue);
    rmConf.setBoolean(toFullPropertyName(queue, "allowed-users-override"),
                        value);
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
    queues = null;
    rmConf.reloadConfiguration();
  }
  
  /**
   * Print configuration read from the underlying configuration file.
   * 
   * @param writer {@link PrintWriter} to which the output must be written.
   */
  public void printConfiguration(PrintWriter writer) {
    
    Set<String> queueSet = getQueues();
    if (queueSet == null) {
      writer.println("No queues configured.");
      return;
    }
    StringBuffer sb = new StringBuffer();
    for (String queue : queueSet) {
      sb.append(queue).append(",");
    }
    writer.println("hadoop.rm.queue.names: " + sb.substring(0, sb.length()-1));
    
    for (String queue : queueSet) {
      writer.println(toFullPropertyName(queue, "guaranteed-capacity-maps") + 
                      ": " + getGuaranteedCapacityMaps(queue));
      writer.println(toFullPropertyName(queue, "guaranteed-capacity-reduces") + 
                      ": " + getGuaranteedCapacityReduces(queue));
      writer.println(toFullPropertyName(queue, "reclaim-time-limit") + 
                      ": " + getReclaimTimeLimit(queue));
      writer.println(toFullPropertyName(queue, "minimum-user-limit-percent") +
                      ": " + getMinimumUserLimitPercent(queue));
      writer.println(toFullPropertyName(queue, "supports-priority") + 
                      ": " + isPrioritySupported(queue));
      writer.println(toFullPropertyName(queue, "allowed-users-override") +
                      ": " + doAllowedUsersOverride(queue));
      printUserList(writer, queue, "allowed-users", getAllowedUsers(queue));
      printUserList(writer, queue, "denied-users", getDeniedUsers(queue));
    }
  }

  private void printUserList(PrintWriter writer, String queue, 
                              String listType, String[] users) {
    if (users == null) {
      writer.println(toFullPropertyName(queue, listType) + ": No users configured.");
    } else {
      writer.println(toFullPropertyName(queue, listType) + ": ");
      Arrays.sort(users);
      for (String user : users) {
        writer.println("\t" + user);
      }
    }
  }
  
  private synchronized void checkQueue(String queue) {
    if (queues == null) {
      queues = getQueues();
    }
    if (!queues.contains(queue)) {
      throw new IllegalArgumentException("Queue " + queue + " is undefined.");
    }
  }

  private static final String toFullPropertyName(String queue, 
                                                  String property) {
      return QUEUE_CONF_PROPERTY_NAME_PREFIX + queue + "." + property;
  }
}

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

/**
 * Class providing access to Capacity scheduler configuration and default values
 * for queue-configuration. Capacity scheduler configuration includes settings
 * for the {@link JobInitializationPoller} and default values for queue
 * configuration. These are read from the file
 * {@link CapacitySchedulerConf#SCHEDULER_CONF_FILE} on the CLASSPATH. The main
 * queue configuration is defined in the file
 * {@link QueueManager#QUEUE_CONF_FILE_NAME} on the CLASSPATH.
 * 
 * <p>
 * 
 * This class also provides APIs to get and set the configuration for the
 * queues.
 */
class CapacitySchedulerConf {

  static final Log LOG = LogFactory.getLog(CapacitySchedulerConf.class);

  static final String CAPACITY_PROPERTY = "capacity";

  static final String SUPPORTS_PRIORITY_PROPERTY = "supports-priority";

  static final String MAXIMUM_INITIALIZED_JOBS_PER_USER_PROPERTY =
      "maximum-initialized-jobs-per-user";

  static final String MINIMUM_USER_LIMIT_PERCENT_PROPERTY =
      "minimum-user-limit-percent";

  /** Default file name from which the capacity scheduler configuration is read. */
  public static final String SCHEDULER_CONF_FILE = "capacity-scheduler.xml";
  
  private int defaultUlimitMinimum;
  
  private boolean defaultSupportPriority;
  
  private static final String QUEUE_CONF_PROPERTY_NAME_PREFIX = 
    "mapred.capacity-scheduler.queue.";

  private Map<String, Properties> queueProperties
    = new HashMap<String,Properties>();

  /**
   * If {@link JobConf#MAPRED_TASK_MAXPMEM_PROPERTY} is set to
   * {@link JobConf#DISABLED_MEMORY_LIMIT}, this configuration will be used to
   * calculate job's physical memory requirements as a percentage of the job's
   * virtual memory requirements set via
   * {@link JobConf#setMaxVirtualMemoryForTask()}. This property thus provides
   * default value of physical memory for job's that don't explicitly specify
   * physical memory requirements.
   * <p/>
   * It defaults to {@link JobConf#DISABLED_MEMORY_LIMIT} and if not explicitly
   * set to a valid value, scheduler will not consider physical memory for
   * scheduling even if virtual memory based scheduling is enabled.
   *
   * @deprecated
   */
  @Deprecated
  static String DEFAULT_PERCENTAGE_OF_PMEM_IN_VMEM_PROPERTY =
    "mapred.capacity-scheduler.task.default-pmem-percentage-in-vmem";

  /**
   * Configuration that provides an upper limit on the maximum physical memory
   * that can be specified by a job. The job configuration
   * {@link JobConf#MAPRED_TASK_MAXPMEM_PROPERTY} should,
   * by definition, be less than this value. If not, the job will be rejected
   * by the scheduler. If it is set to {@link JobConf#DISABLED_MEMORY_LIMIT},
   * scheduler will not consider physical memory for scheduling even if virtual
   * memory based scheduling is enabled.
   *
   * @deprecated
   */
  @Deprecated
  static final String UPPER_LIMIT_ON_TASK_PMEM_PROPERTY =
    "mapred.capacity-scheduler.task.limit.maxpmem";
  
  /**
   * A maximum capacity defines a limit beyond which a sub-queue
   * cannot use the capacity of its parent queue.
   */
  static final String MAX_CAPACITY_PROPERTY ="maximum-capacity";

  /**
   * The constant which defines the default initialization thread
   * polling interval, denoted in milliseconds.
   */
  private static final int INITIALIZATION_THREAD_POLLING_INTERVAL = 5000;

  /**
   * The constant which defines the maximum number of worker threads to be
   * spawned off for job initialization
   */
  private static final int MAX_INITIALIZATION_WORKER_THREADS = 5;

  private Configuration rmConf;

  private int defaultMaxJobsPerUsersToInitialize;

  /**
   * Create a new CapacitySchedulerConf.
   * This method reads from the default configuration file mentioned in
   * {@link SCHEDULER_CONF_FILE}, that must be present in the classpath of the
   * application.
   */
  public CapacitySchedulerConf() {
    rmConf = new Configuration(false);
    getCSConf().addResource(SCHEDULER_CONF_FILE);
    initializeDefaults();
  }

  /**
   * Create a new CapacitySchedulerConf reading the specified configuration
   * file.
   * 
   * @param configFile {@link Path} to the configuration file containing
   * the Capacity scheduler configuration.
   */
  public CapacitySchedulerConf(Path configFile) {
    rmConf = new Configuration(false);
    getCSConf().addResource(configFile);
    initializeDefaults();
  }
  
  /*
   * Method used to initialize the default values and the queue list
   * which is used by the Capacity Scheduler.
   */
  private void initializeDefaults() {
    defaultUlimitMinimum = getCSConf().getInt(
        "mapred.capacity-scheduler.default-minimum-user-limit-percent", 100);
    defaultSupportPriority = getCSConf().getBoolean(
        "mapred.capacity-scheduler.default-supports-priority", false);
    defaultMaxJobsPerUsersToInitialize = getCSConf().getInt(
        "mapred.capacity-scheduler.default-maximum-initialized-jobs-per-user",
        2);
  }

  void setProperties(String queueName , Properties properties) {
    this.queueProperties.put(queueName,properties);
  }
  
  /**
   * Get the percentage of the cluster for the specified queue.
   * 
   * This method defaults to configured default Capacity if
   * no value is specified in the configuration for this queue. 
   * If the configured capacity is negative value or greater than 100 an
   * {@link IllegalArgumentException} is thrown.
   * 
   * If default capacity is not configured for a queue, then
   * system allocates capacity based on what is free at the time of 
   * capacity scheduler start
   * 
   * 
   * @param queue name of the queue
   * @return percent of the cluster for the queue.
   */
  public float getCapacity(String queue) {
    //Check done in order to return default capacity which can be negative
    //In case of both capacity and default capacity not configured.
    //Last check is if the configuration is specified and is marked as
    //negative we throw exception
    String raw = getProperty(queue, CAPACITY_PROPERTY);

    float result = this.getFloat(raw,-1);
    
    if (result > 100.0) {
      throw new IllegalArgumentException("Illegal capacity for queue " + queue +
                                         " of " + result);
    }
    return result;
  }

  String getProperty(String queue,String property) {
    if(!queueProperties.containsKey(queue))
     throw new IllegalArgumentException("Invalid queuename " + queue);

    //This check is still required as sometimes we create queue with null
    //This is typically happens in case of test.
    if(queueProperties.get(queue) != null) {
      return queueProperties.get(queue).getProperty(property);
    }

    return null;
  }

  /**
   * Return the maximum percentage of the cluster capacity that can be
   * used by the given queue
   * This percentage defines a limit beyond which a
   * sub-queue cannot use the capacity of its parent queue.
   * This provides a means to limit how much excess capacity a
   * sub-queue can use. By default, there is no limit.
   *
   * The maximum-capacity-stretch of a queue can only be
   * greater than or equal to its minimum capacity.
   * 
   * @param queue name of the queue
   * @return maximum capacity percent of cluster for the queue
   */
  public float getMaxCapacity(String queue) {
    String raw = getProperty(queue, MAX_CAPACITY_PROPERTY);
    float result = getFloat(raw,-1);
    result = (result <= 0) ? -1 : result; 
    if (result > 100.0) {
      throw new IllegalArgumentException("Illegal maximum-capacity-stretch " +
        "for queue " + queue +" of " + result);
    }

    if((result != -1) && (result < getCapacity(queue))) {
      throw new IllegalArgumentException("maximum-capacity-stretch " +
        "for a queue should be greater than capacity ");
    }
    return result;
  }

  /**
   * Get whether priority is supported for this queue.
   * 
   * If this value is false, then job priorities will be ignored in 
   * scheduling decisions. This method defaults to <code>false</code> if 
   * the property is not configured for this queue. 
   * @param queue name of the queue
   * @return Whether this queue supports priority or not.
   */
  public boolean isPrioritySupported(String queue) {
    String raw = getProperty(queue, SUPPORTS_PRIORITY_PROPERTY);
    return Boolean.parseBoolean(raw);
  }

  /**
   * Get the minimum limit of resources for any user submitting jobs in 
   * this queue, in percentage.
   * 
   * This method defaults to default user limit configured if
   * no value is specified in the configuration for this queue.
   * 
   * Throws an {@link IllegalArgumentException} when invalid value is 
   * configured.
   * 
   * @param queue name of the queue
   * @return minimum limit of resources, in percentage, that will be 
   * available for a user.
   * 
   */
  public int getMinimumUserLimitPercent(String queue) {
    String raw = getProperty(queue, MINIMUM_USER_LIMIT_PERCENT_PROPERTY);
    int userLimit = getInt(raw,defaultUlimitMinimum);
    if(userLimit <= 0 || userLimit > 100) {
      throw new IllegalArgumentException("Invalid user limit : "
          + userLimit + " for queue : " + queue);
    }
    return userLimit;
  }
  
  static final String toFullPropertyName(String queue, 
                                                  String property) {
      return QUEUE_CONF_PROPERTY_NAME_PREFIX + queue + "." + property;
  }

  /**
   * Gets the maximum number of jobs which are allowed to initialize in the
   * job queue.
   * 
   * @param queue queue name.
   * @return maximum number of jobs allowed to be initialized per user.
   * @throws IllegalArgumentException if maximum number of users is negative
   * or zero.
   */
  public int getMaxJobsPerUserToInitialize(String queue) {
    String raw =
        getProperty(queue, MAXIMUM_INITIALIZED_JOBS_PER_USER_PROPERTY);
    int maxJobsPerUser = getInt(raw,defaultMaxJobsPerUsersToInitialize);
    if(maxJobsPerUser <= 0) {
      throw new IllegalArgumentException(
          "Invalid maximum jobs per user configuration " + maxJobsPerUser);
    }
    return maxJobsPerUser;
  }


  /**
   * Amount of time in milliseconds which poller thread and initialization
   * thread would sleep before looking at the queued jobs.
   * 
   * The default value if no corresponding configuration is present is
   * 5000 Milliseconds.
   *  
   * @return time in milliseconds.
   * @throws IllegalArgumentException if time is negative or zero.
   */
  public long getSleepInterval() {
    long sleepInterval = getCSConf().getLong(
        "mapred.capacity-scheduler.init-poll-interval", 
        INITIALIZATION_THREAD_POLLING_INTERVAL);
    
    if(sleepInterval <= 0) {
      throw new IllegalArgumentException(
          "Invalid initializater poller interval " + sleepInterval);
    }
    
    return sleepInterval;
  }

  /**
   * Gets maximum number of threads which are spawned to initialize jobs
   * in job queue in  parallel. The number of threads should be always less than
   * or equal to number of job queues present.
   * 
   * If number of threads is configured to be more than job queues present,
   * then number of job queues is used as number of threads used for initializing
   * jobs.
   * 
   * So a given thread can have responsibility of initializing jobs from more 
   * than one queue.
   * 
   * The default value is 5
   * 
   * @return maximum number of threads spawned to initialize jobs in job queue
   * in parallel.
   */
  public int getMaxWorkerThreads() {
    int maxWorkerThreads = getCSConf().getInt(
        "mapred.capacity-scheduler.init-worker-threads", 
        MAX_INITIALIZATION_WORKER_THREADS);
    if(maxWorkerThreads <= 0) {
      throw new IllegalArgumentException(
          "Invalid initializater worker thread number " + maxWorkerThreads);
    }
    return maxWorkerThreads;
  }

  public Configuration getCSConf() {
    return rmConf;
  }

  float getFloat(String valueString,float defaultValue) {
    if (valueString == null)
      return defaultValue;
    try {
      return Float.parseFloat(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  int getInt(String valueString,int defaultValue) {
    if (valueString == null)
      return defaultValue;
    try {
      return Integer.parseInt(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}

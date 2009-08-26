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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Queue.QueueState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.SecurityUtil.AccessControlList;
import org.apache.hadoop.util.StringUtils;

/**
 * Class that exposes information about queues maintained by the Hadoop
 * Map/Reduce framework.
 * 
 * The Map/Reduce framework can be configured with one or more queues,
 * depending on the scheduler it is configured with. While some 
 * schedulers work only with one queue, some schedulers support multiple 
 * queues.
 *  
 * Queues can be configured with various properties. Some of these
 * properties are common to all schedulers, and those are handled by this
 * class. Schedulers might also associate several custom properties with 
 * queues. Where such a case exists, the queue name must be used to link 
 * the common properties with the scheduler specific ones.  
 */
class QueueManager {
  
  private static final Log LOG = LogFactory.getLog(QueueManager.class);

  // Configured queues this is backed by queues Map , mentioned below
  private Set<String> queueNames;

  // Map of a queue name and Queue object
  private HashMap<String, Queue> queues;

  // Whether ACLs are enabled in the system or not.
  private boolean aclsEnabled;

  static final String QUEUE_STATE_SUFFIX = "state";

  static final String QUEUE_CONF_FILE_NAME = "mapred-queues.xml";

  // Prefix in configuration for queue related keys
  static final String QUEUE_CONF_PROPERTY_NAME_PREFIX
                          = "mapred.queue.";//Resource in which queue acls are configured.

  /**
   * Construct a new QueueManager using configuration specified in the passed
   * in {@link org.apache.hadoop.conf.Configuration} object.
   * 
   * @param conf Configuration object where queue configuration is specified.
   */
  public QueueManager(Configuration conf) {
    checkDeprecation(conf);
    conf.addResource(QUEUE_CONF_FILE_NAME);

    queues = new HashMap<String, Queue>();

    // First get the queue names 
    String[] queueNameValues = conf.getStrings("mapred.queue.names",
        new String[]{JobConf.DEFAULT_QUEUE_NAME});

    // Get configured ACLs and state for each queue
    aclsEnabled = conf.getBoolean("mapred.acls.enabled", false);
    for (String name : queueNameValues) {
      try {
        Map<String, AccessControlList> acls = getQueueAcls(name, conf);
        QueueState state = getQueueState(name, conf);
        queues.put(name, new Queue(name, acls, state));
      } catch (Throwable t) {
        LOG.warn("Not able to initialize queue " + name);
      }
    }
    // Sync queue names with the configured queues.
    queueNames = queues.keySet();
  }
  
  /**
   * Return the set of queues configured in the system.
   * 
   * The number of queues configured should be dependent on the Scheduler 
   * configured. Note that some schedulers work with only one queue, whereas
   * others can support multiple queues.
   *  
   * @return Set of queue names.
   */
  public synchronized Set<String> getQueues() {
    return queueNames;
  }
  
  /**
   * Return true if the given {@link Queue.QueueOperation} can be 
   * performed by the specified user on the given queue.
   * 
   * An operation is allowed if all users are provided access for this
   * operation, or if either the user or any of the groups specified is
   * provided access.
   * 
   * @param queueName Queue on which the operation needs to be performed. 
   * @param oper The operation to perform
   * @param ugi The user and groups who wish to perform the operation.
   * 
   * @return true if the operation is allowed, false otherwise.
   */
  public synchronized boolean hasAccess(String queueName, 
                                Queue.QueueOperation oper,
                                UserGroupInformation ugi) {
    return hasAccess(queueName, null, oper, ugi);
  }
  
  /**
   * Return true if the given {@link Queue.QueueOperation} can be 
   * performed by the specified user on the specified job in the given queue.
   * 
   * An operation is allowed either if the owner of the job is the user 
   * performing the task, all users are provided access for this
   * operation, or if either the user or any of the groups specified is
   * provided access.
   * 
   * If the {@link Queue.QueueOperation} is not job specific then the 
   * job parameter is ignored.
   * 
   * @param queueName Queue on which the operation needs to be performed.
   * @param job The {@link JobInProgress} on which the operation is being
   *            performed. 
   * @param oper The operation to perform
   * @param ugi The user and groups who wish to perform the operation.
   * 
   * @return true if the operation is allowed, false otherwise.
   */
  public synchronized boolean hasAccess(String queueName, JobInProgress job, 
                                Queue.QueueOperation oper, 
                                UserGroupInformation ugi) {
    if (!aclsEnabled) {
      return true;
    }
    
    Queue q = queues.get(queueName);
    if (q == null) {
      LOG.info("Queue " + queueName + " is not present");
      return false;
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("checking access for : " 
          + QueueManager.toFullPropertyName(queueName, oper.getAclName()));
    }

    if (oper.isJobOwnerAllowed()) {
      if (job != null 
          && job.getJobConf().getUser().equals(ugi.getUserName())) {
        return true;
      }
    }
    
    AccessControlList acl = q.getAcls().get(
                                toFullPropertyName(queueName, 
                                    oper.getAclName()));
    if (acl == null) {
      return false;
    }

    // Check the ACL list
    boolean allowed = acl.allAllowed();
    if (!allowed) {
      // Check the allowed users list
      if (acl.getUsers().contains(ugi.getUserName())) {
        allowed = true;
      } else {
        // Check the allowed groups list
        Set<String> allowedGroups = acl.getGroups();
        for (String group : ugi.getGroupNames()) {
          if (allowedGroups.contains(group)) {
            allowed = true;
            break;
          }
        }
      }
    }

    return allowed;
  }

  /**
   * Checks whether the given queue is running or not.
   * @param queueName name of the queue
   * @return true, if the queue is running.
   */
  synchronized boolean isRunning(String queueName) {
    Queue q = queues.get(queueName);
    if (q != null) {
      return q.getState().equals(QueueState.RUNNING);
    }
    return false;
  }
  
  /**
   * Set a generic Object that represents scheduling information relevant
   * to a queue.
   * 
   * A string representation of this Object will be used by the framework
   * to display in user facing applications like the JobTracker web UI and
   * the hadoop CLI.
   * 
   * @param queueName queue for which the scheduling information is to be set. 
   * @param queueInfo scheduling information for this queue.
   */
  public synchronized void setSchedulerInfo(String queueName, 
                                              Object queueInfo) {
    if (queues.get(queueName) != null)
      queues.get(queueName).setSchedulingInfo(queueInfo);
  }
  
  /**
   * Return the scheduler information configured for this queue.
   * 
   * @param queueName queue for which the scheduling information is required.
   * @return The scheduling information for this queue.
   * 
   * @see #setSchedulingInfo(String, Object)
   */
  public synchronized Object getSchedulerInfo(String queueName) {
    if (queues.get(queueName) != null)
      return queues.get(queueName).getSchedulingInfo();
    return null;
  }
  
  /**
   * Refresh the acls and state for the configured queues in the system 
   * by reading it from mapred-queues.xml.
   * 
   * Previously configured queues and if or not acls are disabled is retained.
   * 
   * @throws IOException when queue configuration file is invalid.
   */
  synchronized void refreshQueues(Configuration conf) throws IOException {

    // First check if things are configured in mapred-site.xml,
    // so we can print out a deprecation warning. 
    // This check is needed only until we support the configuration
    // in mapred-site.xml
    checkDeprecation(conf);
    
    // Add the queue configuration file. Values from mapred-site.xml
    // will be overridden.
    conf.addResource(QUEUE_CONF_FILE_NAME);
    
    // Now we refresh the properties of the queues. Note that we
    // do *not* refresh the queue names or the acls flag. Instead
    // we use the older values configured for them.
    LOG.info("Refreshing acls and state for configured queues.");
    try {
      Iterator<String> itr = queueNames.iterator();
      while(itr.hasNext()) {
        String name = itr.next();
        Queue q = queues.get(name);
        Map<String, AccessControlList> newAcls = getQueueAcls(name, conf);
        QueueState newState = getQueueState(name, conf);
        q.setAcls(newAcls);
        q.setState(newState);
      }
    } catch (Throwable t) {
      String exceptionString = StringUtils.stringifyException(t);
      LOG.warn("Queues could not be refreshed because there was an " +
      		"exception in parsing the configuration: "+ exceptionString +
      		". Existing ACLs/state is retained.");
      throw new IOException(exceptionString);
    }
  }
  
  // Check if queue properties are configured in the passed in
  // configuration. If yes, print out deprecation warning messages.
  private void checkDeprecation(Configuration conf) {

    // check if queues are defined.
    String[] queues = null;
    String queueNameValues = conf.get("mapred.queue.names");
    if (queueNameValues != null) {
      LOG.warn("Configuring \"mapred.queue.names\" in mapred-site.xml or " +
          		"hadoop-site.xml is deprecated. Configure " +
              "\"mapred.queue.names\" in " +
          		QUEUE_CONF_FILE_NAME);
      // store queues so we can check if ACLs are also configured
      // in the deprecated files.
      queues = conf.getStrings("mapred.queue.names");
    }
    
    // check if the acls flag is defined
    String aclsEnable = conf.get("mapred.acls.enabled");
    if (aclsEnable != null) {
      LOG.warn("Configuring \"mapred.acls.enabled\" in mapred-site.xml or " +
          		"hadoop-site.xml is deprecated. Configure " +
              "\"mapred.acls.enabled\" in " +
          		QUEUE_CONF_FILE_NAME);
    }
    
    // check if acls are defined
    if (queues != null) {
      for (String queue : queues) {
        for (Queue.QueueOperation oper : Queue.QueueOperation.values()) {
          String key = toFullPropertyName(queue, oper.getAclName());
          String aclString = conf.get(key);
          if (aclString != null) {
            LOG.warn("Configuring queue ACLs in mapred-site.xml or " +
          	  "hadoop-site.xml is deprecated. Configure queue ACLs in " + 
          		QUEUE_CONF_FILE_NAME);
            // even if one string is configured, it is enough for printing
            // the warning. so we can return from here.
            return;
          }
        }
      }
    }
  }
  
  // Parse ACLs for the queue from the configuration.
  private Map<String, AccessControlList> getQueueAcls(String name,
                                                        Configuration conf) {
    HashMap<String, AccessControlList> map = 
        new HashMap<String, AccessControlList>();
    for (Queue.QueueOperation oper : Queue.QueueOperation.values()) {
      String aclKey = toFullPropertyName(name, oper.getAclName());
      map.put(aclKey, new AccessControlList(conf.get(aclKey, "*")));
    }
    return map;
  }

  // Parse ACLs for the queue from the configuration.
  private QueueState getQueueState(String name, Configuration conf) {
    QueueState retState = QueueState.RUNNING;
    String stateVal = conf.get(toFullPropertyName(name,
                                                  QueueManager.QUEUE_STATE_SUFFIX),
                               QueueState.RUNNING.getStateName());
    for (QueueState state : QueueState.values()) {
      if (state.getStateName().equalsIgnoreCase(stateVal)) {
        retState = state;
        break;
      }
    }
    return retState;
  }
 
  public static final String toFullPropertyName(String queue,
      String property) {
    return QUEUE_CONF_PROPERTY_NAME_PREFIX + queue + "." + property;
  }

  synchronized JobQueueInfo[] getJobQueueInfos() {
    ArrayList<JobQueueInfo> queueInfoList = new ArrayList<JobQueueInfo>();
    for (String queue : queueNames) {
      JobQueueInfo queueInfo = getJobQueueInfo(queue);
      if (queueInfo != null) {
        queueInfoList.add(getJobQueueInfo(queue));  
      }
    }
    return  queueInfoList.toArray(
            new JobQueueInfo[queueInfoList.size()]);
  }

  synchronized JobQueueInfo getJobQueueInfo(String queue) {
    if (queues.get(queue) != null) {
      Object schedulingInfo = queues.get(queue).getSchedulingInfo();
      JobQueueInfo qInfo;
      if (schedulingInfo != null) {
        qInfo = new JobQueueInfo(queue, schedulingInfo.toString());
      } else {
        qInfo = new JobQueueInfo(queue, null);
      }
      qInfo.setQueueState(queues.get(queue).getState().getStateName());
      return qInfo;
    }
    return null;
  }

  /**
   * Generates the array of QueueAclsInfo object. 
   * 
   * The array consists of only those queues for which user has acls.
   *
   * @return QueueAclsInfo[]
   * @throws java.io.IOException
   */
  synchronized QueueAclsInfo[] getQueueAcls(UserGroupInformation ugi)
                                            throws IOException {
    //List of all QueueAclsInfo objects , this list is returned
    ArrayList<QueueAclsInfo> queueAclsInfolist =
            new ArrayList<QueueAclsInfo>();
    Queue.QueueOperation[] operations = Queue.QueueOperation.values();
    for (String queueName : queueNames) {
      QueueAclsInfo queueAclsInfo = null;
      ArrayList<String> operationsAllowed = null;
      for (Queue.QueueOperation operation : operations) {
        if (hasAccess(queueName, operation, ugi)) {
          if (operationsAllowed == null) {
            operationsAllowed = new ArrayList<String>();
          }
          operationsAllowed.add(operation.getAclName());
        }
      }
      if (operationsAllowed != null) {
        //There is atleast 1 operation supported for queue <queueName>
        //, hence initialize queueAclsInfo
        queueAclsInfo = new QueueAclsInfo(queueName, operationsAllowed.toArray
                (new String[operationsAllowed.size()]));
        queueAclsInfolist.add(queueAclsInfo);
      }
    }
    return queueAclsInfolist.toArray(new QueueAclsInfo[
            queueAclsInfolist.size()]);
  }

  // ONLY FOR TESTING - Do not use in production code.
  synchronized void setQueues(Queue[] queues) {
    for (Queue queue : queues) {
      this.queues.put(queue.getName(), queue);
    }
  }

  /**
   * prints the configuration of QueueManager in Json format.
   * The method should be modified accordingly whenever
   * QueueManager(Configuration) constructor is modified.
   * @param writer {@link}Writer object to which the configuration properties 
   * are printed in json format
   * @throws IOException
   */
  static void dumpConfiguration(Writer writer) throws IOException {
    Configuration conf = new Configuration(false);
    conf.addResource(QUEUE_CONF_FILE_NAME);
    Configuration.dumpConfiguration(conf, writer);
  }
}

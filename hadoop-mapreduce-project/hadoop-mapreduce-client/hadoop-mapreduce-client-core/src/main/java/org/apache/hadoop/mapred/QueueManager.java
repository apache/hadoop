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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.net.URL;


/**
 * Class that exposes information about queues maintained by the Hadoop
 * Map/Reduce framework.
 * <p>
 * The Map/Reduce framework can be configured with one or more queues,
 * depending on the scheduler it is configured with. While some
 * schedulers work only with one queue, some schedulers support multiple
 * queues. Some schedulers also support the notion of queues within
 * queues - a feature called hierarchical queues.
 * <p>
 * Queue names are unique, and used as a key to lookup queues. Hierarchical
 * queues are named by a 'fully qualified name' such as q1:q2:q3, where
 * q2 is a child queue of q1 and q3 is a child queue of q2.
 * <p>
 * Leaf level queues are queues that contain no queues within them. Jobs
 * can be submitted only to leaf level queues.
 * <p>
 * Queues can be configured with various properties. Some of these
 * properties are common to all schedulers, and those are handled by this
 * class. Schedulers might also associate several custom properties with
 * queues. These properties are parsed and maintained per queue by the
 * framework. If schedulers need more complicated structure to maintain
 * configuration per queue, they are free to not use the facilities
 * provided by the framework, but define their own mechanisms. In such cases,
 * it is likely that the name of the queue will be used to relate the
 * common properties of a queue with scheduler specific properties.
 * <p>
 * Information related to a queue, such as its name, properties, scheduling
 * information and children are exposed by this class via a serializable
 * class called {@link JobQueueInfo}.
 * <p>
 * Queues are configured in the configuration file mapred-queues.xml.
 * To support backwards compatibility, queues can also be configured
 * in mapred-site.xml. However, when configured in the latter, there is
 * no support for hierarchical queues.
 */
@InterfaceAudience.Private
public class QueueManager {

  private static final Log LOG = LogFactory.getLog(QueueManager.class);

  // Map of a queue name and Queue object
  private Map<String, Queue> leafQueues = new HashMap<String,Queue>();
  private Map<String, Queue> allQueues = new HashMap<String, Queue>();
  public static final String QUEUE_CONF_FILE_NAME = "mapred-queues.xml";
  static final String QUEUE_CONF_DEFAULT_FILE_NAME = "mapred-queues-default.xml";

  //Prefix in configuration for queue related keys
  static final String QUEUE_CONF_PROPERTY_NAME_PREFIX = "mapred.queue.";

  //Resource in which queue acls are configured.
  private Queue root = null;
  
  // represents if job and queue acls are enabled on the mapreduce cluster
  private boolean areAclsEnabled = false;

  /**
   * Factory method to create an appropriate instance of a queue
   * configuration parser.
   * <p>
   * Returns a parser that can parse either the deprecated property
   * style queue configuration in mapred-site.xml, or one that can
   * parse hierarchical queues in mapred-queues.xml. First preference
   * is given to configuration in mapred-site.xml. If no queue
   * configuration is found there, then a parser that can parse
   * configuration in mapred-queues.xml is created.
   *
   * @param conf Configuration instance that determines which parser
   *             to use.
   * @return Queue configuration parser
   */
  static QueueConfigurationParser getQueueConfigurationParser(
    Configuration conf, boolean reloadConf, boolean areAclsEnabled) {
    if (conf != null && conf.get(
      DeprecatedQueueConfigurationParser.MAPRED_QUEUE_NAMES_KEY) != null) {
      if (reloadConf) {
        conf.reloadConfiguration();
      }
      return new DeprecatedQueueConfigurationParser(conf);
    } else {
      URL xmlInUrl =
        Thread.currentThread().getContextClassLoader()
          .getResource(QUEUE_CONF_FILE_NAME);
      if (xmlInUrl == null) {
        xmlInUrl = Thread.currentThread().getContextClassLoader()
          .getResource(QUEUE_CONF_DEFAULT_FILE_NAME);
        assert xmlInUrl != null; // this should be in our jar
      }
      InputStream stream = null;
      try {
        stream = xmlInUrl.openStream();
        return new QueueConfigurationParser(new BufferedInputStream(stream),
            areAclsEnabled);
      } catch (IOException ioe) {
        throw new RuntimeException("Couldn't open queue configuration at " +
                                   xmlInUrl, ioe);
      } finally {
        IOUtils.closeStream(stream);
      }
    }
  }

  QueueManager() {// acls are disabled
    this(false);
  }

  QueueManager(boolean areAclsEnabled) {
    this.areAclsEnabled = areAclsEnabled;
    initialize(getQueueConfigurationParser(null, false, areAclsEnabled));
  }

  /**
   * Construct a new QueueManager using configuration specified in the passed
   * in {@link org.apache.hadoop.conf.Configuration} object.
   * <p>
   * This instance supports queue configuration specified in mapred-site.xml,
   * but without support for hierarchical queues. If no queue configuration
   * is found in mapred-site.xml, it will then look for site configuration
   * in mapred-queues.xml supporting hierarchical queues.
   *
   * @param clusterConf    mapreduce cluster configuration
   */
  public QueueManager(Configuration clusterConf) {
    areAclsEnabled = clusterConf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
    initialize(getQueueConfigurationParser(clusterConf, false, areAclsEnabled));
  }

  /**
   * Create an instance that supports hierarchical queues, defined in
   * the passed in configuration file.
   * <p>
   * This is mainly used for testing purposes and should not called from
   * production code.
   *
   * @param confFile File where the queue configuration is found.
   */
  QueueManager(String confFile, boolean areAclsEnabled) {
    this.areAclsEnabled = areAclsEnabled;
    QueueConfigurationParser cp =
        new QueueConfigurationParser(confFile, areAclsEnabled);
    initialize(cp);
  }

  /**
   * Initialize the queue-manager with the queue hierarchy specified by the
   * given {@link QueueConfigurationParser}.
   * 
   * @param cp
   */
  private void initialize(QueueConfigurationParser cp) {
    this.root = cp.getRoot();
    leafQueues.clear();
    allQueues.clear();
    //At this point we have root populated
    //update data structures leafNodes.
    leafQueues = getRoot().getLeafQueues();
    allQueues.putAll(getRoot().getInnerQueues());
    allQueues.putAll(leafQueues);

    LOG.info("AllQueues : " + allQueues + "; LeafQueues : " + leafQueues);
  }

  /**
   * Return the set of leaf level queues configured in the system to
   * which jobs are submitted.
   * <p>
   * The number of queues configured should be dependent on the Scheduler
   * configured. Note that some schedulers work with only one queue, whereas
   * others can support multiple queues.
   *
   * @return Set of queue names.
   */
  public synchronized Set<String> getLeafQueueNames() {
    return leafQueues.keySet();
  }

  /**
   * Return true if the given user is part of the ACL for the given
   * {@link QueueACL} name for the given queue.
   * <p>
   * An operation is allowed if all users are provided access for this
   * operation, or if either the user or any of the groups specified is
   * provided access.
   *
   * @param queueName Queue on which the operation needs to be performed.
   * @param qACL      The queue ACL name to be checked
   * @param ugi       The user and groups who wish to perform the operation.
   * @return true     if the operation is allowed, false otherwise.
   */
  public synchronized boolean hasAccess(
    String queueName, QueueACL qACL, UserGroupInformation ugi) {

    Queue q = leafQueues.get(queueName);

    if (q == null) {
      LOG.info("Queue " + queueName + " is not present");
      return false;
    }

    if(q.getChildren() != null && !q.getChildren().isEmpty()) {
      LOG.info("Cannot submit job to parent queue " + q.getName());
      return false;
    }

    if (!areAclsEnabled()) {
      return true;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking access for the acl " + toFullPropertyName(queueName,
        qACL.getAclName()) + " for user " + ugi.getShortUserName());
    }

    AccessControlList acl = q.getAcls().get(
        toFullPropertyName(queueName, qACL.getAclName()));
    if (acl == null) {
      return false;
    }

    // Check if user is part of the ACL
    return acl.isUserAllowed(ugi);
  }

  /**
   * Checks whether the given queue is running or not.
   *
   * @param queueName name of the queue
   * @return true, if the queue is running.
   */
  synchronized boolean isRunning(String queueName) {
    Queue q = leafQueues.get(queueName);
    if (q != null) {
      return q.getState().equals(QueueState.RUNNING);
    }
    return false;
  }

  /**
   * Set a generic Object that represents scheduling information relevant
   * to a queue.
   * <p>
   * A string representation of this Object will be used by the framework
   * to display in user facing applications like the JobTracker web UI and
   * the hadoop CLI.
   *
   * @param queueName queue for which the scheduling information is to be set.
   * @param queueInfo scheduling information for this queue.
   */
  public synchronized void setSchedulerInfo(
    String queueName,
    Object queueInfo) {
    if (allQueues.get(queueName) != null) {
      allQueues.get(queueName).setSchedulingInfo(queueInfo);
    }
  }

  /**
   * Return the scheduler information configured for this queue.
   *
   * @param queueName queue for which the scheduling information is required.
   * @return The scheduling information for this queue.
   */
  public synchronized Object getSchedulerInfo(String queueName) {
    if (allQueues.get(queueName) != null) {
      return allQueues.get(queueName).getSchedulingInfo();
    }
    return null;
  }

  static final String MSG_REFRESH_FAILURE_WITH_CHANGE_OF_HIERARCHY =
      "Unable to refresh queues because queue-hierarchy changed. "
          + "Retaining existing configuration. ";

  static final String MSG_REFRESH_FAILURE_WITH_SCHEDULER_FAILURE =
      "Scheduler couldn't refresh it's queues with the new"
          + " configuration properties. "
          + "Retaining existing configuration throughout the system.";

  /**
   * Refresh acls, state and scheduler properties for the configured queues.
   * <p>
   * This method reloads configuration related to queues, but does not
   * support changes to the list of queues or hierarchy. The expected usage
   * is that an administrator can modify the queue configuration file and
   * fire an admin command to reload queue configuration. If there is a
   * problem in reloading configuration, then this method guarantees that
   * existing queue configuration is untouched and in a consistent state.
   * 
   * @param schedulerRefresher
   * @throws IOException when queue configuration file is invalid.
   */
  synchronized void refreshQueues(Configuration conf,
      QueueRefresher schedulerRefresher)
      throws IOException {

    // Create a new configuration parser using the passed conf object.
    QueueConfigurationParser cp =
        getQueueConfigurationParser(conf, true, areAclsEnabled);

    /*
     * (1) Validate the refresh of properties owned by QueueManager. As of now,
     * while refreshing queue properties, we only check that the hierarchy is
     * the same w.r.t queue names, ACLs and state for each queue and don't
     * support adding new queues or removing old queues
     */
    if (!root.isHierarchySameAs(cp.getRoot())) {
      LOG.warn(MSG_REFRESH_FAILURE_WITH_CHANGE_OF_HIERARCHY);
      throw new IOException(MSG_REFRESH_FAILURE_WITH_CHANGE_OF_HIERARCHY);
    }

    /*
     * (2) QueueManager owned properties are validated. Now validate and
     * refresh the properties of scheduler in a single step.
     */
    if (schedulerRefresher != null) {
      try {
        schedulerRefresher.refreshQueues(cp.getRoot().getJobQueueInfo().getChildren());
      } catch (Throwable e) {
        StringBuilder msg =
            new StringBuilder(
                "Scheduler's refresh-queues failed with the exception : "
                    + StringUtils.stringifyException(e));
        msg.append("\n");
        msg.append(MSG_REFRESH_FAILURE_WITH_SCHEDULER_FAILURE);
        LOG.error(msg.toString());
        throw new IOException(msg.toString());
      }
    }

    /*
     * (3) Scheduler has validated and refreshed its queues successfully, now
     * refresh the properties owned by QueueManager
     */

    // First copy the scheduling information recursively into the new
    // queue-hierarchy. This is done to retain old scheduling information. This
    // is done after scheduler refresh and not before it because during refresh,
    // schedulers may wish to change their scheduling info objects too.
    cp.getRoot().copySchedulingInfo(this.root);

    // Now switch roots.
    initialize(cp);

    LOG.info("Queue configuration is refreshed successfully.");
  }

  // this method is for internal use only
  public static final String toFullPropertyName(
    String queue,
    String property) {
    return QUEUE_CONF_PROPERTY_NAME_PREFIX + queue + "." + property;
  }

  /**
   * Return an array of {@link JobQueueInfo} objects for all the
   * queues configurated in the system.
   *
   * @return array of JobQueueInfo objects.
   */
  synchronized JobQueueInfo[] getJobQueueInfos() {
    ArrayList<JobQueueInfo> queueInfoList = new ArrayList<JobQueueInfo>();
    for (String queue : allQueues.keySet()) {
      JobQueueInfo queueInfo = getJobQueueInfo(queue);
      if (queueInfo != null) {
        queueInfoList.add(queueInfo);
      }
    }
    return queueInfoList.toArray(
      new JobQueueInfo[queueInfoList.size()]);
  }


  /**
   * Return {@link JobQueueInfo} for a given queue.
   *
   * @param queue name of the queue
   * @return JobQueueInfo for the queue, null if the queue is not found.
   */
  synchronized JobQueueInfo getJobQueueInfo(String queue) {
    if (allQueues.containsKey(queue)) {
      return allQueues.get(queue).getJobQueueInfo();
    }

    return null;
  }

  /**
   * JobQueueInfo for all the queues.
   * <p>
   * Contribs can use this data structure to either create a hierarchy or for
   * traversing.
   * They can also use this to refresh properties in case of refreshQueues
   *
   * @return a map for easy navigation.
   */
  synchronized Map<String, JobQueueInfo> getJobQueueInfoMapping() {
    Map<String, JobQueueInfo> m = new HashMap<String, JobQueueInfo>();

    for (Map.Entry<String,Queue> entry : allQueues.entrySet()) {
      m.put(entry.getKey(), entry.getValue().getJobQueueInfo());
    }

    return m;
  }

  /**
   * Generates the array of QueueAclsInfo object.
   * <p>
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
    QueueACL[] qAcls = QueueACL.values();
    for (String queueName : leafQueues.keySet()) {
      QueueAclsInfo queueAclsInfo = null;
      ArrayList<String> operationsAllowed = null;
      for (QueueACL qAcl : qAcls) {
        if (hasAccess(queueName, qAcl, ugi)) {
          if (operationsAllowed == null) {
            operationsAllowed = new ArrayList<String>();
          }
          operationsAllowed.add(qAcl.getAclName());
        }
      }
      if (operationsAllowed != null) {
        //There is atleast 1 operation supported for queue <queueName>
        //, hence initialize queueAclsInfo
        queueAclsInfo = new QueueAclsInfo(
          queueName, operationsAllowed.toArray
            (new String[operationsAllowed.size()]));
        queueAclsInfolist.add(queueAclsInfo);
      }
    }
    return queueAclsInfolist.toArray(
      new QueueAclsInfo[queueAclsInfolist.size()]);
  }

 
 

  /**
   * Return if ACLs are enabled for the Map/Reduce system
   *
   * @return true if ACLs are enabled.
   */
  boolean areAclsEnabled() {
    return areAclsEnabled;
  }

  /**
   * Used only for test.
   *
   * @return
   */
  Queue getRoot() {
    return root;
  }

  
  /**
   * Dumps the configuration of hierarchy of queues
   * @param out the writer object to which dump is written
   * @throws IOException
   */
  static void dumpConfiguration(Writer out,Configuration conf) throws IOException {
    dumpConfiguration(out, null,conf);
  }
  
  /***
   * Dumps the configuration of hierarchy of queues with 
   * the xml file path given. It is to be used directly ONLY FOR TESTING.
   * @param out the writer object to which dump is written to.
   * @param configFile the filename of xml file
   * @throws IOException
   */
  static void dumpConfiguration(Writer out, String configFile,
      Configuration conf) throws IOException {
    if (conf != null && conf.get(DeprecatedQueueConfigurationParser.
        MAPRED_QUEUE_NAMES_KEY) != null) {
      return;
    }
    
    JsonFactory dumpFactory = new JsonFactory();
    JsonGenerator dumpGenerator = dumpFactory.createGenerator(out);
    QueueConfigurationParser parser;
    boolean aclsEnabled = false;
    if (conf != null) {
      aclsEnabled = conf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
    }
    if (configFile != null && !"".equals(configFile)) {
      parser = new QueueConfigurationParser(configFile, aclsEnabled);
    }
    else {
      parser = getQueueConfigurationParser(null, false, aclsEnabled);
    }
    dumpGenerator.writeStartObject();
    dumpGenerator.writeFieldName("queues");
    dumpGenerator.writeStartArray();
    dumpConfiguration(dumpGenerator,parser.getRoot().getChildren());
    dumpGenerator.writeEndArray();
    dumpGenerator.writeEndObject();
    dumpGenerator.flush();
  }

  /**
   * method to perform depth-first search and write the parameters of every 
   * queue in JSON format.
   * @param dumpGenerator JsonGenerator object which takes the dump and flushes
   *  to a writer object
   * @param rootQueues the top-level queues
   * @throws JsonGenerationException
   * @throws IOException
   */
  private static void dumpConfiguration(JsonGenerator dumpGenerator,
      Set<Queue> rootQueues) throws JsonGenerationException, IOException {
    for (Queue queue : rootQueues) {
      dumpGenerator.writeStartObject();
      dumpGenerator.writeStringField("name", queue.getName());
      dumpGenerator.writeStringField("state", queue.getState().toString());
      AccessControlList submitJobList = null;
      AccessControlList administerJobsList = null;
      if (queue.getAcls() != null) {
        submitJobList =
          queue.getAcls().get(toFullPropertyName(queue.getName(),
              QueueACL.SUBMIT_JOB.getAclName()));
        administerJobsList =
          queue.getAcls().get(toFullPropertyName(queue.getName(),
              QueueACL.ADMINISTER_JOBS.getAclName()));
      }
      String aclsSubmitJobValue = " ";
      if (submitJobList != null ) {
        aclsSubmitJobValue = submitJobList.getAclString();
      }
      dumpGenerator.writeStringField("acl_submit_job", aclsSubmitJobValue);
      String aclsAdministerValue = " ";
      if (administerJobsList != null) {
        aclsAdministerValue = administerJobsList.getAclString();
      }
      dumpGenerator.writeStringField("acl_administer_jobs",
          aclsAdministerValue);
      dumpGenerator.writeFieldName("properties");
      dumpGenerator.writeStartArray();
      if (queue.getProperties() != null) {
        for (Map.Entry<Object, Object>property :
          queue.getProperties().entrySet()) {
          dumpGenerator.writeStartObject();
          dumpGenerator.writeStringField("key", (String)property.getKey());
          dumpGenerator.writeStringField("value", (String)property.getValue());
          dumpGenerator.writeEndObject();
        }
      }
      dumpGenerator.writeEndArray();
      Set<Queue> childQueues = queue.getChildren();
      dumpGenerator.writeFieldName("children");
      dumpGenerator.writeStartArray();
      if (childQueues != null && childQueues.size() > 0) {
        dumpConfiguration(dumpGenerator, childQueues);
      }
      dumpGenerator.writeEndArray();
      dumpGenerator.writeEndObject();
    }
  }

}

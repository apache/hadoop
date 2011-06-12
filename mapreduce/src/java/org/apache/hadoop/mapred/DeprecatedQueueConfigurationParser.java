/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.security.authorize.AccessControlList;
import static org.apache.hadoop.mapred.QueueManager.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Class to build queue hierarchy using deprecated conf(mapred-site.xml).
 * Generates a single level of queue hierarchy. 
 * 
 */
class DeprecatedQueueConfigurationParser extends QueueConfigurationParser {
  private static final Log LOG =
    LogFactory.getLog(DeprecatedQueueConfigurationParser.class);
  static final String MAPRED_QUEUE_NAMES_KEY = "mapred.queue.names";

  DeprecatedQueueConfigurationParser(Configuration conf) {
    //If not configuration done return immediately.
    if(!deprecatedConf(conf)) {
      return;
    }
    List<Queue> listq = createQueues(conf);
    this.setAclsEnabled(conf.getBoolean(MRConfig.MR_ACLS_ENABLED, false));
    root = new Queue();
    root.setName("");
    for (Queue q : listq) {
      root.addChild(q);
    }
  }

  private List<Queue> createQueues(Configuration conf) {
    String[] queueNameValues = conf.getStrings(
      MAPRED_QUEUE_NAMES_KEY);
    List<Queue> list = new ArrayList<Queue>();
    for (String name : queueNameValues) {
      try {
        Map<String, AccessControlList> acls = getQueueAcls(
          name, conf);
        QueueState state = getQueueState(name, conf);
        Queue q = new Queue(name, acls, state);
        list.add(q);
      } catch (Throwable t) {
        LOG.warn("Not able to initialize queue " + name);
      }
    }
    return list;
  }

  /**
   * Only applicable to leaf level queues
   * Parse ACLs for the queue from the configuration.
   */
  private QueueState getQueueState(String name, Configuration conf) {
    String stateVal = conf.get(
        toFullPropertyName(name, "state"),
        QueueState.RUNNING.getStateName());
    return QueueState.getState(stateVal);
  }

  /**
   * Check if queue properties are configured in the passed in
   * configuration. If yes, print out deprecation warning messages.
   */
  private boolean deprecatedConf(Configuration conf) {
    String[] queues = null;
    String queueNameValues = getQueueNames(conf);
    if (queueNameValues == null) {
      return false;
    } else {
      LOG.warn(
          "Configuring \"" + MAPRED_QUEUE_NAMES_KEY
          + "\" in mapred-site.xml or "
          + "hadoop-site.xml is deprecated and will overshadow "
          + QUEUE_CONF_FILE_NAME + ". Remove this property and configure "
          + "queue hierarchy in " + QUEUE_CONF_FILE_NAME);
      // store queues so we can check if ACLs are also configured
      // in the deprecated files.
      queues = conf.getStrings(MAPRED_QUEUE_NAMES_KEY);
    }

    // check if acls are defined
    if (queues != null) {
      for (String queue : queues) {
        for (QueueACL qAcl : QueueACL.values()) {
          String key = toFullPropertyName(queue, qAcl.getAclName());
          String aclString = conf.get(key);
          if (aclString != null) {
            LOG.warn(
              "Configuring queue ACLs in mapred-site.xml or " +
                "hadoop-site.xml is deprecated. Configure queue ACLs in " +
                QUEUE_CONF_FILE_NAME);
            // even if one string is configured, it is enough for printing
            // the warning. so we can return from here.
            return true;
          }
        }
      }
    }
    return true;
  }

  private String getQueueNames(Configuration conf) {
    String queueNameValues = conf.get(MAPRED_QUEUE_NAMES_KEY);
    return queueNameValues;
  }

  /**
   * Parse ACLs for the queue from the configuration.
   */
  private Map<String, AccessControlList> getQueueAcls(
    String name,
    Configuration conf) {
    HashMap<String, AccessControlList> map =
      new HashMap<String, AccessControlList>();
    for (QueueACL qAcl : QueueACL.values()) {
      String aclKey = toFullPropertyName(name, qAcl.getAclName());
      map.put(
        aclKey, new AccessControlList(
          conf.get(
            aclKey, "*")));
    }
    return map;
  }
}

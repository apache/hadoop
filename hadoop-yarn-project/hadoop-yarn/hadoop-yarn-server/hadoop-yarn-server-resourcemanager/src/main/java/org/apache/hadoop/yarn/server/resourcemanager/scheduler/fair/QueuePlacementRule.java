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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public abstract class QueuePlacementRule {
  protected boolean create;
  public static final Log LOG =
      LogFactory.getLog(QueuePlacementRule.class.getName());

  /**
   * Initializes the rule with any arguments.
   * 
   * @param args
   *    Additional attributes of the rule's xml element other than create.
   */
  public QueuePlacementRule initialize(boolean create, Map<String, String> args) {
    this.create = create;
    return this;
  }
  
  /**
   * 
   * @param requestedQueue
   *    The queue explicitly requested.
   * @param user
   *    The user submitting the app.
   * @param groups
   *    The groups of the user submitting the app.
   * @param configuredQueues
   *    The queues specified in the scheduler configuration.
   * @return
   *    The queue to place the app into. An empty string indicates that we should
   *    continue to the next rule, and null indicates that the app should be rejected.
   */
  public String assignAppToQueue(String requestedQueue, String user,
      Groups groups, Map<FSQueueType, Set<String>> configuredQueues)
      throws IOException {
   String queue = getQueueForApp(requestedQueue, user, groups,
        configuredQueues);
    if (create || configuredQueues.get(FSQueueType.LEAF).contains(queue)
        || configuredQueues.get(FSQueueType.PARENT).contains(queue)) {
      return queue;
    } else {
      return "";
    }
  }
  
  public void initializeFromXml(Element el)
      throws AllocationConfigurationException {
    boolean create = true;
    NamedNodeMap attributes = el.getAttributes();
    Map<String, String> args = new HashMap<String, String>();
    for (int i = 0; i < attributes.getLength(); i++) {
      Node node = attributes.item(i);
      String key = node.getNodeName();
      String value = node.getNodeValue();
      if (key.equals("create")) {
        create = Boolean.parseBoolean(value);
      } else {
        args.put(key, value);
      }
    }
    initialize(create, args);
  }
  
  /**
   * Returns true if this rule never tells the policy to continue.
   */
  public abstract boolean isTerminal();
  
  /**
   * Applies this rule to an app with the given requested queue and user/group
   * information.
   * 
   * @param requestedQueue
   *    The queue specified in the ApplicationSubmissionContext
   * @param user
   *    The user submitting the app.
   * @param groups
   *    The groups of the user submitting the app.
   * @return
   *    The name of the queue to assign the app to, or null to empty string
   *    continue to the next rule.
   */
  protected abstract String getQueueForApp(String requestedQueue, String user,
      Groups groups, Map<FSQueueType, Set<String>> configuredQueues)
      throws IOException;

  /**
   * Places apps in queues by username of the submitter
   */
  public static class User extends QueuePlacementRule {
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues) {
      return "root." + cleanName(user);
    }
    
    @Override
    public boolean isTerminal() {
      return create;
    }
  }
  
  /**
   * Places apps in queues by primary group of the submitter
   */
  public static class PrimaryGroup extends QueuePlacementRule {
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues)
        throws IOException {
      final List<String> groupList = groups.getGroups(user);
      if (groupList.isEmpty()) {
        throw new IOException("No groups returned for user " + user);
      }
      return "root." + cleanName(groupList.get(0));
    }
    
    @Override
    public boolean isTerminal() {
      return create;
    }
  }
  
  /**
   * Places apps in queues by secondary group of the submitter
   * 
   * Match will be made on first secondary group that exist in
   * queues
   */
  public static class SecondaryGroupExistingQueue extends QueuePlacementRule {
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues)
        throws IOException {
      List<String> groupNames = groups.getGroups(user);
      for (int i = 1; i < groupNames.size(); i++) {
        String group = cleanName(groupNames.get(i));
        if (configuredQueues.get(FSQueueType.LEAF).contains("root." + group)
            || configuredQueues.get(FSQueueType.PARENT).contains(
                "root." + group)) {
          return "root." + group;
        }
      }
      
      return "";
    }
        
    @Override
    public boolean isTerminal() {
      return false;
    }
  }

  /**
   * Places apps in queues with name of the submitter under the queue
   * returned by the nested rule.
   */
  public static class NestedUserQueue extends QueuePlacementRule {
    @VisibleForTesting
    QueuePlacementRule nestedRule;

    /**
     * Parse xml and instantiate the nested rule 
     */
    @Override
    public void initializeFromXml(Element el)
        throws AllocationConfigurationException {
      NodeList elements = el.getChildNodes();

      for (int i = 0; i < elements.getLength(); i++) {
        Node node = elements.item(i);
        if (node instanceof Element) {
          Element element = (Element) node;
          if ("rule".equals(element.getTagName())) {
            QueuePlacementRule rule = QueuePlacementPolicy
                .createAndInitializeRule(node);
            if (rule == null) {
              throw new AllocationConfigurationException(
                  "Unable to create nested rule in nestedUserQueue rule");
            }
            this.nestedRule = rule;
            break;
          } else {
            continue;
          }
        }
      }

      if (this.nestedRule == null) {
        throw new AllocationConfigurationException(
            "No nested rule specified in <nestedUserQueue> rule");
      }
      super.initializeFromXml(el);
    }
    
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues)
        throws IOException {
      // Apply the nested rule
      String queueName = nestedRule.assignAppToQueue(requestedQueue, user,
          groups, configuredQueues);
      
      if (queueName != null && queueName.length() != 0) {
        if (!queueName.startsWith("root.")) {
          queueName = "root." + queueName;
        }
        
        // Verify if the queue returned by the nested rule is an configured leaf queue,
        // if yes then skip to next rule in the queue placement policy
        if (configuredQueues.get(FSQueueType.LEAF).contains(queueName)) {
          return "";
        }
        return queueName + "." + cleanName(user);
      }
      return queueName;
    }

    @Override
    public boolean isTerminal() {
      return false;
    }
  }
  
  /**
   * Places apps in queues by requested queue of the submitter
   */
  public static class Specified extends QueuePlacementRule {
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues) {
      if (requestedQueue.equals(YarnConfiguration.DEFAULT_QUEUE_NAME)) {
        return "";
      } else {
        if (!requestedQueue.startsWith("root.")) {
          requestedQueue = "root." + requestedQueue;
        }
        return requestedQueue;
      }
    }
    
    @Override
    public boolean isTerminal() {
      return false;
    }
  }
  
  /**
   * Places apps in the specified default queue. If no default queue is
   * specified the app is placed in root.default queue.
   */
  public static class Default extends QueuePlacementRule {
    @VisibleForTesting
    String defaultQueueName;

    @Override
    public QueuePlacementRule initialize(boolean create,
        Map<String, String> args) {
      if (defaultQueueName == null) {
        defaultQueueName = "root." + YarnConfiguration.DEFAULT_QUEUE_NAME;
      }
      return super.initialize(create, args);
    }
    
    @Override
    public void initializeFromXml(Element el)
        throws AllocationConfigurationException {
      defaultQueueName = el.getAttribute("queue");
      if (defaultQueueName != null && !defaultQueueName.isEmpty()) {
        if (!defaultQueueName.startsWith("root.")) {
          defaultQueueName = "root." + defaultQueueName;
        }
      } else {
        defaultQueueName = "root." + YarnConfiguration.DEFAULT_QUEUE_NAME;
      }
      super.initializeFromXml(el);
    }

    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues) {
      return defaultQueueName;
    }

    @Override
    public boolean isTerminal() {
      return true;
    }
  }

  /**
   * Rejects all apps
   */
  public static class Reject extends QueuePlacementRule {
    @Override
    public String assignAppToQueue(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues) {
      return null;
    }
    
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Map<FSQueueType, Set<String>> configuredQueues) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isTerminal() {
      return true;
    }
  }

  /**
   * Replace the periods in the username or groupname with "_dot_" and
   * remove trailing and leading whitespace.
   */
  protected String cleanName(String name) {
    name = name.trim();
    if (name.contains(".")) {
      String converted = name.replaceAll("\\.", "_dot_");
      LOG.warn("Name " + name + " is converted to " + converted
          + " when it is used as a queue name.");
      return converted;
    } else {
      return name;
    }
  }
}

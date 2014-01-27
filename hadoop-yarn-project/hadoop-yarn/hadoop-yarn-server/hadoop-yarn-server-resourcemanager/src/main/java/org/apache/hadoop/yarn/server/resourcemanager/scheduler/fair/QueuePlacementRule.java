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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public abstract class QueuePlacementRule {
  protected boolean create;
  
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
      Groups groups, Collection<String> configuredQueues) throws IOException {
    String queue = getQueueForApp(requestedQueue, user, groups, configuredQueues);
    if (create || configuredQueues.contains(queue)) {
      return queue;
    } else {
      return "";
    }
  }
  
  public void initializeFromXml(Element el) {
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
      Groups groups, Collection<String> configuredQueues) throws IOException;

  /**
   * Places apps in queues by username of the submitter
   */
  public static class User extends QueuePlacementRule {
    @Override
    protected String getQueueForApp(String requestedQueue,
        String user, Groups groups, Collection<String> configuredQueues) {
      return "root." + user;
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
    protected String getQueueForApp(String requestedQueue,
        String user, Groups groups, 
        Collection<String> configuredQueues) throws IOException {
      return "root." + groups.getGroups(user).get(0);
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
    protected String getQueueForApp(String requestedQueue,
        String user, Groups groups, 
        Collection<String> configuredQueues) throws IOException {
      List<String> groupNames = groups.getGroups(user);
      for (int i = 1; i < groupNames.size(); i++) {
        if (configuredQueues.contains("root." + groupNames.get(i))) {
          return "root." + groupNames.get(i);
        }
      }
      
      return "";
    }
        
    @Override
    public boolean isTerminal() {
      return create;
    }
  }

  /**
   * Places apps in queues by requested queue of the submitter
   */
  public static class Specified extends QueuePlacementRule {
    @Override
    protected String getQueueForApp(String requestedQueue,
        String user, Groups groups, Collection<String> configuredQueues) {
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
   * Places all apps in the default queue
   */
  public static class Default extends QueuePlacementRule {
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Collection<String> configuredQueues) {
      return "root." + YarnConfiguration.DEFAULT_QUEUE_NAME;
    }
    
    @Override
    public boolean isTerminal() {
      return create;
    }
  }
  
  /**
   * Rejects all apps
   */
  public static class Reject extends QueuePlacementRule {
    @Override
    public String assignAppToQueue(String requestedQueue, String user,
        Groups groups, Collection<String> configuredQueues) {
      return null;
    }
    
    @Override
    protected String getQueueForApp(String requestedQueue, String user,
        Groups groups, Collection<String> configuredQueues) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isTerminal() {
      return true;
    }
  }
}

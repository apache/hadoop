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
import java.util.Collection;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.Groups;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public abstract class PoolPlacementRule {
  protected boolean create;
  
  /**
   * Initializes the rule with any arguments.
   * 
   * @param args
   *    Additional attributes of the rule's xml element other than create.
   */
  public PoolPlacementRule initialize(boolean create, Map<String, String> args) {
    this.create = create;
    return this;
  }
  
  /**
   * 
   * @param requestedPool
   *    The pool explicitly requested.
   * @param user
   *    The user submitting the job.
   * @param groups
   *    The groups of the user submitting the job.
   * @param configuredPools
   *    The pools specified in the scheduler configuration.
   * @return
   *    The pool to place the job into. An empty string indicates that we should
   *    continue to the next rule, and null indicates that the app should be rejected.
   */
  public String assignJobToPool(String requestedPool, String user,
      Groups groups, Collection<String> configuredPools) throws IOException {
    String pool = getPoolForJob(requestedPool, user, groups);
    if (create || configuredPools.contains(pool)) {
      return pool;
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
   * Applies this rule to an job with the given requested pool and user/group
   * information.
   * 
   * @param requestedPool
   *    The Pool specified in the Context
   * @param user
   *    The user submitting the job.
   * @param groups
   *    The groups of the user submitting the job.
   * @return
   *    The name of the Pool to assign the job to, or null to empty string
   *    continue to the next rule.
   */
  protected abstract String getPoolForJob(String requestedPool, String user,
      Groups groups) throws IOException;

  /**
   * Places jobs in pools by username of the submitter
   */
  public static class User extends PoolPlacementRule {
    @Override
    protected String getPoolForJob(String requestedPool,
        String user, Groups groups) {
      if (user != null) {
        return user; 
      } else {
        return Pool.DEFAULT_POOL_NAME;
      }
    }
    
    @Override
    public boolean isTerminal() {
      return create;
    }
  }
  
  /**
   * Places jobs in pools by primary group of the submitter
   */
  public static class PrimaryGroup extends PoolPlacementRule {
    @Override
    protected String getPoolForJob(String requestedPool,
        String user, Groups groups) throws IOException {
      if (user == null) {
        return Pool.DEFAULT_POOL_NAME;
      }
      List<String> groupList = groups.getGroups(user);

      if (groupList.size() > 0) {
        return groupList.get(0);
      } else {
        return Pool.DEFAULT_POOL_NAME;
      }
    }
    
    @Override
    public boolean isTerminal() {
      return create;
    }
  }

  /**
   * Places jobs in pools by requested pool of the submitter
   */
  public static class Specified extends PoolPlacementRule {
    @Override
    protected String getPoolForJob(String requestedPool,
        String user, Groups groups) {
      if (requestedPool.equals(Pool.DEFAULT_POOL_NAME)) {
        return "";
      } else {
        return requestedPool;
      }
    }
    
    @Override
    public boolean isTerminal() {
      return false;
    }
  }
  
  /**
   * Places all jobs in the default pool
   */
  public static class Default extends PoolPlacementRule {
    @Override
    protected String getPoolForJob(String requestedPool, String user,
        Groups groups) {
      return Pool.DEFAULT_POOL_NAME;
    }
    
    @Override
    public boolean isTerminal() {
      return create;
    }
  }
  
  /**
   * Rejects all jobs
   */
  public static class Reject extends PoolPlacementRule {
    @Override
    public String assignJobToPool(String requestedPool, String user,
        Groups groups, Collection<String> configuredPool) {
      return null;
    }
    
    @Override
    protected String getPoolForJob(String requestedPool, String user,
        Groups groups) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isTerminal() {
      return true;
    }
  }
}

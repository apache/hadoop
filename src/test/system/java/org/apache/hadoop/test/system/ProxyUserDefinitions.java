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
package org.apache.hadoop.test.system;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.net.URI;

/**
 *  Its the data container which contains host names and
 *  groups against each proxy user.
 */
public abstract class ProxyUserDefinitions {

  /**
   *  Groups and host names container
   */
  public class GroupsAndHost {
    private List<String> groups;
    private List<String> hosts;
    public List<String> getGroups() {
      return groups;
    }
    public void setGroups(List<String> groups) {
      this.groups = groups;
    }
    public List<String> getHosts() {
      return hosts;
    }
    public void setHosts(List<String> hosts) {
      this.hosts = hosts;
    }
  }

  protected Map<String, GroupsAndHost> proxyUsers;
  protected ProxyUserDefinitions () {
    proxyUsers = new HashMap<String, GroupsAndHost>();
  }

  /**
   * Add proxy user data to a container.
   * @param userName - proxy user name.
   * @param definitions - groups and host names.
   */
  public void addProxyUser (String userName, GroupsAndHost definitions) {
    proxyUsers.put(userName, definitions);
  }

  /**
   * Get the host names and groups against given proxy user.
   * @return - GroupsAndHost object.
   */
  public GroupsAndHost getProxyUser (String userName) {
    return proxyUsers.get(userName);
  }

  /**
   * Get the Proxy users data which contains the host names
   * and groups against each user.
   * @return - the proxy users data as hash map.
   */
  public Map<String, GroupsAndHost> getProxyUsers () {
    return proxyUsers;
  }

  /**
   * The implementation of this method has to be provided by a child of the class
   * @param filePath
   * @return
   * @throws IOException
   */
  public abstract boolean writeToFile(URI filePath) throws IOException;
}

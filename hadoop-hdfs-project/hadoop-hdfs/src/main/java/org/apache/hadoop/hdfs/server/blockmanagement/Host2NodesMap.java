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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSUtil;

/** A map from host names to datanode descriptors. */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class Host2NodesMap {
  private HashMap<String, String> mapHost = new HashMap<String, String>();
  private final HashMap<String, DatanodeDescriptor[]> map
    = new HashMap<String, DatanodeDescriptor[]>();
  private final ReadWriteLock hostmapLock = new ReentrantReadWriteLock();

  /** Check if node is already in the map. */
  boolean contains(DatanodeDescriptor node) {
    if (node==null) {
      return false;
    }
      
    String ipAddr = node.getIpAddr();
    hostmapLock.readLock().lock();
    try {
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      if (nodes != null) {
        for(DatanodeDescriptor containedNode:nodes) {
          if (node==containedNode) {
            return true;
          }
        }
      }
    } finally {
      hostmapLock.readLock().unlock();
    }
    return false;
  }
    
  /** add node to the map 
   * return true if the node is added; false otherwise.
   */
  boolean add(DatanodeDescriptor node) {
    hostmapLock.writeLock().lock();
    try {
      if (node==null || contains(node)) {
        return false;
      }
      
      String ipAddr = node.getIpAddr();
      String hostname = node.getHostName();
      
      mapHost.put(hostname, ipAddr);
      
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      DatanodeDescriptor[] newNodes;
      if (nodes==null) {
        newNodes = new DatanodeDescriptor[1];
        newNodes[0]=node;
      } else { // rare case: more than one datanode on the host
        newNodes = new DatanodeDescriptor[nodes.length+1];
        System.arraycopy(nodes, 0, newNodes, 0, nodes.length);
        newNodes[nodes.length] = node;
      }
      map.put(ipAddr, newNodes);
      return true;
    } finally {
      hostmapLock.writeLock().unlock();
    }
  }
    
  /** remove node from the map 
   * return true if the node is removed; false otherwise.
   */
  boolean remove(DatanodeDescriptor node) {
    if (node==null) {
      return false;
    }
      
    String ipAddr = node.getIpAddr();
    String hostname = node.getHostName();
    hostmapLock.writeLock().lock();
    try {

      DatanodeDescriptor[] nodes = map.get(ipAddr);
      if (nodes==null) {
        return false;
      }
      if (nodes.length==1) {
        if (nodes[0]==node) {
          map.remove(ipAddr);
          //remove hostname key since last datanode is removed
          mapHost.remove(hostname);
          return true;
        } else {
          return false;
        }
      }
      //rare case
      int i=0;
      for(; i<nodes.length; i++) {
        if (nodes[i]==node) {
          break;
        }
      }
      if (i==nodes.length) {
        return false;
      } else {
        DatanodeDescriptor[] newNodes;
        newNodes = new DatanodeDescriptor[nodes.length-1];
        System.arraycopy(nodes, 0, newNodes, 0, i);
        System.arraycopy(nodes, i+1, newNodes, i, nodes.length-i-1);
        map.put(ipAddr, newNodes);
        return true;
      }
    } finally {
      hostmapLock.writeLock().unlock();
    }
  }
    
  /**
   * Get a data node by its IP address.
   * @return DatanodeDescriptor if found, null otherwise 
   */
  DatanodeDescriptor getDatanodeByHost(String ipAddr) {
    if (ipAddr == null) {
      return null;
    }
      
    hostmapLock.readLock().lock();
    try {
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      // no entry
      if (nodes== null) {
        return null;
      }
      // one node
      if (nodes.length == 1) {
        return nodes[0];
      }
      // more than one node
      return nodes[DFSUtil.getRandom().nextInt(nodes.length)];
    } finally {
      hostmapLock.readLock().unlock();
    }
  }
  
  /**
   * Find data node by its transfer address
   *
   * @return DatanodeDescriptor if found or null otherwise
   */
  public DatanodeDescriptor getDatanodeByXferAddr(String ipAddr,
      int xferPort) {
    if (ipAddr==null) {
      return null;
    }

    hostmapLock.readLock().lock();
    try {
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      // no entry
      if (nodes== null) {
        return null;
      }
      for(DatanodeDescriptor containedNode:nodes) {
        if (xferPort == containedNode.getXferPort()) {
          return containedNode;
        }
      }
      return null;
    } finally {
      hostmapLock.readLock().unlock();
    }
  }

  

  /** get a data node by its hostname. This should be used if only one 
   * datanode service is running on a hostname. If multiple datanodes
   * are running on a hostname then use methods getDataNodeByXferAddr and
   * getDataNodeByHostNameAndPort.
   * @return DatanodeDescriptor if found; otherwise null.
   */
  DatanodeDescriptor getDataNodeByHostName(String hostname) {
    if(hostname == null) {
      return null;
    }
    
    hostmapLock.readLock().lock();
    try {
      String ipAddr = mapHost.get(hostname);
      if(ipAddr == null) {
        return null;
      } else {  
        return getDatanodeByHost(ipAddr);
      }
    } finally {
      hostmapLock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName())
        .append("[");
    for(Map.Entry<String, String> host: mapHost.entrySet()) {
      DatanodeDescriptor[] e = map.get(host.getValue());
      b.append("\n  " + host.getKey() + " => "+host.getValue() + " => " 
          + Arrays.asList(e));
    }
    return b.append("\n]").toString();
  }
}

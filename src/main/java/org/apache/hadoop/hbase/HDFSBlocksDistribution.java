/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;


/**
 * Data structure to describe the distribution of HDFS blocks amount hosts
 */
public class HDFSBlocksDistribution {
  private Map<String,HostAndWeight> hostAndWeights = null;
  private long uniqueBlocksTotalWeight = 0;
    
  /**
   * Stores the hostname and weight for that hostname.
   *
   * This is used when determining the physical locations of the blocks making
   * up a region.
   *
   * To make a prioritized list of the hosts holding the most data of a region,
   * this class is used to count the total weight for each host.  The weight is
   * currently just the size of the file.
   */
  public static class HostAndWeight {

    private String host;
    private long weight;

    /**
     * Constructor
     * @param host the host name
     * @param weight the weight
     */    
    public HostAndWeight(String host, long weight) {
      this.host = host;
      this.weight = weight;
    }

    /**
     * add weight
     * @param weight the weight
     */        
    public void addWeight(long weight) {
      this.weight += weight;
    }

    /**
     * @return the host name
     */            
    public String getHost() {
      return host;
    }

    /**
     * @return the weight
     */                
    public long getWeight() {
      return weight;
    }

    /**
     * comparator used to sort hosts based on weight
     */                
    public static class WeightComparator implements Comparator<HostAndWeight> {
      @Override
      public int compare(HostAndWeight l, HostAndWeight r) {
        if(l.getWeight() == r.getWeight()) {
          return l.getHost().compareTo(r.getHost());
        }
        return l.getWeight() < r.getWeight() ? -1 : 1;
      }
    }
  }
  
  /**
   * Constructor
   */
  public HDFSBlocksDistribution() {
    this.hostAndWeights =
      new TreeMap<String,HostAndWeight>();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public synchronized String toString() {
    return "number of unique hosts in the disribution=" +
      this.hostAndWeights.size();
  }

  /**
   * add some weight to a list of hosts, update the value of unique block weight
   * @param hosts the list of the host
   * @param weight the weight
   */
  public void addHostsAndBlockWeight(String[] hosts, long weight) {
    if (hosts == null || hosts.length == 0) {
      throw new NullPointerException("empty hosts");
    }
    addUniqueWeight(weight);
    for (String hostname : hosts) {
      addHostAndBlockWeight(hostname, weight);
    }
  }

  /**
   * add some weight to the total unique weight
   * @param weight the weight
   */        
  private void addUniqueWeight(long weight) {
    uniqueBlocksTotalWeight += weight;
  }
  
  
  /**
   * add some weight to a specific host
   * @param host the host name
   * @param weight the weight
   */
  private void addHostAndBlockWeight(String host, long weight) {
    if (host == null) {
      throw new NullPointerException("Passed hostname is null");
    }

    HostAndWeight hostAndWeight = this.hostAndWeights.get(host);
    if(hostAndWeight == null) {
      hostAndWeight = new HostAndWeight(host, weight);
      this.hostAndWeights.put(host, hostAndWeight);
    } else {
      hostAndWeight.addWeight(weight);
    }
  }

  /**
   * @return the hosts and their weights
   */
  public Map<String,HostAndWeight> getHostAndWeights() {
    return this.hostAndWeights;
  }

  /**
   * return the weight for a specific host, that will be the total bytes of all
   * blocks on the host
   * @param host the host name
   * @return the weight of the given host
   */
  public long getWeight(String host) {
    long weight = 0;
    if (host != null) {
      HostAndWeight hostAndWeight = this.hostAndWeights.get(host);
      if(hostAndWeight != null) {
        weight = hostAndWeight.getWeight();
      }
    }
    return weight;
  }
  
  /**
   * @return the sum of all unique blocks' weight
   */
  public long getUniqueBlocksTotalWeight() {
    return uniqueBlocksTotalWeight;
  }
  
  /**
   * return the locality index of a given host
   * @param host the host name
   * @return the locality index of the given host
   */
  public float getBlockLocalityIndex(String host) {
    float localityIndex = 0;
    HostAndWeight hostAndWeight = this.hostAndWeights.get(host);
    if (hostAndWeight != null && uniqueBlocksTotalWeight != 0) {
      localityIndex=(float)hostAndWeight.weight/(float)uniqueBlocksTotalWeight;
    }
    return localityIndex;
  }
  
  
  /**
   * This will add the distribution from input to this object
   * @param otherBlocksDistribution the other hdfs blocks distribution
   */
  public void add(HDFSBlocksDistribution otherBlocksDistribution) {
    Map<String,HostAndWeight> otherHostAndWeights =
      otherBlocksDistribution.getHostAndWeights();
    for (Map.Entry<String, HostAndWeight> otherHostAndWeight:
      otherHostAndWeights.entrySet()) {
      addHostAndBlockWeight(otherHostAndWeight.getValue().host,
        otherHostAndWeight.getValue().weight);
    }
    addUniqueWeight(otherBlocksDistribution.getUniqueBlocksTotalWeight());
  }
  
  /**
   * return the sorted list of hosts in terms of their weights
   */
  public List<String> getTopHosts() {
    NavigableSet<HostAndWeight> orderedHosts = new TreeSet<HostAndWeight>(
      new HostAndWeight.WeightComparator());
    orderedHosts.addAll(this.hostAndWeights.values());
    List<String> topHosts = new ArrayList<String>(orderedHosts.size());
    for(HostAndWeight haw : orderedHosts.descendingSet()) {
      topHosts.add(haw.getHost());
    }
    return topHosts;
  }

}

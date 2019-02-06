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
package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements the {@link DNSToSwitchMapping} via static mappings. Used
 * in testcases that simulate racks, and in the
 * {@link org.apache.hadoop.hdfs.MiniDFSCluster}
 *
 * A shared, static mapping is used; to reset it call {@link #resetMap()}.
 *
 * When an instance of the class has its {@link #setConf(Configuration)}
 * method called, nodes listed in the configuration will be added to the map.
 * These do not get removed when the instance is garbage collected.
 *
 * The switch mapping policy of this class is the same as for the
 * {@link ScriptBasedMapping} -the presence of a non-empty topology script.
 * The script itself is not used.
 */
public class StaticMapping extends AbstractDNSToSwitchMapping  {

  /**
   * Key to define the node mapping as a comma-delimited list of host=rack
   * mappings, e.g. <code>host1=r1,host2=r1,host3=r2</code>.
   * <p>
   * Value: {@value}
   * <p>
   * <b>Important: </b>spaces not trimmed and are considered significant.
   */
  public static final String KEY_HADOOP_CONFIGURED_NODE_MAPPING =
      "hadoop.configured.node.mapping";

  /**
   * Configure the mapping by extracting any mappings defined in the
   * {@link #KEY_HADOOP_CONFIGURED_NODE_MAPPING} field
   * @param conf new configuration
   */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      String[] mappings = conf.getStrings(KEY_HADOOP_CONFIGURED_NODE_MAPPING);
      if (mappings != null) {
        for (String str : mappings) {
          String host = str.substring(0, str.indexOf('='));
          String rack = str.substring(str.indexOf('=') + 1);
          addNodeToRack(host, rack);
        }
      }
    }
  }

  /**
   * retained lower case setter for compatibility reasons; relays to
   * {@link #setConf(Configuration)}
   * @param conf new configuration
   */
  public void setconf(Configuration conf) {
    setConf(conf);
  }

  /* Only one instance per JVM */
  private static final Map<String, String> nameToRackMap = new HashMap<String, String>();

  /**
   * Add a node to the static map. The moment any entry is added to the map,
   * the map goes multi-rack.
   * @param name node name
   * @param rackId rack ID
   */
  public static void addNodeToRack(String name, String rackId) {
    synchronized (nameToRackMap) {
      nameToRackMap.put(name, rackId);
    }
  }

  @Override
  public List<String> resolve(List<String> names) {
    List<String> m = new ArrayList<String>();
    synchronized (nameToRackMap) {
      for (String name : names) {
        String rackId;
        if ((rackId = nameToRackMap.get(name)) != null) {
          m.add(rackId);
        } else {
          m.add(NetworkTopology.DEFAULT_RACK);
        }
      }
      return m;
    }
  }

  /**
   * The switch policy of this mapping is driven by the same policy
   * as the Scripted mapping: the presence of the script name in
   * the configuration file
   * @return false, always
   */
  @Override
  public boolean isSingleSwitch() {
    return isSingleSwitchByScriptPolicy();
  }

  /**
   * Get a copy of the map (for diagnostics)
   * @return a clone of the map or null for none known
   */
  @Override
  public Map<String, String> getSwitchMap() {
    synchronized (nameToRackMap) {
      return new HashMap<String, String>(nameToRackMap);
    }
  }

  @Override
  public String toString() {
    return "static mapping with single switch = " + isSingleSwitch();
  }

  /**
   * Clear the map
   */
  public static void resetMap() {
    synchronized (nameToRackMap) {
      nameToRackMap.clear();
    }
  }
  
  public void reloadCachedMappings() {
    // reloadCachedMappings does nothing for StaticMapping; there is
    // nowhere to reload from since all data is in memory.
  }

  @Override
  public void reloadCachedMappings(List<String> names) {
    // reloadCachedMappings does nothing for StaticMapping; there is
    // nowhere to reload from since all data is in memory.
  }
}

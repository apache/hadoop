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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is a base class for DNS to Switch mappings. <p/> It is not mandatory to
 * derive {@link DNSToSwitchMapping} implementations from it, but it is strongly
 * recommended, as it makes it easy for the Hadoop developers to add new methods
 * to this base class that are automatically picked up by all implementations.
 * <p/>
 *
 * This class does not extend the <code>Configured</code>
 * base class, and should not be changed to do so, as it causes problems
 * for subclasses. The constructor of the <code>Configured</code> calls
 * the  {@link #setConf(Configuration)} method, which will call into the
 * subclasses before they have been fully constructed.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractDNSToSwitchMapping
    implements DNSToSwitchMapping, Configurable {

  private Configuration conf;

  /**
   * Create an unconfigured instance
   */
  protected AbstractDNSToSwitchMapping() {
  }

  /**
   * Create an instance, caching the configuration file.
   * This constructor does not call {@link #setConf(Configuration)}; if
   * a subclass extracts information in that method, it must call it explicitly.
   * @param conf the configuration
   */
  protected AbstractDNSToSwitchMapping(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Predicate that indicates that the switch mapping is known to be
   * single-switch. The base class returns false: it assumes all mappings are
   * multi-rack. Subclasses may override this with methods that are more aware
   * of their topologies.
   *
   * <p/>
   *
   * This method is used when parts of Hadoop need know whether to apply
   * single rack vs multi-rack policies, such as during block placement.
   * Such algorithms behave differently if they are on multi-switch systems.
   * </p>
   *
   * @return true if the mapping thinks that it is on a single switch
   */
  public boolean isSingleSwitch() {
    return false;
  }

  /**
   * Get a copy of the map (for diagnostics)
   * @return a clone of the map or null for none known
   */
  public Map<String, String> getSwitchMap() {
    return null;
  }

  /**
   * Generate a string listing the switch mapping implementation,
   * the mapping for every known node and the number of nodes and
   * unique switches known about -each entry to a separate line.
   * @return a string that can be presented to the ops team or used in
   * debug messages.
   */
  public String dumpTopology() {
    Map<String, String> rack = getSwitchMap();
    StringBuilder builder = new StringBuilder();
    builder.append("Mapping: ").append(toString()).append("\n");
    if (rack != null) {
      builder.append("Map:\n");
      Set<String> switches = new HashSet<String>();
      for (Map.Entry<String, String> entry : rack.entrySet()) {
        builder.append("  ")
            .append(entry.getKey())
            .append(" -> ")
            .append(entry.getValue())
            .append("\n");
        switches.add(entry.getValue());
      }
      builder.append("Nodes: ").append(rack.size()).append("\n");
      builder.append("Switches: ").append(switches.size()).append("\n");
    } else {
      builder.append("No topology information");
    }
    return builder.toString();
  }

  protected boolean isSingleSwitchByScriptPolicy() {
    return conf != null
        && conf.get(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY) == null;
  }

  /**
   * Query for a {@link DNSToSwitchMapping} instance being on a single
   * switch.
   * <p/>
   * This predicate simply assumes that all mappings not derived from
   * this class are multi-switch.
   * @param mapping the mapping to query
   * @return true if the base class says it is single switch, or the mapping
   * is not derived from this class.
   */
  public static boolean isMappingSingleSwitch(DNSToSwitchMapping mapping) {
    return mapping != null && mapping instanceof AbstractDNSToSwitchMapping
        && ((AbstractDNSToSwitchMapping) mapping).isSingleSwitch();
  }

}

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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;


/**
 * This class extends ScriptBasedMapping class and implements 
 * the {@link DNSToSwitchMappingWithDependency} interface using 
 * a script configured via the 
 * {@link CommonConfigurationKeys#NET_DEPENDENCY_SCRIPT_FILE_NAME_KEY} option.
 * <p/>
 * It contains a static class <code>RawScriptBasedMappingWithDependency</code>
 * that performs the getDependency work.
 * <p/>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ScriptBasedMappingWithDependency  extends ScriptBasedMapping 
    implements DNSToSwitchMappingWithDependency {
  /**
   * key to the dependency script filename {@value}
   */
  static final String DEPENDENCY_SCRIPT_FILENAME_KEY =
      CommonConfigurationKeys.NET_DEPENDENCY_SCRIPT_FILE_NAME_KEY;

  private Map<String, List<String>> dependencyCache = 
      new ConcurrentHashMap<String, List<String>>();

  /**
   * Create an instance with the default configuration.
   * </p>
   * Calling {@link #setConf(Configuration)} will trigger a
   * re-evaluation of the configuration settings and so be used to
   * set up the mapping script.
   */
  public ScriptBasedMappingWithDependency() {
    super(new RawScriptBasedMappingWithDependency());
  }

  /**
   * Get the cached mapping and convert it to its real type
   * @return the inner raw script mapping.
   */
  private RawScriptBasedMappingWithDependency getRawMapping() {
    return (RawScriptBasedMappingWithDependency)rawMapping;
  }

  @Override
  public String toString() {
    return "script-based mapping with " + getRawMapping().toString();
  }

  /**
   * {@inheritDoc}
   * <p/>
   * This will get called in the superclass constructor, so a check is needed
   * to ensure that the raw mapping is defined before trying to relaying a null
   * configuration.
   * @param conf
   */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    getRawMapping().setConf(conf);
  }

  /**
   * Get dependencies in the topology for a given host
   * @param name - host name for which we are getting dependency
   * @return a list of hosts dependent on the provided host name
   */
  @Override
  public List<String> getDependency(String name) {
    //normalize all input names to be in the form of IP addresses
    name = NetUtils.normalizeHostName(name);

    if (name==null) {
      return Collections.emptyList();
    }

    List<String> dependencies = dependencyCache.get(name);
    if (dependencies == null) {
      //not cached
      dependencies = getRawMapping().getDependency(name);
      if(dependencies != null) {
        dependencyCache.put(name, dependencies);
      }
    }

    return dependencies;
}

  /**
   * This is the uncached script mapping that is fed into the cache managed
   * by the superclass {@link CachedDNSToSwitchMapping}
   */
  private static final class RawScriptBasedMappingWithDependency
      extends ScriptBasedMapping.RawScriptBasedMapping 
      implements DNSToSwitchMappingWithDependency {
    private String dependencyScriptName;

    /**
     * Set the configuration and extract the configuration parameters of interest
     * @param conf the new configuration
     */
    @Override
    public void setConf (Configuration conf) {
      super.setConf(conf);
      if (conf != null) {
        dependencyScriptName = conf.get(DEPENDENCY_SCRIPT_FILENAME_KEY);
      } else {
        dependencyScriptName = null;
      }
    }

    /**
     * Constructor. The mapping is not ready to use until
     * {@link #setConf(Configuration)} has been called
     */
    public RawScriptBasedMappingWithDependency() {}

    @Override
    public List<String> getDependency(String name) {
      if (name==null || dependencyScriptName==null) {
        return Collections.emptyList();
      }

      List <String> m = new LinkedList<String>();
      List <String> args = new ArrayList<String>(1);
      args.add(name);
  
      String output = runResolveCommand(args,dependencyScriptName);
      if (output != null) {
        StringTokenizer allSwitchInfo = new StringTokenizer(output);
        while (allSwitchInfo.hasMoreTokens()) {
          String switchInfo = allSwitchInfo.nextToken();
          m.add(switchInfo);
        }
      } else {
        // an error occurred. return null to signify this.
        // (exn was already logged in runResolveCommand)
        return null;
      }

      return m;
    }

    @Override
    public String toString() {
      return super.toString() + ", " + dependencyScriptName != null ?
          ("dependency script " + dependencyScriptName) : NO_SCRIPT;
    }
  }
}

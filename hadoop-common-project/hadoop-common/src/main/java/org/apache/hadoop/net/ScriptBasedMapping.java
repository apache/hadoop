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
import java.io.*;

import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the {@link DNSToSwitchMapping} interface using a 
 * script configured via the
 * {@link CommonConfigurationKeys#NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY} option.
 * <p>
 * It contains a static class <code>RawScriptBasedMapping</code> that performs
 * the work: reading the configuration parameters, executing any defined
 * script, handling errors and such like. The outer
 * class extends {@link CachedDNSToSwitchMapping} to cache the delegated
 * queries.
 * <p>
 * This DNS mapper's {@link #isSingleSwitch()} predicate returns
 * true if and only if a script is defined.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ScriptBasedMapping extends CachedDNSToSwitchMapping {

  /**
   * Minimum number of arguments: {@value}
   */
  static final int MIN_ALLOWABLE_ARGS = 1;

  /**
   * Default number of arguments: {@value}
   */
  static final int DEFAULT_ARG_COUNT = 
                     CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_DEFAULT;

  /**
   * key to the script filename {@value}
   */
  static final String SCRIPT_FILENAME_KEY = 
                     CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY ;

  /**
   * key to the argument count that the script supports
   * {@value}
   */
  static final String SCRIPT_ARG_COUNT_KEY =
                     CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY ;
  /**
   * Text used in the {@link #toString()} method if there is no string
   * {@value}
   */
  public static final String NO_SCRIPT = "no script";

  /**
   * Create an instance with the default configuration.
   * <p>
   * Calling {@link #setConf(Configuration)} will trigger a
   * re-evaluation of the configuration settings and so be used to
   * set up the mapping script.
   *
   */
  public ScriptBasedMapping() {
    this(new RawScriptBasedMapping());
  }

  /**
   * Create an instance from the given raw mapping
   * @param rawMap raw DNSTOSwithMapping
   */
  public ScriptBasedMapping(DNSToSwitchMapping rawMap) {
    super(rawMap);
  }

  /**
   * Create an instance from the given configuration
   * @param conf configuration
   */
  public ScriptBasedMapping(Configuration conf) {
    this();
    setConf(conf);
  }

  /**
   * Get the cached mapping and convert it to its real type
   * @return the inner raw script mapping.
   */
  private RawScriptBasedMapping getRawMapping() {
    return (RawScriptBasedMapping)rawMapping;
  }

  @Override
  public Configuration getConf() {
    return getRawMapping().getConf();
  }

  @Override
  public String toString() {
    return "script-based mapping with " + getRawMapping().toString();
  }

  /**
   * {@inheritDoc}
   * <p>
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
   * This is the uncached script mapping that is fed into the cache managed
   * by the superclass {@link CachedDNSToSwitchMapping}
   */
  protected static class RawScriptBasedMapping
      extends AbstractDNSToSwitchMapping {
    private String scriptName;
    private int maxArgs; //max hostnames per call of the script
    private static final Logger LOG =
        LoggerFactory.getLogger(ScriptBasedMapping.class);

    /**
     * Set the configuration and extract the configuration parameters of interest
     * @param conf the new configuration
     */
    @Override
    public void setConf (Configuration conf) {
      super.setConf(conf);
      if (conf != null) {
        scriptName = conf.get(SCRIPT_FILENAME_KEY);
        maxArgs = conf.getInt(SCRIPT_ARG_COUNT_KEY, DEFAULT_ARG_COUNT);
      } else {
        scriptName = null;
        maxArgs = 0;
      }
    }

    /**
     * Constructor. The mapping is not ready to use until
     * {@link #setConf(Configuration)} has been called
     */
    public RawScriptBasedMapping() {}

    @Override
    public List<String> resolve(List<String> names) {
      List<String> m = new ArrayList<String>(names.size());

      if (names.isEmpty()) {
        return m;
      }

      if (scriptName == null) {
        for (String name : names) {
          m.add(NetworkTopology.DEFAULT_RACK);
        }
        return m;
      }

      String output = runResolveCommand(names, scriptName);
      if (output != null) {
        StringTokenizer allSwitchInfo = new StringTokenizer(output);
        while (allSwitchInfo.hasMoreTokens()) {
          String switchInfo = allSwitchInfo.nextToken();
          m.add(switchInfo);
        }

        if (m.size() != names.size()) {
          // invalid number of entries returned by the script
          LOG.error("Script " + scriptName + " returned "
              + Integer.toString(m.size()) + " values when "
              + Integer.toString(names.size()) + " were expected.");
          return null;
        }
      } else {
        // an error occurred. return null to signify this.
        // (exn was already logged in runResolveCommand)
        return null;
      }

      return m;
    }

    /**
     * Build and execute the resolution command. The command is
     * executed in the directory specified by the system property
     * "user.dir" if set; otherwise the current working directory is used
     * @param args a list of arguments
     * @return null if the number of arguments is out of range,
     * or the output of the command.
     */
    protected String runResolveCommand(List<String> args, 
        String commandScriptName) {
      int loopCount = 0;
      if (args.size() == 0) {
        return null;
      }
      StringBuilder allOutput = new StringBuilder();
      int numProcessed = 0;
      if (maxArgs < MIN_ALLOWABLE_ARGS) {
        LOG.warn("Invalid value " + Integer.toString(maxArgs)
            + " for " + SCRIPT_ARG_COUNT_KEY + "; must be >= "
            + Integer.toString(MIN_ALLOWABLE_ARGS));
        return null;
      }

      while (numProcessed != args.size()) {
        int start = maxArgs * loopCount;
        List<String> cmdList = new ArrayList<String>();
        cmdList.add(commandScriptName);
        for (numProcessed = start; numProcessed < (start + maxArgs) &&
            numProcessed < args.size(); numProcessed++) {
          cmdList.add(args.get(numProcessed));
        }
        File dir = null;
        String userDir;
        if ((userDir = System.getProperty("user.dir")) != null) {
          dir = new File(userDir);
        }
        ShellCommandExecutor s = new ShellCommandExecutor(
            cmdList.toArray(new String[cmdList.size()]), dir);
        try {
          s.execute();
          allOutput.append(s.getOutput()).append(" ");
        } catch (Exception e) {
          LOG.warn("Exception running " + s, e);
          return null;
        }
        loopCount++;
      }
      return allOutput.toString();
    }

    /**
     * Declare that the mapper is single-switched if a script was not named
     * in the configuration.
     * @return true iff there is no script
     */
    @Override
    public boolean isSingleSwitch() {
      return scriptName == null;
    }

    @Override
    public String toString() {
      return scriptName != null ? ("script " + scriptName) : NO_SCRIPT;
    }

    @Override
    public void reloadCachedMappings() {
      // Nothing to do here, since RawScriptBasedMapping has no cache, and
      // does not inherit from CachedDNSToSwitchMapping
    }

    @Override
    public void reloadCachedMappings(List<String> names) {
      // Nothing to do here, since RawScriptBasedMapping has no cache, and
      // does not inherit from CachedDNSToSwitchMapping
    }
  }
}

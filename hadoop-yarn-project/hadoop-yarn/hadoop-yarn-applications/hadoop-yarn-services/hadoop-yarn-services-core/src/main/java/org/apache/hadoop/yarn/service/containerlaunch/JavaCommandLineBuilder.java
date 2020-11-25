/*
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

package org.apache.hadoop.yarn.service.containerlaunch;


import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.apache.hadoop.yarn.service.exceptions.BadConfigException;

import java.util.Map;

/**
 * Command line builder purely for the Java CLI.
 * Some of the <code>define</code> methods are designed to work with Hadoop tool and
 * Slider launcher applications.
 */
public class JavaCommandLineBuilder extends CommandLineBuilder {

  public JavaCommandLineBuilder() {
    add(getJavaBinary());
  }

  /**
   * Get the java binary. This is called in the constructor so don't try and
   * do anything other than return a constant.
   * @return the path to the Java binary
   */
  protected String getJavaBinary() {
    return ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java";
  }

  /**
   * Set JVM opts.
   * @param jvmOpts JVM opts
   */
  public void setJVMOpts(String jvmOpts) {
    if (ServiceUtils.isSet(jvmOpts)) {
      add(jvmOpts);
    }
  }

  /**
   * Turn Java assertions on
   */
  public void enableJavaAssertions() {
    add("-ea");
    add("-esa");
  }

  /**
   * Add a system property definition -must be used before setting the main entry point
   * @param property
   * @param value
   */
  public void sysprop(String property, String value) {
    Preconditions.checkArgument(property != null, "null property name");
    Preconditions.checkArgument(value != null, "null value");
    add("-D" + property + "=" + value);
  }
  
  public JavaCommandLineBuilder forceIPv4() {
    sysprop("java.net.preferIPv4Stack", "true");
    return this;
  }
  
  public JavaCommandLineBuilder headless() {
    sysprop("java.awt.headless", "true");
    return this;
  }

  public boolean addConfOption(Configuration conf, String key) {
    return defineIfSet(key, conf.get(key));
  }

  /**
   * Add a varargs list of configuration parameters —if they are present
   * @param conf configuration source
   * @param keys keys
   */
  public void addConfOptions(Configuration conf, String... keys) {
    for (String key : keys) {
      addConfOption(conf, key);
    }
  }

  /**
   * Add all configuration options which match the prefix
   * @param conf configuration
   * @param prefix prefix, e.g {@code "slider."}
   * @return the number of entries copied
   */
  public int addPrefixedConfOptions(Configuration conf, String prefix) {
    int copied = 0;
    for (Map.Entry<String, String> entry : conf) {
      if (entry.getKey().startsWith(prefix)) {
        define(entry.getKey(), entry.getValue());
        copied++;
      }
    }
    return copied;
  }

  /**
   * Ass a configuration option to the command line of  the application
   * @param conf configuration
   * @param key key
   * @param defVal default value
   * @return the resolved configuration option
   * @throws IllegalArgumentException if key is null or the looked up value
   * is null (that is: the argument is missing and devVal was null.
   */
  public String addConfOptionToCLI(Configuration conf,
      String key,
      String defVal) {
    Preconditions.checkArgument(key != null, "null key");
    String val = conf.get(key, defVal);
    define(key, val);
    return val;
  }

  /**
   * Add a <code>-D key=val</code> command to the CLI. This is very Hadoop API
   * @param key key
   * @param val value
   * @throws IllegalArgumentException if either argument is null
   */
  public void define(String key, String val) {
    Preconditions.checkArgument(key != null, "null key");
    Preconditions.checkArgument(val != null, "null value");
    add("-D", key + "=" + val);
  }

  /**
   * Add a <code>-D key=val</code> command to the CLI if <code>val</code>
   * is not null
   * @param key key
   * @param val value
   */
  public boolean defineIfSet(String key, String val) {
    Preconditions.checkArgument(key != null, "null key");
    if (val != null) {
      define(key, val);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Add a mandatory config option
   * @param conf configuration
   * @param key key
   * @throws BadConfigException if the key is missing
   */
  public void addMandatoryConfOption(Configuration conf,
      String key) throws BadConfigException {
    if (!addConfOption(conf, key)) {
      throw new BadConfigException("Missing configuration option: " + key);
    }
  }

}

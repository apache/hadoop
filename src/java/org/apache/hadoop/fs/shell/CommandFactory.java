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

package org.apache.hadoop.fs.shell;

import java.util.Arrays;
import java.util.Hashtable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/** class to search for and register commands */

@InterfaceAudience.Private
@InterfaceStability.Unstable

public class CommandFactory extends Configured implements Configurable {
  private Hashtable<String, Class<? extends Command>> classMap =
    new Hashtable<String, Class<? extends Command>>();

  /** Factory constructor for commands */
  public CommandFactory() {
    this(null);
  }
  
  /**
   * Factory constructor for commands
   * @param conf the hadoop configuration
   */
  public CommandFactory(Configuration conf) {
    super(conf);
  }

  /**
   * Invokes "static void registerCommands(CommandFactory)" on the given class.
   * This method abstracts the contract between the factory and the command
   * class.  Do not assume that directly invoking registerCommands on the
   * given class will have the same effect.
   * @param registrarClass class to allow an opportunity to register
   */
  public void registerCommands(Class<?> registrarClass) {
    try {
      registrarClass.getMethod(
          "registerCommands", CommandFactory.class
      ).invoke(null, this);
    } catch (Exception e) {
      throw new RuntimeException(StringUtils.stringifyException(e));
    }
  }

  /**
   * Register the given class as handling the given list of command
   * names.
   * @param cmdClass the class implementing the command names
   * @param names one or more command names that will invoke this class
   */
  public void addClass(Class<? extends Command> cmdClass, String ... names) {
    for (String name : names) classMap.put(name, cmdClass);
  }
  
  /**
   * Returns the class implementing the given command.  The
   * class must have been registered via
   * {@link #addClass(Class, String...)}
   * @param cmd name of the command
   * @return instance of the requested command
   */
  protected Class<? extends Command> getClass(String cmd) {
    return classMap.get(cmd);
  }
  
  /**
   * Returns an instance of the class implementing the given command.  The
   * class must have been registered via
   * {@link #addClass(Class, String...)}
   * @param cmd name of the command
   * @return instance of the requested command
   */
  public Command getInstance(String cmd) {
    return getInstance(cmd, getConf());
  }

  /**
   * Get an instance of the requested command
   * @param cmdName name of the command to lookup
   * @param conf the hadoop configuration
   * @return the {@link Command} or null if the command is unknown
   */
  public Command getInstance(String cmdName, Configuration conf) {
    if (conf == null) throw new NullPointerException("configuration is null");
    
    Command instance = null;
    Class<? extends Command> cmdClass = getClass(cmdName);
    if (cmdClass != null) {
      instance = ReflectionUtils.newInstance(cmdClass, conf);
      instance.setCommandName(cmdName);
    }
    return instance;
  }
  
  /**
   * Gets all of the registered commands
   * @return a sorted list of command names
   */
  public String[] getNames() {
    String[] names = classMap.keySet().toArray(new String[0]);
    Arrays.sort(names);
    return names;
  }
}

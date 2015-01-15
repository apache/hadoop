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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/** class to search for and register commands */

@InterfaceAudience.Private
@InterfaceStability.Unstable

public class CommandFactory extends Configured {
  private Map<String, Class<? extends Command>> classMap =
    new HashMap<String, Class<? extends Command>>();

  private Map<String, Command> objectMap =
    new HashMap<String, Command>();

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
   * Register the given object as handling the given list of command
   * names.  Avoid calling this method and use
   * {@link #addClass(Class, String...)} whenever possible to avoid
   * startup overhead from excessive command object instantiations.  This
   * method is intended only for handling nested non-static classes that
   * are re-usable.  Namely -help/-usage.
   * @param cmdObject the object implementing the command names
   * @param names one or more command names that will invoke this class
   */
  public void addObject(Command cmdObject, String ... names) {
    for (String name : names) {
      objectMap.put(name, cmdObject);
      classMap.put(name, null); // just so it shows up in the list of commands
    }
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
    
    Command instance = objectMap.get(cmdName);
    if (instance == null) {
      Class<? extends Command> cmdClass = classMap.get(cmdName);
      if (cmdClass != null) {
        instance = ReflectionUtils.newInstance(cmdClass, conf);
        instance.setName(cmdName);
        instance.setCommandFactory(this);
      }
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

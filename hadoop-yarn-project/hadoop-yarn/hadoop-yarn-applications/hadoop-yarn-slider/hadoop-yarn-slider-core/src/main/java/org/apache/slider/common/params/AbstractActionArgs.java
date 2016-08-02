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

package org.apache.slider.common.params;

import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.UsageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Base args for all actions
 */
public abstract class AbstractActionArgs extends ArgOps implements Arguments {
  protected static final Logger log =
    LoggerFactory.getLogger(AbstractActionArgs.class);


  protected AbstractActionArgs() {
  }

  /**
   * URI/binding to the filesystem
   */
  @Parameter(names = {ARG_FILESYSTEM, ARG_FILESYSTEM_LONG},
             description = "Filesystem Binding")
  public String filesystemBinding;

  @Parameter(names = {ARG_BASE_PATH},
             description = "Slider base path on the filesystem",
             converter =  PathArgumentConverter.class)
  public Path basePath;

  /**
   * This is the default parameter
   */
  @Parameter
  public final List<String> parameters = new ArrayList<>();

  /**
   * get the name: relies on arg 1 being the cluster name in all operations 
   * @return the name argument, null if there is none
   */
  public String getClusterName() {
    return (parameters.isEmpty()) ? null : parameters.get(0);
  }

  /**
   -D name=value

   Define an HBase configuration option which overrides any options in
   the configuration XML files of the image or in the image configuration
   directory. The values will be persisted.
   Configuration options are only passed to the cluster when creating or reconfiguring a cluster.

   */

  @Parameter(names = ARG_DEFINE, arity = 1, description = "Definitions")
  public final List<String> definitions = new ArrayList<>();

  /**
   * System properties
   */
  @Parameter(names = {ARG_SYSPROP}, arity = 1,
             description = "system properties in the form name value" +
                           " These are set after the JVM is started.")
  public final List<String> sysprops = new ArrayList<>(0);


  @Parameter(names = {ARG_MANAGER_SHORT, ARG_MANAGER},
             description = "Binding (usually hostname:port) of the YARN resource manager")
  public String manager;


  @Parameter(names = ARG_DEBUG, description = "Debug mode")
  public boolean debug = false;

  @Parameter(names = {ARG_HELP}, description = "Help", help = true)
  public boolean help = false;

  /**
   * Get the min #of params expected
   * @return the min number of params in the {@link #parameters} field
   */
  public int getMinParams() {
    return 1;
  }

  /**
   * Get the name of the action
   * @return the action name
   */
  public abstract String getActionName() ;

  /**
   * Get the max #of params expected
   * @return the number of params in the {@link #parameters} field;
   */
  public int getMaxParams() {
    return getMinParams();
  }

  public void validate() throws BadCommandArgumentsException, UsageException {
    
    int minArgs = getMinParams();
    int actionArgSize = parameters.size();
    if (minArgs > actionArgSize) {
      throw new BadCommandArgumentsException(
        ErrorStrings.ERROR_NOT_ENOUGH_ARGUMENTS + getActionName() +
        " Expected minimum " + minArgs + " but got " + actionArgSize);
    }
    int maxArgs = getMaxParams();
    if (maxArgs == -1) {
      maxArgs = minArgs;
    }
    if (actionArgSize > maxArgs) {
      String message = String.format("%s for action %s: limit is %d but saw %d: ",
                                     ErrorStrings.ERROR_TOO_MANY_ARGUMENTS,
                                     getActionName(), maxArgs,
                                     actionArgSize);
      
      log.error(message);
      int index = 1;
      for (String actionArg : parameters) {
        log.error("[{}] \"{}\"", index++, actionArg);
        message += " \"" + actionArg + "\" ";
      }
      throw new BadCommandArgumentsException(message);
    }
  }

  @Override
  public String toString() {
    return super.toString() + ": " + getActionName();
  }

  /**
   * Override point: 
   * Flag to indicate that core hadoop API services are needed (HDFS, YARN, etc)
   * —and that validation of the client state should take place.
   * 
   * @return a flag to indicate that the core hadoop services will be needed.
   */
  public boolean getHadoopServicesRequired() {
    return true;
  }

  /**
   * Flag to disable secure login.
   * This MUST only be set if the action is bypassing security or setting
   * it itself
   * @return true if login at slider client init time is to be skipped
   */
  public boolean disableSecureLogin() {
    return false;
  }
}

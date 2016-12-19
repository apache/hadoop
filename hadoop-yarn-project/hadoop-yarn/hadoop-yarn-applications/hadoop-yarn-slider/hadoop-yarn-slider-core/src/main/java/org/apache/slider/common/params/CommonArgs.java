/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.common.params;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.ParameterException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.UsageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains the common argument set for all tne entry points,
 * and the core parsing logic to verify that the action is on the list
 * of allowed actions -and that the remaining number of arguments is
 * in the range allowed
 */

public abstract class CommonArgs extends ArgOps implements SliderActions,
                                                           Arguments {

  protected static final Logger log = LoggerFactory.getLogger(CommonArgs.class);


  private static final int DIFF_BETWEEN_DESCIPTION_AND_COMMAND_NAME = 30;


  @Parameter(names = ARG_HELP, help = true)
  public boolean help;


  /**
   -D name=value

   Define an HBase configuration option which overrides any options in
   the configuration XML files of the image or in the image configuration
   directory. The values will be persisted.
   Configuration options are only passed to the cluster when creating or reconfiguring a cluster.

   */

  public Map<String, String> definitionMap = new HashMap<String, String>();
  /**
   * System properties
   */
  public Map<String, String> syspropsMap = new HashMap<String, String>();


  /**
   * fields
   */
  public final JCommander commander;
  private final String[] args;

  private AbstractActionArgs coreAction;

  /**
   * get the name: relies on arg 1 being the cluster name in all operations 
   * @return the name argument, null if there is none
   */
  public String getClusterName() {
    return coreAction.getClusterName();
  }

  protected CommonArgs(String[] args) {
    this.args = args;
    commander = new JCommander(this);
  }

  protected CommonArgs(Collection args) {
    List<String> argsAsStrings = SliderUtils.collectionToStringList(args);
    this.args = argsAsStrings.toArray(new String[argsAsStrings.size()]);
    commander = new JCommander(this);
  }

  public String usage() {
    return usage(this, null);
  }

  public static String usage(CommonArgs serviceArgs, String commandOfInterest) {
    String result = null;
    StringBuilder helperMessage = new StringBuilder();
    if (commandOfInterest == null) {
      // JCommander.usage is too verbose for a command with many options like
      // slider no short version of that is found Instead, we compose our msg by
      helperMessage.append("\nUsage: slider COMMAND [options]\n");
      helperMessage.append("where COMMAND is one of\n");
      for (String jcommand : serviceArgs.commander.getCommands().keySet()) {
        helperMessage.append(String.format("\t%-"
            + DIFF_BETWEEN_DESCIPTION_AND_COMMAND_NAME + "s%s", jcommand,
            serviceArgs.commander.getCommandDescription(jcommand) + "\n"));
      }
      helperMessage
          .append("Most commands print help when invoked without parameters or with --help");
      result = helperMessage.toString();
    } else {
      helperMessage.append("\nUsage: slider ").append(commandOfInterest);
      helperMessage.append(serviceArgs.coreAction.getMinParams() > 0 ? " <application>" : "");
      helperMessage.append("\n");
      for (ParameterDescription paramDesc : serviceArgs.commander.getCommands()
          .get(commandOfInterest).getParameters()) {
        String optional = paramDesc.getParameter().required() ? "  (required)"
            : "  (optional)";
        String paramName = paramDesc.getParameterized().getType() == Boolean.TYPE ? paramDesc
            .getLongestName() : paramDesc.getLongestName() + " <"
            + paramDesc.getParameterized().getName() + ">";
        helperMessage.append(String.format("\t%-"
            + DIFF_BETWEEN_DESCIPTION_AND_COMMAND_NAME + "s%s", paramName,
            paramDesc.getDescription() + optional + "\n"));
        result = helperMessage.toString();
      }
    }
    return result;
  }

  public static String usage(CommonArgs serviceArgs) {
    return usage(serviceArgs, null);
  }

  /**
   * Parse routine -includes registering the action-specific argument classes
   * and postprocess it
   * @throws SliderException on any problem
   */
  public void parse() throws SliderException {
    addActionArguments();
    try {
      commander.parse(args);
    } catch (ParameterException e) {
      throw new BadCommandArgumentsException(e, "%s in %s",
                                             e.toString(),
                                             (args != null
                                              ? (SliderUtils.join(args,
                                                 " ", false))
                                              : "[]"));
    }
    //now copy back to this class some of the attributes that are common to all
    //actions
    postProcess();
  }

  /**
   * Add a command
   * @param name action
   * @param arg value
   */
  protected void addAction(String name, Object arg) {
    commander.addCommand(name, arg);
  }

  protected void addActions(Object... actions) {
    for (Object action : actions) {
      commander.addCommand(action);
    }
  }

  /**
   * Override point to add a set of actions
   */
  protected void addActionArguments() {

  }

  /**
   * validate args via {@link #validate()}
   * then postprocess the arguments
   */
  public void postProcess() throws SliderException {
    applyAction();
    validate();

    //apply entry set
    for (Map.Entry<String, String> entry : syspropsMap.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
  }


  /**
   * Implementors must implement their action apply routine here
   */
  public abstract void applyAction() throws SliderException;


  /**
   * Bind the core action; this extracts any attributes that are used
   * across routines
   * @param action action to bind
   */
  protected void bindCoreAction(AbstractActionArgs action) {
    coreAction = action;

    splitPairs(coreAction.definitions, definitionMap);
    splitPairs(coreAction.sysprops, syspropsMap);
  }

  /**
   * Get the core action -type depends on the action
   * @return the action class
   */
  public AbstractActionArgs getCoreAction() {
    return coreAction;
  }

  /**
   * Validate the arguments against the action requested
   */
  public void validate() throws BadCommandArgumentsException, UsageException {
    if (coreAction == null) {
      throw new UsageException(ErrorStrings.ERROR_NO_ACTION + usage());
    }
    log.debug("action={}", getAction());
    // let the action validate itself
    try {
      coreAction.validate();
    } catch (BadCommandArgumentsException e) {
      StringBuilder badArgMsgBuilder = new StringBuilder();
      badArgMsgBuilder.append(e.toString()).append("\n");
      badArgMsgBuilder.append(usage(this, coreAction.getActionName()));
      throw new BadCommandArgumentsException(badArgMsgBuilder.toString());
    } catch (UsageException e) {
      StringBuilder badArgMsgBuilder = new StringBuilder();
      badArgMsgBuilder.append(e.toString()).append("\n");
      badArgMsgBuilder.append(usage(this, coreAction.getActionName()));
      throw new UsageException(badArgMsgBuilder.toString());
    }
  }

  /**
   * Apply all the definitions on the command line to the configuration
   * @param conf config
   */
  public void applyDefinitions(Configuration conf) throws
                                                   BadCommandArgumentsException {
    applyDefinitions(definitionMap, conf);
  }


  /**
   * If the Filesystem binding was provided, it overrides anything in
   * the configuration
   * @param conf configuration
   */
  public void applyFileSystemBinding(Configuration conf) {
    ArgOps.applyFileSystemBinding(getFilesystemBinding(), conf);
  }

  public boolean isDebug() {
    return coreAction.debug;
  }


  public String getFilesystemBinding() {
    return coreAction.filesystemBinding;
  }

  public Path getBasePath() { return coreAction.basePath; }

  public String getManager() {
    return coreAction.manager;
  }

  public String getAction() {
    return commander.getParsedCommand();
  }

  public List<String> getActionArgs() {
    return coreAction.parameters;
  }

}

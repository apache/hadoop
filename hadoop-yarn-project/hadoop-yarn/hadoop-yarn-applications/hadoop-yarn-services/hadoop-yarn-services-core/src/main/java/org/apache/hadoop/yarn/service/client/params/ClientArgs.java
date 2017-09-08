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

package org.apache.hadoop.yarn.service.client.params;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.utils.SliderUtils;
import org.apache.hadoop.yarn.service.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.yarn.service.exceptions.ErrorStrings;
import org.apache.hadoop.yarn.service.exceptions.SliderException;

import java.util.Collection;

/**
 * Client CLI Args
 */

public class ClientArgs extends CommonArgs {

  // =========================================================
  // Keep all of these in alphabetical order. Thanks.
  // =========================================================

  private final ActionBuildArgs actionBuildArgs = new ActionBuildArgs();
  private final ActionClientArgs actionClientArgs = new ActionClientArgs();
  private final ActionCreateArgs actionCreateArgs = new ActionCreateArgs();
  private final ActionDependencyArgs actionDependencyArgs = new ActionDependencyArgs();
  private final ActionDestroyArgs actionDestroyArgs = new ActionDestroyArgs();
  private final ActionExistsArgs actionExistsArgs = new ActionExistsArgs();
  private final ActionFlexArgs actionFlexArgs = new ActionFlexArgs();
  private final ActionFreezeArgs actionFreezeArgs = new ActionFreezeArgs();
  private final ActionHelpArgs actionHelpArgs = new ActionHelpArgs();
  private final ActionKeytabArgs actionKeytabArgs = new ActionKeytabArgs();
  private final ActionListArgs actionListArgs = new ActionListArgs();
  private final ActionRegistryArgs actionRegistryArgs = new ActionRegistryArgs();
  private final ActionResolveArgs actionResolveArgs = new ActionResolveArgs();
  private final ActionResourceArgs actionResourceArgs = new ActionResourceArgs();
  private final ActionStatusArgs actionStatusArgs = new ActionStatusArgs();
  private final ActionThawArgs actionThawArgs = new ActionThawArgs();
  private final ActionTokensArgs actionTokenArgs = new ActionTokensArgs();
  private final ActionUpdateArgs actionUpdateArgs = new ActionUpdateArgs();

  public ClientArgs(String[] args) {
    super(args);
  }

  public ClientArgs(Collection args) {
    super(args);
  }

  @Override
  protected void addActionArguments() {

    addActions(
        actionBuildArgs,
        actionCreateArgs,
        actionDependencyArgs,
        actionDestroyArgs,
        actionFlexArgs,
        actionFreezeArgs,
        actionHelpArgs,
        actionStatusArgs,
        actionThawArgs
    );
  }

  @Override
  public void applyDefinitions(Configuration conf) throws
                                                   BadCommandArgumentsException {
    super.applyDefinitions(conf);
  }


  public ActionBuildArgs getActionBuildArgs() {
    return actionBuildArgs;
  }

  public ActionUpdateArgs getActionUpdateArgs() {
    return actionUpdateArgs;
  }

  public ActionCreateArgs getActionCreateArgs() {
    return actionCreateArgs;
  }

  public ActionDependencyArgs getActionDependencyArgs() {
    return actionDependencyArgs;
  }

  public ActionDestroyArgs getActionDestroyArgs() {
    return actionDestroyArgs;
  }

  public ActionExistsArgs getActionExistsArgs() {
    return actionExistsArgs;
  }

  public ActionFlexArgs getActionFlexArgs() {
    return actionFlexArgs;
  }

  public ActionFreezeArgs getActionFreezeArgs() {
    return actionFreezeArgs;
  }

  public ActionListArgs getActionListArgs() {
    return actionListArgs;
  }


  public ActionRegistryArgs getActionRegistryArgs() {
    return actionRegistryArgs;
  }

  public ActionResolveArgs getActionResolveArgs() {
    return actionResolveArgs;
  }

  public ActionResourceArgs getActionResourceArgs() {
    return actionResourceArgs;
  }

  public ActionStatusArgs getActionStatusArgs() {
    return actionStatusArgs;
  }

  public ActionThawArgs getActionThawArgs() {
    return actionThawArgs;
  }

  public ActionTokensArgs getActionTokenArgs() {
    return actionTokenArgs;
  }

  /**
   * Look at the chosen action and bind it as the core action for the operation.
   * @throws SliderException bad argument or similar
   */
  @Override
  public void applyAction() throws SliderException {
    String action = getAction();
    if (SliderUtils.isUnset(action)) {
      action = ACTION_HELP;
    }
    switch (action) {
      case ACTION_BUILD:
        bindCoreAction(actionBuildArgs);
        break;

      case ACTION_CREATE:
        bindCoreAction(actionCreateArgs);
        break;

      case ACTION_STOP:
        bindCoreAction(actionFreezeArgs);
        break;

      case ACTION_START:
        bindCoreAction(actionThawArgs);
        break;

      case ACTION_DEPENDENCY:
        bindCoreAction(actionDependencyArgs);
        break;

      case ACTION_DESTROY:
        bindCoreAction(actionDestroyArgs);
        break;

      case ACTION_EXISTS:
        bindCoreAction(actionExistsArgs);
        break;

      case ACTION_FLEX:
        bindCoreAction(actionFlexArgs);
        break;

      case ACTION_HELP:
        bindCoreAction(actionHelpArgs);
        break;

      case ACTION_KEYTAB:
        bindCoreAction(actionKeytabArgs);
        break;

      case ACTION_LIST:
        bindCoreAction(actionListArgs);
        break;

      case ACTION_REGISTRY:
        bindCoreAction(actionRegistryArgs);
        break;

      case ACTION_RESOLVE:
        bindCoreAction(actionResolveArgs);
        break;

      case ACTION_RESOURCE:
        bindCoreAction(actionResourceArgs);
        break;

      case ACTION_STATUS:
        bindCoreAction(actionStatusArgs);
        break;

      case ACTION_TOKENS:
        bindCoreAction(actionTokenArgs);
        break;

      case ACTION_UPDATE:
        bindCoreAction(actionUpdateArgs);
        break;

      default:
        throw new BadCommandArgumentsException(ErrorStrings.ERROR_UNKNOWN_ACTION
        + " " + action);
    }
  }

}

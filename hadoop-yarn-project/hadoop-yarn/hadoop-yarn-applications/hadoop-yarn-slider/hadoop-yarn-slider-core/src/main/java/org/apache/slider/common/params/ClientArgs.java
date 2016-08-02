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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.SliderException;

import java.util.Collection;

/**
 * Slider Client CLI Args
 */

public class ClientArgs extends CommonArgs {

  /*
   
   All the arguments for specific actions
  
   */
  /**
   * This is not bonded to jcommander, it is set up
   * after the construction to point to the relevant
   * entry
   * 
   * KEEP IN ALPHABETICAL ORDER
   */
  private AbstractClusterBuildingActionArgs buildingActionArgs;

  // =========================================================
  // Keep all of these in alphabetical order. Thanks.
  // =========================================================

  private final ActionAMSuicideArgs actionAMSuicideArgs = new ActionAMSuicideArgs();
  private final ActionBuildArgs actionBuildArgs = new ActionBuildArgs();
  private final ActionClientArgs actionClientArgs = new ActionClientArgs();
  private final ActionCreateArgs actionCreateArgs = new ActionCreateArgs();
  private final ActionDependencyArgs actionDependencyArgs = new ActionDependencyArgs();
  private final ActionDestroyArgs actionDestroyArgs = new ActionDestroyArgs();
  private final ActionDiagnosticArgs actionDiagnosticArgs = new ActionDiagnosticArgs();
  private final ActionExistsArgs actionExistsArgs = new ActionExistsArgs();
  private final ActionFlexArgs actionFlexArgs = new ActionFlexArgs();
  private final ActionFreezeArgs actionFreezeArgs = new ActionFreezeArgs();
  private final ActionHelpArgs actionHelpArgs = new ActionHelpArgs();
  private final ActionInstallPackageArgs actionInstallPackageArgs = new ActionInstallPackageArgs();
  private final ActionInstallKeytabArgs actionInstallKeytabArgs = new ActionInstallKeytabArgs();
  private final ActionKDiagArgs actionKDiagArgs = new ActionKDiagArgs();
  private final ActionKeytabArgs actionKeytabArgs = new ActionKeytabArgs();
  private final ActionKillContainerArgs actionKillContainerArgs =
    new ActionKillContainerArgs();
  private final ActionListArgs actionListArgs = new ActionListArgs();
  private final ActionLookupArgs actionLookupArgs = new ActionLookupArgs();
  private final ActionNodesArgs actionNodesArgs = new ActionNodesArgs();
  private final ActionPackageArgs actionPackageArgs = new ActionPackageArgs();
  private final ActionRegistryArgs actionRegistryArgs = new ActionRegistryArgs();
  private final ActionResolveArgs actionResolveArgs = new ActionResolveArgs();
  private final ActionResourceArgs actionResourceArgs = new ActionResourceArgs();
  private final ActionStatusArgs actionStatusArgs = new ActionStatusArgs();
  private final ActionThawArgs actionThawArgs = new ActionThawArgs();
  private final ActionTokensArgs actionTokenArgs = new ActionTokensArgs();
  private final ActionUpdateArgs actionUpdateArgs = new ActionUpdateArgs();
  private final ActionUpgradeArgs actionUpgradeArgs = new ActionUpgradeArgs();
  private final ActionVersionArgs actionVersionArgs = new ActionVersionArgs();

  public ClientArgs(String[] args) {
    super(args);
  }

  public ClientArgs(Collection args) {
    super(args);
  }

  @Override
  protected void addActionArguments() {

    addActions(
        actionAMSuicideArgs,
        actionBuildArgs,
        actionClientArgs,
        actionCreateArgs,
        actionDependencyArgs,
        actionDestroyArgs,
        actionDiagnosticArgs,
        actionExistsArgs,
        actionFlexArgs,
        actionFreezeArgs,
        actionHelpArgs,
        actionInstallKeytabArgs,
        actionInstallPackageArgs,
        actionKDiagArgs,
        actionKeytabArgs,
        actionKillContainerArgs,
        actionListArgs,
        actionLookupArgs,
        actionNodesArgs,
        actionPackageArgs,
        actionRegistryArgs,
        actionResolveArgs,
        actionResourceArgs,
        actionStatusArgs,
        actionThawArgs,
        actionTokenArgs,
        actionUpdateArgs,
        actionUpgradeArgs,
        actionVersionArgs
    );
  }

  @Override
  public void applyDefinitions(Configuration conf) throws
                                                   BadCommandArgumentsException {
    super.applyDefinitions(conf);
    //RM
    if (getManager() != null) {
      log.debug("Setting RM to {}", getManager());
      conf.set(YarnConfiguration.RM_ADDRESS, getManager());
    }
    if (getBasePath() != null) {
      log.debug("Setting basePath to {}", getBasePath());
      conf.set(SliderXmlConfKeys.KEY_SLIDER_BASE_PATH,
          getBasePath().toString());
    }
  }

  public ActionDiagnosticArgs getActionDiagnosticArgs() {
	  return actionDiagnosticArgs;
  }

  public AbstractClusterBuildingActionArgs getBuildingActionArgs() {
    return buildingActionArgs;
  }

  public ActionAMSuicideArgs getActionAMSuicideArgs() {
    return actionAMSuicideArgs;
  }

  public ActionBuildArgs getActionBuildArgs() {
    return actionBuildArgs;
  }

  public ActionInstallPackageArgs getActionInstallPackageArgs() { return actionInstallPackageArgs; }

  public ActionClientArgs getActionClientArgs() { return actionClientArgs; }

  public ActionPackageArgs getActionPackageArgs() { return actionPackageArgs; }

  public ActionInstallKeytabArgs getActionInstallKeytabArgs() { return actionInstallKeytabArgs; }

  public ActionKDiagArgs getActionKDiagArgs() {
    return actionKDiagArgs;
  }

  public ActionKeytabArgs getActionKeytabArgs() { return actionKeytabArgs; }

  public ActionUpdateArgs getActionUpdateArgs() {
    return actionUpdateArgs;
  }

  public ActionUpgradeArgs getActionUpgradeArgs() {
    return actionUpgradeArgs;
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

  public ActionKillContainerArgs getActionKillContainerArgs() {
    return actionKillContainerArgs;
  }

  public ActionListArgs getActionListArgs() {
    return actionListArgs;
  }

  public ActionNodesArgs getActionNodesArgs() {
    return actionNodesArgs;
  }

  public ActionLookupArgs getActionLookupArgs() {
    return actionLookupArgs;
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
        //its a builder, so set those actions too
        buildingActionArgs = actionBuildArgs;
        break;

      case ACTION_CREATE:
        bindCoreAction(actionCreateArgs);
        //its a builder, so set those actions too
        buildingActionArgs = actionCreateArgs;
        break;

      case ACTION_FREEZE:
        bindCoreAction(actionFreezeArgs);
        break;

      case ACTION_THAW:
        bindCoreAction(actionThawArgs);
        break;

      case ACTION_AM_SUICIDE:
        bindCoreAction(actionAMSuicideArgs);
        break;

      case ACTION_CLIENT:
        bindCoreAction(actionClientArgs);
        break;

      case ACTION_DEPENDENCY:
        bindCoreAction(actionDependencyArgs);
        break;

      case ACTION_DESTROY:
        bindCoreAction(actionDestroyArgs);
        break;

      case ACTION_DIAGNOSTICS:
        bindCoreAction(actionDiagnosticArgs);
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

      case ACTION_INSTALL_KEYTAB:
        bindCoreAction(actionInstallKeytabArgs);
        break;

      case ACTION_INSTALL_PACKAGE:
        bindCoreAction(actionInstallPackageArgs);
        break;

      case ACTION_KDIAG:
        bindCoreAction(actionKDiagArgs);
        break;

      case ACTION_KEYTAB:
        bindCoreAction(actionKeytabArgs);
        break;

      case ACTION_KILL_CONTAINER:
        bindCoreAction(actionKillContainerArgs);
        break;

      case ACTION_LIST:
        bindCoreAction(actionListArgs);
        break;

      case ACTION_LOOKUP:
        bindCoreAction(actionLookupArgs);
        break;

      case ACTION_NODES:
        bindCoreAction(actionNodesArgs);
        break;

      case ACTION_PACKAGE:
        bindCoreAction(actionPackageArgs);
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

      case ACTION_UPGRADE:
        bindCoreAction(actionUpgradeArgs);
        break;

      case ACTION_VERSION:
        bindCoreAction(actionVersionArgs);
        break;

      default:
        throw new BadCommandArgumentsException(ErrorStrings.ERROR_UNKNOWN_ACTION
        + " " + action);
    }
  }

}

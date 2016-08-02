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

package org.apache.slider.providers.agent;

import org.apache.slider.providers.agent.application.metadata.CommandOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stores the command dependency order for all components in a service. <commandOrder>
 * <command>SUPERVISOR-START</command> <requires>NIMBUS-STARTED</requires> </commandOrder> Means, SUPERVISOR START
 * requires NIMBUS to be STARTED
 */
public class ComponentCommandOrder {
  public static final Logger log =
      LoggerFactory.getLogger(ComponentCommandOrder.class);
  private static char SPLIT_CHAR = '-';
  Map<Command, Map<String, List<ComponentState>>> dependencies =
      new HashMap<Command, Map<String, List<ComponentState>>>();

  public ComponentCommandOrder(List<CommandOrder> commandOrders) {
    if (commandOrders != null && commandOrders.size() > 0) {
      for (CommandOrder commandOrder : commandOrders) {
        ComponentCommand componentCmd = getComponentCommand(commandOrder.getCommand());
        String requires = commandOrder.getRequires();
        List<ComponentState> requiredStates = parseRequiredStates(requires);
        if (requiredStates.size() > 0) {
          Map<String, List<ComponentState>> compDep = dependencies.get(componentCmd.command);
          if (compDep == null) {
            compDep = new HashMap<>();
            dependencies.put(componentCmd.command, compDep);
          }

          List<ComponentState> requirements = compDep.get(componentCmd.componentName);
          if (requirements == null) {
            requirements = new ArrayList<>();
            compDep.put(componentCmd.componentName, requirements);
          }

          requirements.addAll(requiredStates);
        }
      }
    }
  }

  private List<ComponentState> parseRequiredStates(String requires) {
    if (requires == null || requires.length() < 2) {
      throw new IllegalArgumentException("Input cannot be null and must contain component and state.");
    }

    String[] componentStates = requires.split(",");
    List<ComponentState> retList = new ArrayList<ComponentState>();
    for (String componentStateStr : componentStates) {
      retList.add(getComponentState(componentStateStr));
    }

    return retList;
  }

  private ComponentCommand getComponentCommand(String compCmdStr) {
    if (compCmdStr == null || compCmdStr.trim().length() < 2) {
      throw new IllegalArgumentException("Input cannot be null and must contain component and command.");
    }

    compCmdStr = compCmdStr.trim();
    int splitIndex = compCmdStr.lastIndexOf(SPLIT_CHAR);
    if (splitIndex == -1 || splitIndex == 0 || splitIndex == compCmdStr.length() - 1) {
      throw new IllegalArgumentException("Input does not appear to be well-formed.");
    }
    String compStr = compCmdStr.substring(0, splitIndex);
    String cmdStr = compCmdStr.substring(splitIndex + 1);

    Command cmd = Command.valueOf(cmdStr);

    if (cmd != Command.START) {
      throw new IllegalArgumentException("Dependency order can only be specified for START.");
    }
    return new ComponentCommand(compStr, cmd);
  }

  private ComponentState getComponentState(String compStStr) {
    if (compStStr == null || compStStr.trim().length() < 2) {
      throw new IllegalArgumentException("Input cannot be null.");
    }

    compStStr = compStStr.trim();
    int splitIndex = compStStr.lastIndexOf(SPLIT_CHAR);
    if (splitIndex == -1 || splitIndex == 0 || splitIndex == compStStr.length() - 1) {
      throw new IllegalArgumentException("Input does not appear to be well-formed.");
    }
    String compStr = compStStr.substring(0, splitIndex);
    String stateStr = compStStr.substring(splitIndex + 1);

    State state = State.valueOf(stateStr);
    if (state != State.STARTED && state != State.INSTALLED) {
      throw new IllegalArgumentException("Dependency order can only be specified against STARTED/INSTALLED.");
    }
    return new ComponentState(compStr, state);
  }

  // dependency is still on component level, but not package level
  // so use component name to check dependency, not component-package
  public boolean canExecute(String component, Command command, Collection<ComponentInstanceState> currentStates) {
    boolean canExecute = true;
    if (dependencies.containsKey(command) && dependencies.get(command).containsKey(component)) {
      List<ComponentState> required = dependencies.get(command).get(component);
      for (ComponentState stateToMatch : required) {
        for (ComponentInstanceState currState : currentStates) {
          log.debug("Checking schedule {} {} against dependency {} is {}",
                    component, command, currState.getComponentName(), currState.getState());
          if (currState.getComponentName().equals(stateToMatch.componentName)) {
            if (currState.getState() != stateToMatch.state) {
              if (stateToMatch.state == State.STARTED) {
                log.info("Cannot schedule {} {} as dependency {} is {}",
                         component, command, currState.getComponentName(), currState.getState());
                canExecute = false;
              } else {
                //state is INSTALLED
                if (currState.getState() != State.STARTING && currState.getState() != State.STARTED) {
                  log.info("Cannot schedule {} {} as dependency {} is {}",
                           component, command, currState.getComponentName(), currState.getState());
                  canExecute = false;
                }
              }
            }
          }
          if (!canExecute) {
            break;
          }
        }
        if (!canExecute) {
          break;
        }
      }
    }

    return canExecute;
  }

  static class ComponentState {
    public String componentName;
    public State state;

    public ComponentState(String componentName, State state) {
      this.componentName = componentName;
      this.state = state;
    }
  }

  static class ComponentCommand {
    public String componentName;
    public Command command;

    public ComponentCommand(String componentName, Command command) {
      this.componentName = componentName;
      this.command = command;
    }
  }
}

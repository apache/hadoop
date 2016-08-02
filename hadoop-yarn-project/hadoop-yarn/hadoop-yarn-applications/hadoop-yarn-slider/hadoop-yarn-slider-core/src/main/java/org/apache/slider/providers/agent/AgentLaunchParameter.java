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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

class AgentLaunchParameter {
  public static final Logger log =
      LoggerFactory.getLogger(AgentLaunchParameter.class);
  private static final String DEFAULT_PARAMETER = "";
  private static final String ANY_COMPONENT = "ANY";
  private static final String NONE_VALUE = "NONE";
  private final Map<String, CommandTracker> launchParameterTracker;

  public AgentLaunchParameter(String parameters) {
    launchParameterTracker = parseExpectedLaunchParameters(parameters);
  }

  /**
   * Get command for the component type
   *
   * @param componentGroup
   *
   * @return
   */
  public String getNextLaunchParameter(String componentGroup) {
    if (launchParameterTracker != null) {
      if (launchParameterTracker.containsKey(componentGroup)
          || launchParameterTracker.containsKey(ANY_COMPONENT)) {
        synchronized (this) {
          CommandTracker indexTracker = null;
          if (launchParameterTracker.containsKey(componentGroup)) {
            indexTracker = launchParameterTracker.get(componentGroup);
          } else {
            indexTracker = launchParameterTracker.get(ANY_COMPONENT);
          }

          return indexTracker.getNextCommand();
        }
      }
    }

    return DEFAULT_PARAMETER;
  }

  /**
   * Parse launch parameters of the form ANY:PARAM_FOR_FIRST:PARAM_FOR_SECOND:...:PARAM_FOR_REST|HBASE_MASTER:...
   *
   * E.g. ANY:DO_NOT_REGISTER:DO_NOT_HEARTBEAT:NONE For any container, first one gets DO_NOT_REGISTER second one gets
   * DO_NOT_HEARTBEAT, then all of the rest get nothing
   *
   * E.g. HBASE_MASTER:FAIL_AFTER_START:NONE For HBASE_MASTER, first one gets FAIL_AFTER_START then "" for all
   *
   * @param launchParameters
   *
   * @return
   */
  Map<String, CommandTracker> parseExpectedLaunchParameters(String launchParameters) {
    Map<String, CommandTracker> trackers = null;
    if (launchParameters != null && launchParameters.length() > 0) {
      String[] componentSpecificParameters = launchParameters.split(Pattern.quote("|"));
      for (String componentSpecificParameter : componentSpecificParameters) {
        if (componentSpecificParameter.length() != 0) {
          String[] parameters = componentSpecificParameter.split(Pattern.quote(":"));

          if (parameters.length > 1 && parameters[0].length() > 0) {

            for (int index = 1; index < parameters.length; index++) {
              if (parameters[index].equals(NONE_VALUE)) {
                parameters[index] = DEFAULT_PARAMETER;
              }
            }

            if (trackers == null) {
              trackers = new HashMap<String, CommandTracker>(10);
            }
            String componentName = parameters[0];
            CommandTracker tracker = new CommandTracker(Arrays.copyOfRange(parameters, 1, parameters.length));
            trackers.put(componentName, tracker);
          }
        }
      }
    }

    return trackers;
  }

  class CommandTracker {
    private final int maxIndex;
    private final String[] launchCommands;
    private int currentIndex;

    CommandTracker(String[] launchCommands) {
      this.currentIndex = 0;
      this.maxIndex = launchCommands.length - 1;
      this.launchCommands = launchCommands;
    }

    String getNextCommand() {
      String retVal = launchCommands[currentIndex];
      if (currentIndex != maxIndex) {
        currentIndex++;
      }

      return retVal;
    }
  }
}

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
package org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;

import java.util.List;

public class MappingRuleConditionalVariables {
  /**
   * Utility class, hiding constructor.
   */
  private MappingRuleConditionalVariables() {}

  /**
   * SecondaryGroupVariable represents a conditional variable which is supposed
   * to evaluate path parts with "%secondary_group". The evaluation depends on
   * if parent path is provided.
   * If there was no parent path provided, the %secondary_group variable will be
   * equal to the first non-primary group of the user which has a matching queue
   * in the queue hierarchy. This means the queue name must be disambiguous as
   * well.
   * If there is a parent provided (the %secondary_group variable is not the
   * first element in the path), the %secondary_group variable will be
   * equal to the first non-primary group of the user which has a matching queue
   * UNDER the parent path. The parent path must be a full path, to avoid
   * ambiguity problems.
   */
  public static class SecondaryGroupVariable implements
      MappingRuleConditionalVariable {
    /**
     * This is the name of the variable we are replacing.
     */
    public final static String VARIABLE_NAME = "%secondary_group";

    /**
     * We need an instance of queue manager in order to look for queues under
     * the parent path.
     */
    private CapacitySchedulerQueueManager queueManager;
    /**
     * We store the potential secondary_groups candidates in this list, it must
     * not contain the primary group.
     */
    private List<String> potentialGroups;

    /**
     * Constructor requires a queue manager instance and a list of potential
     * secondary groups.
     * @param qm The queue manager which will be used to check which potential
     *           secondary group should be used.
     * @param groups List of potential secondary groups.
     */
    public SecondaryGroupVariable(CapacitySchedulerQueueManager qm,
        List<String> groups) {
      queueManager = qm;
      potentialGroups = groups;
    }

    /**
     * Method used to evaluate the variable when used in a path.
     * @param parts Split representation of the path.
     * @param currentIndex The index of the evaluation in the path. This shows
     *                     which part is currently being evaluated.
     * @return Substituted queue path part, this method will only return the
     * value of the conditional variable, not the whole path.
     */
    public String evaluateInPath(String[] parts, int currentIndex) {
      //First we need to determine the parent path (if any)
      StringBuilder parentBuilder = new StringBuilder();
      //Building the parent prefix, if we don't have any parent path
      //in case of currentIndex == 0 we will have an empty prefix.
      for (int i = 0; i < currentIndex; i++) {
        parentBuilder.append(parts[i]);
        //Generally this is not a good idea, we would need a condition, to not
        //append a '.' after the last part, however we are generating parent
        //prefix paths, so we need paths prefixes, like 'root.group.something.'
        parentBuilder.append(".");
      }

      //We'll use this prefix to lookup the groups, when we have a parent
      //provided we need to find a queue under that parent, which matches the
      //name of the secondaryGroup, if we don't have a parent the prefix is
      //empty
      String lookupPrefix = parentBuilder.toString();

      //Going through the potential groups to check if there is a matching queue
      for (String group : potentialGroups) {
        String path = lookupPrefix + group;
        if (queueManager.getQueue(path) != null) {
          return group;
        }
      }

      //No valid group found
      return "";
    }

    @Override
    public String toString() {
      return "SecondaryGroupVariable{" +
          "variableName='" + VARIABLE_NAME + "'," +
          "groups=" + potentialGroups +
          "}";
    }
  }

}

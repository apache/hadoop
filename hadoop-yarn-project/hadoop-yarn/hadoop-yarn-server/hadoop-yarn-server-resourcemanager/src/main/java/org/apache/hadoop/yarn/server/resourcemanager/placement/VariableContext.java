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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleConditionalVariable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class is a key-value store for the variables and their respective values
 * during an application placement. The class gives support for immutable
 * variables, which can be set only once, and has helper methods for replacing
 * the variables with their respective values in provided strings.
 * We don't extend the map interface, because we don't need all the features
 * a map provides, this class tries to be as simple as possible.
 */
public class VariableContext {
  /**
   * This is our actual variable store.
   */
  private Map<String, String> variables = new HashMap<>();
  private Map<String, String> originalVariables = new HashMap<>();

  /**
   * This is our conditional variable store.
   */
  private Map<String, MappingRuleConditionalVariable> conditionalVariables =
      new HashMap<>();

  /**
   * This set contains the names of the immutable variables if null it is
   * ignored.
   */
  private Set<String> immutableNames;

  /**
   * Some matchers may need to find a data in a set, which is not usable
   * as a variable in substitutions, this store is for those sets.
   */
  private Map<String, Set<String>> extraDataset = new HashMap<>();

  /**
   * Checks if the provided variable is immutable.
   * @param name Name of the variable to check
   * @return true if the variable is immutable
   */
  public boolean isImmutable(String name) {
    return (immutableNames != null && immutableNames.contains(name));
  }

  /**
   * Can be used to provide a set which contains the name of the variables which
   * should be immutable.
   * @param variableNames Set containing the names of the immutable variables
   * @throws IllegalStateException if the immutable set is already provided.
   * @return same instance of VariableContext for daisy chaining.
   */
  public VariableContext setImmutables(Set<String> variableNames) {
    if (this.immutableNames != null) {
      throw new IllegalStateException("Immutable variables are already defined,"
          + " variable immutability cannot be changed once set!");
    }
    this.immutableNames = ImmutableSet.copyOf(variableNames);
    return this;
  }

  /**
   * Can be used to provide an array of strings which contains the names of the
   * variables which should be immutable. An immutable set will be created
   * from the array.
   * @param variableNames Set containing the names of the immutable variables
   * @throws IllegalStateException if the immutable set is already provided.
   * @return same instance of VariableContext for daisy chaining.
   */
  public VariableContext setImmutables(String... variableNames) {
    if (this.immutableNames != null) {
      throw new IllegalStateException("Immutable variables are already defined,"
          + " variable immutability cannot be changed once set!");
    }
    this.immutableNames = ImmutableSet.copyOf(variableNames);
    return this;
  }

  /**
   * Adds a variable with value to the context or overrides an already existing
   * one. If the variable is already set and immutable an IllegalStateException
   * is thrown.
   * @param name Name of the variable to be added to the context
   * @param value Value of the variable
   * @throws IllegalStateException if the variable is immutable and already set
   * @return same instance of VariableContext for daisy chaining.
   */
  public VariableContext put(String name, String value) {
    if (variables.containsKey(name) && isImmutable(name)) {
      throw new IllegalStateException(
          "Variable '" + name + "' is immutable, cannot update it's value!");
    }

    if (conditionalVariables.containsKey(name)) {
      throw new IllegalStateException(
          "Variable '" + name + "' is already defined as a conditional" +
              " variable, cannot change it's value!");
    }
    variables.put(name, value);
    return this;
  }

  public void putOriginal(String name, String value) {
    originalVariables.put(name, value);
  }

  /**
   * This method is used to add a conditional variable to the variable context.
   * @param name Name of the variable
   * @param variable The conditional variable evaluator
   * @return VariableContext for daisy chaining
   */
  public VariableContext putConditional(String name,
      MappingRuleConditionalVariable variable) {
    if (conditionalVariables.containsKey(name)) {
      throw new IllegalStateException(
          "Variable '" + name + "' is conditional, cannot update it's value!");
    }
    conditionalVariables.put(name, variable);
    return this;
  }

  /**
   * Returns the value of a variable, null values are replaced with "".
   * @param name Name of the variable
   * @return The value of the variable
   */
  public String get(String name) {
    String ret = variables.get(name);
    return ret == null ? "" : ret;
  }

  public String getOriginal(String name) {
    return originalVariables.get(name);
  }

  /**
   * Adds a set to the context, each name can only be added once. The extra
   * dataset is different from the regular variables because it cannot be
   * referenced via tokens in the paths or any other input. However matchers
   * and actions can explicitly access these datasets and can make decisions
   * based on them.
   * @param name Name which can be used to reference the collection
   * @param set The dataset to be stored
   */
  public void putExtraDataset(String name, Set<String> set) {
    if (extraDataset.containsKey(name)) {
      throw new IllegalStateException(
          "Dataset '" + name + "' is already set!");
    }
    extraDataset.put(name, set);
  }

  /**
   * Returns the dataset referenced by the name.
   * @param name Name of the set to be returned.
   * @return the dataset referenced by the name.
   */
  public Set<String> getExtraDataset(String name) {
    return extraDataset.get(name);
  }

  /**
   * Check if a variable is part of the context.
   * @param name Name of the variable to be checked
   * @return True if the variable is added to the context, false otherwise
   */
  public boolean containsKey(String name) {
    return variables.containsKey(name);
  }

  /**
   * This method replaces all variables in the provided string. The variables
   * are reverse ordered by the length of their names in order to avoid partial
   * replaces when a shorter named variable is a substring of a longer named
   * variable.
   * All variables will be replaced in the string.
   * Null values will be considered as empty strings during the replace.
   * If the input is null, null will be returned.
   * @param input The string with variables
   * @return A string with all the variables substituted with their respective
   *         values.
   */
  public String replaceVariables(String input) {
    if (input == null) {
      return null;
    }

    String[] keys = variables.keySet().toArray(new String[]{});
    //Replacing variables starting longest first, to avoid collision when a
    //shorter variable name matches the beginning of a longer one.
    //e.g. %user_something, if %user is defined it may replace the %user before
    //we would reach the %user_something variable, so we start with the longer
    //names first
    Arrays.sort(keys, (a, b) -> b.length() - a.length());

    String ret = input;
    for (String key : keys) {
      //we cannot match for null, so we just skip if we have a variable "name"
      //with null
      if (key == null) {
        continue;
      }
      ret = ret.replace(key, get(key));
    }

    return ret;
  }

  /**
   * This method will consider the input as a queue path, which is a String
   * separated by dot ('.') characters. The input will be split along the dots
   * and all parts will be replaced individually. Replace only occur if a part
   * exactly matches a variable name, no composite names or additional
   * characters are supported.
   * e.g. With variables %user and %default "%user.%default" will be substituted
   * while "%user%default.something" won't.
   * Null values will be considered as empty strings during the replace.
   * If the input is null, null will be returned.
   * @param input The string with variables
   * @return A string with all the variable only path parts substituted with
   *         their respective values.
   */
  public String replacePathVariables(String input) {
    if (input == null) {
      return null;
    }

    String[] parts = input.split("\\.");
    for (int i = 0; i < parts.length; i++) {
      String newVal = parts[i];
      //if the part is a variable it should be in either the variable or the
      //conditional variable map, otherwise we keep it's original value.
      //This means undefined variables will return the name of the variable,
      //but this is working as intended.
      if (variables.containsKey(parts[i])) {
        newVal = variables.get(parts[i]);
      } else if (conditionalVariables.containsKey(parts[i])) {
        MappingRuleConditionalVariable condVariable =
            conditionalVariables.get(parts[i]);
        if (condVariable != null) {
          newVal = condVariable.evaluateInPath(parts, i);
        }
      }

      //if a variable's value is null, we use empty string instead
      if (newVal == null) {
        newVal = "";
      }
      parts[i] = newVal;
    }

    return String.join(".", parts);
  }
}

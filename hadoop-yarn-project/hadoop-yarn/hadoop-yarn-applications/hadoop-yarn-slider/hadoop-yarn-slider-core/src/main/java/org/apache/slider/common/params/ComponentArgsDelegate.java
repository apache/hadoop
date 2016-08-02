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
import org.apache.slider.core.exceptions.BadCommandArgumentsException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ComponentArgsDelegate extends AbstractArgsDelegate {

  /**
   * This is a listing of the roles to create
   */
  @Parameter(names = {ARG_COMPONENT,  ARG_COMPONENT_SHORT, ARG_ROLE},
             arity = 2,
             description = "--component <name> <count> e.g. +1 incr by 1, -2 decr by 2, and 3 makes final count 3",
             splitter = DontSplitArguments.class)
  public List<String> componentTuples = new ArrayList<>(0);


  /**
   * Get the role mapping (may be empty, but never null)
   * @return role mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, String> getComponentMap() throws BadCommandArgumentsException {
    return convertTupleListToMap("component", componentTuples);
  }

  public List<String> getComponentTuples() {
    return componentTuples;
  }
}

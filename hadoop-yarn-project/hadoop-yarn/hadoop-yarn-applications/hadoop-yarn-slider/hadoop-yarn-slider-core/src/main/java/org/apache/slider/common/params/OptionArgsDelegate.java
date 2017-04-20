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

/**
 * Delegate for application and resource options.
 */
public class OptionArgsDelegate extends AbstractArgsDelegate {

  /**
   * Options key value.
   */
  @Parameter(names = {ARG_OPTION, ARG_OPTION_SHORT}, arity = 2,
             description = ARG_OPTION + "<name> <value>",
             splitter = DontSplitArguments.class)
  public List<String> optionTuples = new ArrayList<>(0);


  /**
   * All the app component option triples.
   */
  @Parameter(names = {ARG_COMP_OPT, ARG_COMP_OPT_SHORT}, arity = 3,
             description = "Component option " + ARG_COMP_OPT +
                           " <component> <name> <option>",
             splitter = DontSplitArguments.class)
  public List<String> compOptTriples = new ArrayList<>(0);

  public Map<String, String> getOptionsMap() throws
                                             BadCommandArgumentsException {
    return convertTupleListToMap(ARG_OPTION, optionTuples);
  }

  /**
   * Get the role heap mapping (may be empty, but never null).
   * @return role heap mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, Map<String, String>> getCompOptionMap()
      throws BadCommandArgumentsException {
    return convertTripleListToMaps(ARG_COMP_OPT, compOptTriples);
  }

}

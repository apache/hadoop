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

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;

import java.util.List;
import java.util.Map;

@Parameters(commandNames = {SliderActions.ACTION_FLEX},
            commandDescription = SliderActions.DESCRIBE_ACTION_FLEX)

public class ActionFlexArgs extends AbstractActionArgs {

  @Override
  public String getActionName() {
    return SliderActions.ACTION_FLEX;
  }
  
  @ParametersDelegate
  public ComponentArgsDelegate componentDelegate = new ComponentArgsDelegate();

  /**
   * Get the component mapping (may be empty, but never null)
   * @return mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, String> getComponentMap() throws BadCommandArgumentsException {
    return componentDelegate.getComponentMap();
  }

  public List<String> getComponentTuples() {
    return componentDelegate.getComponentTuples();
  }

}

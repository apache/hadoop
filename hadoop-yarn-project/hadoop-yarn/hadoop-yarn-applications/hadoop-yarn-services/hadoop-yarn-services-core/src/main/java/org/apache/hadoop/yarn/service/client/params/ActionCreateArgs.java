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

package org.apache.hadoop.yarn.service.client.params;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.hadoop.yarn.service.exceptions.BadCommandArgumentsException;

@Parameters(commandNames = { SliderActions.ACTION_CREATE},
            commandDescription = SliderActions.DESCRIBE_ACTION_CREATE)

public class ActionCreateArgs extends AbstractClusterBuildingActionArgs {

  @Parameter(names = { ARG_EXAMPLE, ARG_EXAMPLE_SHORT },
      description = "The name of the example service such as sleeper")
  public String example;

  @Override
  public String getActionName() {
    return SliderActions.ACTION_CREATE;
  }

  @Override
  public void validate() throws BadCommandArgumentsException {
    if (file == null && example == null) {
      throw new BadCommandArgumentsException("No service definition provided.");
    }
  }
}


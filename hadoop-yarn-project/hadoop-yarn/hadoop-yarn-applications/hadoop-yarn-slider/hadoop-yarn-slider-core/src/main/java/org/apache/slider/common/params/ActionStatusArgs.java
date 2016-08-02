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
import com.beust.jcommander.Parameters;

@Parameters(commandNames = {SliderActions.ACTION_STATUS},
            commandDescription = SliderActions.DESCRIBE_ACTION_STATUS)

public class ActionStatusArgs extends AbstractActionArgs {

  @Override
  public String getActionName() {
    return SliderActions.ACTION_STATUS;
  }

  @Parameter(names = {ARG_OUTPUT, ARG_OUTPUT_SHORT},
             description = "Output file for the status information")
  public String output;

  public String getOutput() {
    return output;
  }

  public void setOutput(String output) {
    this.output = output;
  }
}

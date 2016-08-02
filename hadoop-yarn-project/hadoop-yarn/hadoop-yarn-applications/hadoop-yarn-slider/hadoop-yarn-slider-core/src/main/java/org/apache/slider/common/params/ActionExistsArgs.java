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

import java.io.File;

@Parameters(commandNames = {SliderActions.ACTION_EXISTS},
            commandDescription = SliderActions.DESCRIBE_ACTION_EXISTS)

public class ActionExistsArgs extends AbstractActionArgs {

  @Override
  public String getActionName() {
    return SliderActions.ACTION_EXISTS;
  }

  @Parameter(names = {ARG_LIVE},
             description = "verify that the application is running")
  public boolean live;
  
  @Parameter(names = {ARG_STATE},
             description = "verify that the application is in the specific YARN state")
  public String state = "";

  @Parameter(names = {ARG_OUTPUT, ARG_OUTPUT_SHORT},
      description = "output file for any application report")
  public File out;
}

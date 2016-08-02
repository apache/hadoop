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

@Parameters(commandNames = {SliderActions.ACTION_NODES},
            commandDescription = SliderActions.DESCRIBE_ACTION_NODES)
public class ActionNodesArgs extends AbstractActionArgs {

  /**
   * Instance for API use; on CLI the name is derived from {@link #getClusterName()}.
   */
  public String instance;

  @Override
  public String getActionName() {
    return SliderActions.ACTION_NODES;
  }

  @Parameter(names = {ARG_OUTPUT, ARG_OUTPUT_SHORT},
             description = "Output file for the information")
  public File outputFile;

  @Parameter(names = {ARG_LABEL})
  public String label = "";

  @Parameter(names = {ARG_HEALTHY} )
  public boolean healthy;

  @Override
  public int getMinParams() {
    return 0;
  }

  @Override
  public int getMaxParams() {
    return 1;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
      "ActionNodesArgs{");
    sb.append("instance='").append(instance).append('\'');
    sb.append(", outputFile=").append(outputFile);
    sb.append(", label='").append(label).append('\'');
    sb.append(", healthy=").append(healthy);
    sb.append('}');
    return sb.toString();
  }
}

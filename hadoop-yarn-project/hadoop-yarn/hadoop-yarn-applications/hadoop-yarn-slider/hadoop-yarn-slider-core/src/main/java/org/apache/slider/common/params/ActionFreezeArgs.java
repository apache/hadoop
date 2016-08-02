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
import com.beust.jcommander.ParametersDelegate;

@Parameters(commandNames = {SliderActions.ACTION_FREEZE},
            commandDescription = SliderActions.DESCRIBE_ACTION_FREEZE)

public class ActionFreezeArgs extends AbstractActionArgs implements
                                                         WaitTimeAccessor {
  @Override
  public String getActionName() {
    return SliderActions.ACTION_FREEZE;
  }
  
  public static final String FREEZE_COMMAND_ISSUED = "stop command issued";
  @ParametersDelegate
  public WaitArgsDelegate waitDelegate = new WaitArgsDelegate();

  @Override
  public int getWaittime() {
    return waitDelegate.getWaittime();
  }

  @Override
  public void setWaittime(int waittime) {
    waitDelegate.setWaittime(waittime);
  }

  @Parameter(names={ARG_MESSAGE},
             description = "reason for the operation")
  public String message = FREEZE_COMMAND_ISSUED;

  @Parameter(names = {ARG_FORCE},
             description = "force the operation")
  public boolean force;
}

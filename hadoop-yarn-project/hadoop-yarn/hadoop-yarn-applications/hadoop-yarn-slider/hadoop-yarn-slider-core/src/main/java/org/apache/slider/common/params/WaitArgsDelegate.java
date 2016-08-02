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

public class WaitArgsDelegate extends AbstractArgsDelegate implements
                                                           WaitTimeAccessor {


  //--wait [timeout]
  @Parameter(names = {ARG_WAIT},
             description = "time to wait for an action to complete")
  public int waittime = 0;


  @Override
  public int getWaittime() {
    return waittime;
  }

  @Override
  public void setWaittime(int waittime) {
    this.waittime = waittime;
  }
}

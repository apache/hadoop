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

@Parameters(commandNames = {SliderActions.ACTION_INSTALL_KEYTAB},
            commandDescription = SliderActions.DESCRIBE_ACTION_INSTALL_KEYTAB)

public class ActionInstallKeytabArgs extends AbstractActionArgs {
  
  @Override
  public String getActionName() {
    return SliderActions.ACTION_INSTALL_KEYTAB;
  }

  @Parameter(names = {ARG_KEYTAB},
             description = "Path to keytab on local disk")
  public String keytabUri;

  @Parameter(names = {ARG_FOLDER},
             description = "The name of the folder in which to store the keytab")
  public String folder;

  @Parameter(names = {ARG_OVERWRITE}, description = "Overwrite existing keytab")
  public boolean overwrite = false;

  /**
   * Get the min #of params expected
   * @return the min number of params in the {@link #parameters} field
   */
  public int getMinParams() {
    return 0;
  }

  @Override
  public int getMaxParams() {
    return 3;
  }
}

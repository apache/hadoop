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

@Parameters(
  commandNames = {SliderActions.ACTION_DIAGNOSTICS},
  commandDescription = SliderActions.DESCRIBE_ACTION_DIAGNOSTIC)
public class ActionDiagnosticArgs extends AbstractActionArgs {

    @Override
    public String getActionName() {
      return SliderActions.ACTION_DIAGNOSTICS;
    }

    @Parameter(names = {ARG_NAME}, 
        description = "the name of the running application")
    public String name;

	  @Parameter(names = {ARG_CLIENT}, 
	      description = "print configuration of the slider client")
	  public boolean client = false;

	  @Parameter(names = {ARG_APPLICATION}, 
	      description = "print configuration of the running application")
	  public boolean application;

	  @Parameter(names = {ARG_VERBOSE}, 
	      description = "print out information in details")
	  public boolean verbose = false;

	  @Parameter(names = {ARG_YARN}, 
	      description = "print configuration of the YARN cluster")
	  public boolean yarn = false;

	  @Parameter(names = {ARG_CREDENTIALS}, 
	      description = "print credentials of the current user")
	  public boolean credentials = false;

	  @Parameter(names = {ARG_ALL}, 
	      description = "print all of the information above")
	  public boolean all;

	  @Parameter(names = {ARG_LEVEL}, 
	      description = "diagnose each slider configuration one by one")
	  public boolean level;

	  /**
	   * Get the min #of params expected
	   * @return the min number of params in the {@link #parameters} field
	   */
	  @Override
	  public int getMinParams() {
	    return 0;
	  }
}

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
import com.beust.jcommander.ParametersDelegate;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.service.exceptions.BadCommandArgumentsException;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Abstract Action to build things; shares args across build and
 * list
 */
public abstract class AbstractClusterBuildingActionArgs
    extends AbstractActionArgs {
  @Parameter(names = {ARG_APPDEF},
      description = "Template application definition file in JSON format.")
  public File appDef;

  public File getAppDef() {
    return appDef;
  }

  @Parameter(names = {
      ARG_QUEUE }, description = "Queue to submit the application")
  public String queue;

  @Parameter(names = {
      ARG_LIFETIME }, description = "Lifetime of the application from the time of request")
  public long lifetime;

  @ParametersDelegate
  public ComponentArgsDelegate componentDelegate = new ComponentArgsDelegate();

  @ParametersDelegate
  public OptionArgsDelegate optionsDelegate =
      new OptionArgsDelegate();
}

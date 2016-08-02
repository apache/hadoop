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
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.UsageException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Parameters(commandNames = {SliderActions.ACTION_KDIAG},
            commandDescription = SliderActions.DESCRIBE_ACTION_KDIAG)

public class ActionKDiagArgs extends AbstractActionArgs {

  @Override
  public String getActionName() {
    return SliderActions.ACTION_KDIAG;
  }

  @Parameter(names = {ARG_SERVICES}, variableArity = true,
    description =" list of services to check")
  public List<String> services = new ArrayList<>();

  @Parameter(names = {ARG_OUTPUT, ARG_OUTPUT_SHORT},
      description = "output file for report")
  public File out;

  @Parameter(names = {ARG_KEYTAB}, description = "keytab to use")
  public File keytab;

  @Parameter(names = {ARG_KEYLEN}, description = "minimum key length")
  public int keylen = 256;

  @Parameter(names = {ARG_PRINCIPAL}, description = "principal to log in from a keytab")
  public String principal;

  @Parameter(names = {ARG_SECURE}, description = "Is security required")
  public boolean secure = false;

  @Override
  public int getMinParams() {
    return 0;
  }

  @Override
  public boolean getHadoopServicesRequired() {
    return false;
  }

  @Override
  public boolean disableSecureLogin() {
    return true;
  }

  @Override
  public void validate() throws BadCommandArgumentsException, UsageException {
    super.validate();
    if (keytab != null && SliderUtils.isUnset(principal)) {
      throw new UsageException("Missing argument " + ARG_PRINCIPAL);
    }
    if (keytab == null && SliderUtils.isSet(principal)) {
      throw new UsageException("Missing argument " + ARG_KEYTAB);
    }
  }
}

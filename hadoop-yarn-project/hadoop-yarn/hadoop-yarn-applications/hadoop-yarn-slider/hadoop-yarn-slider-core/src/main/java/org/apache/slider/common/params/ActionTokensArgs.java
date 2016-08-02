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
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.UsageException;

import java.io.File;

@Parameters(commandNames = {SliderActions.ACTION_TOKENS},
            commandDescription = "save tokens to a file or list tokens in a file")
public class ActionTokensArgs extends AbstractActionArgs {

  public static final String DUPLICATE_ARGS = "Only one of " +
      ARG_SOURCE + " and " + ARG_OUTPUT + " allowed";

  public static final String MISSING_KT_PROVIDER =
      "Both " + ARG_KEYTAB + " and " + ARG_PRINCIPAL
      + " must be provided";

  @Override
  public String getActionName() {
    return SliderActions.ACTION_TOKENS;
  }

  @Parameter(names = {ARG_OUTPUT},
             description = "File to write")
  public File output;

  @Parameter(names = {ARG_SOURCE},
             description = "source file")
  public File source;

  @Parameter(names = {ARG_KEYTAB}, description = "keytab to use")
  public File keytab;

  @Parameter(names = {ARG_PRINCIPAL}, description = "principal to log in from a keytab")
  public String principal="";

  /**
   * Get the min #of params expected
   * @return the min number of params in the {@link #parameters} field
   */
  public int getMinParams() {
    return 0;
  }

  @Override
  public void validate() throws BadCommandArgumentsException, UsageException {
    super.validate();
    if (output != null && source != null) {
      throw new BadCommandArgumentsException(DUPLICATE_ARGS);
    }

    // this is actually a !xor
    if (keytab != null ^ !principal.isEmpty()) {
      throw new BadCommandArgumentsException(MISSING_KT_PROVIDER);
    }
  }
}

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
import org.apache.commons.lang.StringUtils;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.UsageException;

import java.io.File;

@Parameters(commandNames = {SliderActions.ACTION_LOOKUP},
            commandDescription = SliderActions.DESCRIBE_ACTION_LOOKUP)

public class ActionLookupArgs extends AbstractActionArgs {
  @Override
  public String getActionName() {
    return SliderActions.ACTION_LOOKUP;
  }

  public int getMinParams() {
    return 0;
  }
  public int getMaxParams() {
    return 0;
  }
  
  @Parameter(names = {ARG_ID},
             description = "ID of the application")
  public String id;

  @Parameter(names = {ARG_OUTPUT, ARG_OUTPUT_SHORT},
      description = "output file for any application report")
  public File outputFile;

  @Override
  public void validate() throws BadCommandArgumentsException, UsageException {
    super.validate();
    if (StringUtils.isEmpty(id)) {
      throw new BadCommandArgumentsException("Missing mandatory argument "
                                             + ARG_ID);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder(SliderActions.ACTION_LOOKUP);
    if (id!=null) {
      sb.append(" ");
      sb.append(ARG_ID).append(" ").append(id);
    }
    if (outputFile != null) {
      sb.append(" ");
      sb.append(ARG_OUTPUT).append(" ").append(outputFile.getAbsolutePath());
    }
    return sb.toString();
  }
}

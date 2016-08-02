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

import static org.apache.slider.common.params.SliderActions.ACTION_RESOLVE;
import static org.apache.slider.common.params.SliderActions.DESCRIBE_ACTION_REGISTRY;

/**
 * Resolve registry entries
 * 
 * --path {path}
 * --out {destfile}
 * --verbose
 * --list
 */
@Parameters(commandNames = {ACTION_RESOLVE},
            commandDescription = DESCRIBE_ACTION_REGISTRY)
public class ActionResolveArgs extends AbstractActionArgs {

  public static final String USAGE =
      "Usage: " + SliderActions.ACTION_RESOLVE
      + " "
      + ARG_PATH + " <path> "
      + "[" + ARG_LIST + "] "
      + "[" + ARG_OUTPUT + " <filename> ] "
      + "[" + ARG_DESTDIR + " <directory> ] "
      ;
  public ActionResolveArgs() {
  }

  @Override
  public String getActionName() {
    return ACTION_RESOLVE;
  }

  /**
   * Get the min #of params expected
   * @return the min number of params in the {@link #parameters} field
   */
  @Override
  public int getMinParams() {
    return 0;
  }
  
  @Parameter(names = {ARG_LIST}, 
      description = "list services")
  public boolean list;

  @Parameter(names = {ARG_PATH},
      description = "resolve a path")
  public String path;

  @Parameter(names = {ARG_DESTDIR},
      description = "destination directory for operations")
  public File destdir;

  @Parameter(names = {ARG_OUTPUT, ARG_OUTPUT_SHORT},
      description = "dest file")
  public File out;

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder(ACTION_RESOLVE).append(" ");
    sb.append(ARG_PATH).append(" ").append(path).append(" ");
    if (list) {
      sb.append(ARG_LIST).append(" ");
    }
    if (destdir != null) {
      sb.append(ARG_DESTDIR).append(" ").append(destdir).append(" ");
    }
    if (out != null) {
      sb.append(ARG_OUTPUT).append(" ").append(out).append(" ");
    }
    return sb.toString();
  }

  @Override
  public void validate() throws BadCommandArgumentsException, UsageException {
    super.validate();
    if (StringUtils.isEmpty(path)) {
      throw new BadCommandArgumentsException("Missing mandatory argument "
                                             + ARG_PATH);
    }
    if (list && out != null) {
      throw new BadCommandArgumentsException("Argument "
                                             + ARG_OUTPUT +
                                             " not supported for " + ARG_LIST);
    }
    if (out != null && destdir != null) {
      throw new BadCommandArgumentsException(
          ARG_OUTPUT + " and " + ARG_DESTDIR + " cannot be used together"
      );
    }
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public boolean isList() {
    return list;
  }

  public void setList(boolean list) {
    this.list = list;
  }

  public File getDestdir() {
    return destdir;
  }

  public void setDestdir(File destdir) {
    this.destdir = destdir;
  }

  public File getOut() {
    return out;
  }

  public void setOut(File out) {
    this.out = out;
  }

}

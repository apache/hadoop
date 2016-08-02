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
import org.apache.slider.common.SliderKeys;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.UsageException;
import org.apache.slider.core.registry.docstore.ConfigFormat;

import static org.apache.slider.common.params.SliderActions.ACTION_REGISTRY;
import static org.apache.slider.common.params.SliderActions.DESCRIBE_ACTION_REGISTRY;
import java.io.File;

/**
 * Registry actions
 * 
 * --instance {app name}, if  a / is in it, refers underneath?
 * --dest {destfile}
 * --list : list instances of slider service
 * --listfiles 
 */
@Parameters(commandNames = {ACTION_REGISTRY},
            commandDescription = DESCRIBE_ACTION_REGISTRY)

public class ActionRegistryArgs extends AbstractActionArgs {

  public static final String USAGE =
      "Usage: " + SliderActions.ACTION_REGISTRY
      + " ("
      + Arguments.ARG_LIST + "|"
      + Arguments.ARG_LISTCONF + "|"
      + Arguments.ARG_LISTEXP + "|"
      + Arguments.ARG_LISTFILES + "|"
      + Arguments.ARG_GETCONF + "|"
      + Arguments.ARG_GETEXP + "> "
      + Arguments.ARG_NAME + " <name> "
      + " )"
      + "[" + Arguments.ARG_VERBOSE + "] "
      + "[" + Arguments.ARG_USER + "] "
      + "[" + Arguments.ARG_OUTPUT + " <filename> ] "
      + "[" + Arguments.ARG_SERVICETYPE + " <servicetype> ] "
      + "[" + Arguments.ARG_FORMAT + " <xml|json|properties>] "
      + System.getProperty("line.separator")
      + "Arguments.ARG_GETEXP only supports " + Arguments.ARG_FORMAT + " json"
      ;
  public ActionRegistryArgs() {
  }

  public ActionRegistryArgs(String name) {
    this.name = name;
  }

  @Override
  public String getActionName() {
    return ACTION_REGISTRY;
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

  @Parameter(names = {ARG_LISTCONF}, 
      description = "list configurations")
  public boolean listConf;

  @Parameter(names = {ARG_GETCONF},
      description = "get configuration")
  public String getConf;

  @Parameter(names = {ARG_LISTEXP},
             description = "list exports")
  public boolean listExports;

  @Parameter(names = {ARG_GETEXP},
             description = "get export")
  public String getExport;

  @Parameter(names = {ARG_LISTFILES},
      description = "list files")
  public String listFiles;

  @Parameter(names = {ARG_GETFILES},
      description = "get files")
  public String getFiles;

  //--format 
  @Parameter(names = ARG_FORMAT,
      description = "Format for a response: <xml|json|properties>")
  public String format = ConfigFormat.XML.toString() ;

  @Parameter(names = {ARG_OUTPUT, ARG_OUTPUT_SHORT, ARG_DEST},
      description = "Output destination")
  public File out;

  @Parameter(names = {ARG_NAME},
      description = "name of an instance")
  public String name;

  @Parameter(names = {ARG_SERVICETYPE},
      description = "optional service type")
  public String serviceType = SliderKeys.APP_TYPE;

  @Parameter(names = {ARG_VERBOSE},
      description = "verbose output")
  public boolean verbose;

  @Parameter(names = {ARG_INTERNAL},
      description = "fetch internal registry entries")
  public boolean internal;

  @Parameter(names = {ARG_USER},
      description = "the name of the user whose application is being resolved")
  public String user;

  /**
   * validate health of all the different operations
   * @throws BadCommandArgumentsException
   */
  @Override
  public void validate() throws BadCommandArgumentsException, UsageException {
    super.validate();

    //verify that at most one of the operations is set
    int gets = s(getConf) + s(getFiles) + s(getExport);
    int lists = s(list) + s(listConf) + s(listFiles) + s(listExports);
    int set = lists + gets;
    if (set > 1) {
      throw new UsageException(USAGE);
    }

    if (out != null && ( set == 0)) {
      throw new UsageException("output path"
           + " is only supported on 'get' operations: ");
    }
    if (!list && !is(name)) {
      throw new UsageException("Argument " + ARG_NAME
           +" missing: ");

    }
  }
  
  private int s(String arg) {
    return is(arg) ? 1 : 0;
  }

  private boolean is(String arg) {
    return arg != null;
  }

  private int s(boolean arg) {
    return arg ? 1 : 0;
  }

  private String ifdef(String arg, boolean val) {
    return val ? (arg + " "): "";
  }

  private String ifdef(String arg, String val) {
    if (is(val)) {
      return arg + " " + val + " ";
    } else {
      return "";
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder(ACTION_REGISTRY);
    sb.append(' ');
    sb.append(ifdef(ARG_LIST, list));
    sb.append(ifdef(ARG_LISTCONF, listConf));
    sb.append(ifdef(ARG_LISTFILES, listFiles));
    sb.append(ifdef(ARG_GETCONF, getConf));
    sb.append(ifdef(ARG_GETFILES, getFiles));

    sb.append(ifdef(ARG_NAME, name));
    sb.append(ifdef(ARG_SERVICETYPE, serviceType));


    sb.append(ifdef(ARG_VERBOSE, verbose));
    sb.append(ifdef(ARG_INTERNAL, internal));

    if (out != null) {
      sb.append(ifdef(ARG_OUTPUT, out.toString()));
    }
    sb.append(ifdef(ARG_FORMAT, format));

    return sb.toString();
  }
}

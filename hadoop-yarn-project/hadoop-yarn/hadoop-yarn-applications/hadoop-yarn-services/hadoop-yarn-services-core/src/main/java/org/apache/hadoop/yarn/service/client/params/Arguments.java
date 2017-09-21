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

/**
 * Here are all the arguments that may be parsed by the client or server
 * command lines. 
 * 
 * Important: Please keep the main list in alphabetical order
 * so it is easier to see what arguments are there
 */
public interface Arguments {

  String ARG_FILE = "--file";
  String ARG_FILE_SHORT = "-f";
  String ARG_BASE_PATH = "--basepath";
  String ARG_COMPONENT = "--component";
  String ARG_COMPONENT_SHORT = "--comp";
  String ARG_COMPONENTS = "--components";
  String ARG_COMP_OPT= "--compopt";
  String ARG_COMP_OPT_SHORT = "--co";
  String ARG_CONFIG = "--config";
  String ARG_CONTAINERS = "--containers";
  String ARG_DEBUG = "--debug";
  String ARG_DEFINE = "-D";
  String ARG_DELETE = "--delete";
  String ARG_DEST = "--dest";
  String ARG_DESTDIR = "--destdir";
  String ARG_EXAMPLE = "--example";
  String ARG_EXAMPLE_SHORT = "-e";
  String ARG_FOLDER = "--folder";
  String ARG_FORCE = "--force";
  String ARG_FORMAT = "--format";
  String ARG_GETCONF = "--getconf";
  String ARG_GETEXP = "--getexp";
  String ARG_GETFILES = "--getfiles";
  String ARG_HELP = "--help";
  String ARG_IMAGE = "--image";
  String ARG_INSTALL = "--install";
  String ARG_INTERNAL = "--internal";
  String ARG_KEYLEN = "--keylen";
  String ARG_KEYTAB = "--keytab";
  String ARG_KEYTABINSTALL = ARG_INSTALL;
  String ARG_KEYTABDELETE = ARG_DELETE;
  String ARG_KEYTABLIST = "--list";
  String ARG_LIST = "--list";
  String ARG_LISTCONF = "--listconf";
  String ARG_LISTEXP = "--listexp";
  String ARG_LISTFILES = "--listfiles";
  String ARG_LIVE = "--live";
  String ARG_MANAGER = "--manager";
  String ARG_MANAGER_SHORT = "--m";
  String ARG_MESSAGE = "--message";
  String ARG_NAME = "--name";
  String ARG_OPTION = "--option";
  String ARG_OPTION_SHORT = "-O";
  String ARG_OUTPUT = "--out";
  String ARG_OUTPUT_SHORT = "-o";
  String ARG_OVERWRITE = "--overwrite";
  String ARG_PACKAGE = "--package";
  String ARG_PATH = "--path";
  String ARG_PRINCIPAL = "--principal";
  String ARG_QUEUE = "--queue";
  String ARG_SHORT_QUEUE = "-q";
  String ARG_LIFETIME = "--lifetime";
  String ARG_RESOURCE = "--resource";
  String ARG_RESOURCE_MANAGER = "--rm";
  String ARG_SECURE = "--secure";
  String ARG_SERVICETYPE = "--servicetype";
  String ARG_SERVICES = "--services";
  String ARG_SOURCE = "--source";
  String ARG_STATE = "--state";
  String ARG_SYSPROP = "-S";
  String ARG_USER = "--user";
  String ARG_UPLOAD = "--upload";
  String ARG_VERBOSE = "--verbose";
  String ARG_VERSION = "--version";
  String ARG_WAIT = "--wait";
/*
 STOP: DO NOT ADD YOUR ARGUMENTS HERE. GO BACK AND INSERT THEM IN THE
 RIGHT PLACE IN THE LIST
 */

  // Tha path in hdfs to be read by Service AM
  String ARG_SERVICE_DEF_PATH = "-cluster-uri";

}

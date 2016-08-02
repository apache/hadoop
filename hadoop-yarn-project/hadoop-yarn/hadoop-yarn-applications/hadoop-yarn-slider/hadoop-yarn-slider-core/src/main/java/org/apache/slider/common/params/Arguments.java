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

/**
 * Here are all the arguments that may be parsed by the client or server
 * command lines. 
 * 
 * Important: Please keep the main list in alphabetical order
 * so it is easier to see what arguments are there
 */
public interface Arguments {
  String ARG_ADDON = "--addon";
  String ARG_ALL = "--all";
  String ARG_ALIAS = "--alias";
  String ARG_APPLICATION = "--application";
  String ARG_APPDEF = "--appdef";
  String ARG_APP_HOME = "--apphome";
  String ARG_BASE_PATH = "--basepath";
  String ARG_CLIENT = "--client";
  String ARG_CONFDIR = "--appconf";
  String ARG_COMPONENT = "--component";
  String ARG_COMPONENT_SHORT = "--comp";
  String ARG_COMPONENTS = "--components";
  String ARG_COMP_OPT= "--compopt";
  String ARG_COMP_OPT_SHORT = "--co";
  String ARG_CONFIG = "--config";
  String ARG_CONTAINERS = "--containers";
  String ARG_CREDENTIALS = "--credentials";
  String ARG_DEBUG = "--debug";
  String ARG_DEFINE = "-D";
  String ARG_DELETE = "--delete";
  String ARG_DEST = "--dest";
  String ARG_DESTDIR = "--destdir";
  String ARG_DESTFILE = "--destfile";
  String ARG_EXITCODE = "--exitcode";
  String ARG_FAIL = "--fail";
  /**
   filesystem-uri: {@value}
   */
  String ARG_FILESYSTEM = "--fs";
  String ARG_FILESYSTEM_LONG = "--filesystem";
  String ARG_FOLDER = "--folder";
  String ARG_FORCE = "--force";
  String ARG_FORMAT = "--format";
  String ARG_GETCERTSTORE = "--getcertstore";
  String ARG_GETCONF = "--getconf";
  String ARG_GETEXP = "--getexp";
  String ARG_GETFILES = "--getfiles";
  String ARG_HEALTHY= "--healthy";
  String ARG_HELP = "--help";
  String ARG_HOSTNAME = "--hostname";
  String ARG_ID = "--id";
  String ARG_IMAGE = "--image";
  String ARG_INSTALL = "--install";
  String ARG_INTERNAL = "--internal";
  String ARG_KEYLEN = "--keylen";
  String ARG_KEYTAB = "--keytab";
  String ARG_KEYSTORE = "--keystore";
  String ARG_KEYTABINSTALL = ARG_INSTALL;
  String ARG_KEYTABDELETE = ARG_DELETE;
  String ARG_KEYTABLIST = "--list";
  String ARG_LABEL = "--label";
  String ARG_LEVEL = "--level";
  String ARG_LIST = "--list";
  String ARG_LISTCONF = "--listconf";
  String ARG_LISTEXP = "--listexp";
  String ARG_LISTFILES = "--listfiles";
  String ARG_LIVE = "--live";
  String ARG_MANAGER = "--manager";
  String ARG_MANAGER_SHORT = "--m";
  String ARG_MESSAGE = "--message";
  String ARG_METAINFO = "--metainfo";
  String ARG_METAINFO_JSON = "--metainfojson";
  String ARG_NAME = "--name";
  String ARG_OPTION = "--option";
  String ARG_OPTION_SHORT = "-O";
  String ARG_OUTPUT = "--out";
  String ARG_OUTPUT_SHORT = "-o";
  String ARG_OVERWRITE = "--overwrite";
  String ARG_PACKAGE = "--package";
  String ARG_PASSWORD = "--password";
  String ARG_PATH = "--path";
  String ARG_PKGDELETE = ARG_DELETE;
  String ARG_PKGINSTANCES = "--instances";
  String ARG_PKGLIST = ARG_LIST;
  String ARG_PRINCIPAL = "--principal";
  String ARG_PROVIDER = "--provider";
  String ARG_QUEUE = "--queue";
  String ARG_REPLACE_PKG = "--replacepkg";
  String ARG_RESOURCE = "--resource";
  String ARG_RESOURCES = "--resources";
  String ARG_RES_COMP_OPT = "--rescompopt";
  String ARG_RES_COMP_OPT_SHORT = "--rco";
  String ARG_RESOURCE_MANAGER = "--rm";
  String ARG_RESOURCE_OPT = "--resopt";
  String ARG_RESOURCE_OPT_SHORT = "-ro";
  String ARG_SECURE = "--secure";
  String ARG_SERVICETYPE = "--servicetype";
  String ARG_SERVICES = "--services";
  String ARG_SLIDER = "--slider";
  String ARG_SOURCE = "--source";
  String ARG_STATE = "--state";
  String ARG_SYSPROP = "-S";
  String ARG_TEMPLATE = "--template";
  String ARG_TRUSTSTORE = "--truststore";
  String ARG_USER = "--user";
  String ARG_UPLOAD = "--upload";
  String ARG_VERBOSE = "--verbose";
  String ARG_VERSION = "--version";
  String ARG_WAIT = "--wait";
  String ARG_YARN = "--yarn";
  String ARG_ZKHOSTS = "--zkhosts";
  String ARG_ZKPATH = "--zkpath";
  String ARG_ZKPORT = "--zkport";
/*
 STOP: DO NOT ADD YOUR ARGUMENTS HERE. GO BACK AND INSERT THEM IN THE
 RIGHT PLACE IN THE LIST
 */


  /**
   * Deprecated: use ARG_COMPONENT
   */
  @Deprecated
  String ARG_ROLE = "--role";

  /**
   * Deprecated: use ARG_COMP_OPT
   */
  @Deprecated
  String ARG_ROLEOPT = "--roleopt";

  /**
   * server: URI for the cluster
   */
  String ARG_CLUSTER_URI = "-cluster-uri";


  /**
   * server: Path for the resource manager instance (required)
   */
  String ARG_RM_ADDR = "--rm";


}

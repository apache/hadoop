/**
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
package org.apache.hadoop.tools.dynamometer;

import java.util.regex.Pattern;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathFilter;

import static org.apache.hadoop.yarn.api.records.LocalResourceType.*;

/**
 * Constants used in both Client and Application Master.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DynoConstants {

  private DynoConstants() {}

  // Directory to use for remote storage (a location on the remote FS which
  // can be accessed by all components). This will be the name of the directory
  // within the submitter's home directory.
  public static final String DYNAMOMETER_STORAGE_DIR = ".dynamometer";

  /* The following used for Client -> AM communication */

  // Resource for the zip file of all of the configuration for the
  // DataNodes/NameNode
  public static final DynoResource CONF_ZIP =
      new DynoResource("CONF_ZIP", ARCHIVE, "conf");
  // Resource for the Hadoop binary archive (distribution tar)
  public static final DynoResource HADOOP_BINARY =
      new DynoResource("HADOOP_BINARY", ARCHIVE, "hadoopBinary");
  // Resource for the script used to start the DataNodes/NameNode
  public static final DynoResource START_SCRIPT =
      new DynoResource("START_SCRIPT", FILE, "start-component.sh");
  // Resource for the file system image file used by the NameNode
  public static final DynoResource FS_IMAGE =
      new DynoResource("FS_IMAGE", FILE, null);
  // Resource for the md5 file accompanying the file system image for the
  // NameNode
  public static final DynoResource FS_IMAGE_MD5 =
      new DynoResource("FS_IMAGE_MD5", FILE, null);
  // Resource for the VERSION file accompanying the file system image
  public static final DynoResource VERSION =
      new DynoResource("VERSION", FILE, "VERSION");
  // Resource for the archive containing all dependencies
  public static final DynoResource DYNO_DEPENDENCIES =
      new DynoResource("DYNO_DEPS", ARCHIVE, "dependencies");

  // Environment variable which will contain the location of the directory
  // which holds all of the block files for the DataNodes
  public static final String BLOCK_LIST_PATH_ENV = "BLOCK_ZIP_PATH";
  // The format of the name of a single block file
  public static final Pattern BLOCK_LIST_FILE_PATTERN =
      Pattern.compile("dn[0-9]+-a-[0-9]+-r-[0-9]+");
  // The file name to use when localizing the block file on a DataNode; will be
  // suffixed with an integer
  public static final String BLOCK_LIST_RESOURCE_PATH_PREFIX = "blocks/block";
  public static final PathFilter BLOCK_LIST_FILE_FILTER = (path) ->
      DynoConstants.BLOCK_LIST_FILE_PATTERN.matcher(path.getName()).find();

  // Environment variable which will contain the full path of the directory
  // which should be used for remote (shared) storage
  public static final String REMOTE_STORAGE_PATH_ENV = "REMOTE_STORAGE_PATH";
  // Environment variable which will contain the RPC address of the NameNode
  // which the DataNodes should contact, if the NameNode is not launched
  // internally by this application
  public static final String REMOTE_NN_RPC_ADDR_ENV = "REMOTE_NN_RPC_ADDR";

  // Environment variable which will contain the view ACLs for the launched
  // containers.
  public static final String JOB_ACL_VIEW_ENV = "JOB_ACL_VIEW";

  /* The following used for AM -> DN, NN communication */

  // The name of the file which will store information about the NameNode
  // (within the remote storage directory)
  public static final String NN_INFO_FILE_NAME = "nn_info.prop";

  // Environment variable which will contain additional arguments for the
  // NameNode
  public static final String NN_ADDITIONAL_ARGS_ENV = "NN_ADDITIONAL_ARGS";
  // Environment variable which will contain additional arguments for the
  // DataNode
  public static final String DN_ADDITIONAL_ARGS_ENV = "DN_ADDITIONAL_ARGS";
  // Environment variable which will contain the directory to use for the
  // NameNode's name directory;
  // if not specified a directory within the YARN container working directory
  // will be used.
  public static final String NN_NAME_DIR_ENV = "NN_NAME_DIR";
  // Environment variable which will contain the directory to use for the
  // NameNode's edits directory;
  // if not specified a directory within the YARN container working directory
  // will be used.
  public static final String NN_EDITS_DIR_ENV = "NN_EDITS_DIR";

  public static final String NN_FILE_METRIC_PERIOD_ENV =
      "NN_FILE_METRIC_PERIOD";

  /*
   * These are used as the names of properties and as the environment variables
   */

  // The port to use on the NameNode host when contacting for client RPCs
  public static final String NN_RPC_PORT = "NN_RPC_PORT";
  // The hostname of the machine running the NameNode
  public static final String NN_HOSTNAME = "NN_HOSTNAME";
  // The port to use on the NameNode host when contacting for service RPCs
  public static final String NN_SERVICERPC_PORT = "NN_SERVICERPC_PORT";
  // The port to use on the NameNode host when contacting for HTTP access
  public static final String NN_HTTP_PORT = "NN_HTTP_PORT";

}

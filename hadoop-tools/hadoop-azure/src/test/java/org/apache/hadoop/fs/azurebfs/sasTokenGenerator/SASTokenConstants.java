/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.sasTokenGenerator;

import java.util.*;

public class SASTokenConstants {
  /**
   * Blob service value for SAS token sr field
   */
  public static final String Blob_SignedResource = "b";

  /**
   * Directory service value for SAS token sr field
   */
  public static final String Directory_SignedResource = "d";

  /** HTTPS Signed protocol resource. **/
  public static final String Signed_Protocol_Resource_HTTPS = "https";

  /** Blob Canonicalized resource prefix. **/
  public static final String Blob_Canonicalized_Resource_Prefix = "/blob";

  /**
   * Shared access signature api version query parameter name
   */
  public static final String ApiVersion = "api-version";

  /**
   * End partition key query parameter name
   */
  public static final String EndPartitionKey = "epk";

  /**
   * End row key query parameter name
   */
  public static final String EndRowKey = "erk";

  /**
   * Response cache control query parameter name
   */
  public static final String ResponseCacheControl = "rscc";

  /**
   * Response content disposition query parameter name
   */
  public static final String ResponseContentDisposition = "rscd";

  /**
   * Response content encoding query parameter name
   */
  public static final String ResponseContentEncoding = "rsce";

  /**
   * Response content language query parameter name
   */
  public static final String ResponseContentLanguage = "rscl";

  /**
   * Response content type query parameter name
   */
  public static final String ResponseContentType = "rsct";

  /**
   * Shared access signature signature query parameter name
   */
  public static final String Signature = "sig";

  /**
   * Shared access signature signed expiry query parameter name
   */
  public static final String SignedExpiry = "se";

  /**
   * Shared access signature signed identifier query parameter name
   */
  public static final String SignedIdentifier = "si";

  /**
   * Shared access signature signed IP query parameter name
   */
  public static final String SignedIP = "sip";

  /**
   * Shared access signature signed permission query parameter name
   */
  public static final String SignedPermission = "sp";

  /**
   * Shared access signature signed protocol query parameter name
   */
  public static final String SignedProtocol = "spr";

  /**
   * Shared access signature signed resource query parameter name
   */
  public static final String SignedResource = "sr";

  /**
   * Shared access signature signed resource type query parameter name
   */
  public static final String SignedResourceType = "srt";

  /**
   * Shared access signature signed service query parameter name
   */
  public static final String SignedService = "ss";

  /**
   * Shared access signature signed start query parameter name
   */
  public static final String SignedStart = "st";

  /**
   * Shared access signature signed version query parameter name
   */
  public static final String SignedVersion = "sv";

  /**
   * Start partition key query parameter name
   */
  public static final String StartPartitionKey = "spk";

  /**
   * Start row key query parameter name
   */
  public static final String StartRowKey = "srk";

  /**
   * Table name query parameter name
   */
  public static final String TableName = "tn";

  /**
   * Shared access signature signed key oid query parameter name
   */
  public static final String SignedKeyOid = "skoid";

  /**
   * Shared access signature signed key tid query parameter name
   */
  public static final String SignedKeyTid = "sktid";

  /**
   * Shared access signature signed key start query parameter name
   */
  public static final String SignedKeyStart = "skt";

  /**
   * Shared access signature signed key expiry query parameter name
   */
  public static final String SignedKeyExpiry = "ske";

  /**
   * Shared access signature signed key service query parameter name
   */
  public static final String SignedKeyService = "sks";

  /**
   * Shared access signature signed key version parameter name
   */
  public static final String SignedKeyVersion = "skv";

  /**
   * Shared access signature signed authorized agent oid query parameter name
   */
  public static final String SignedAuthorizedAgentOid = "saoid";

  /**
   * Shared access signature signed unauthorized agent oid query parameter
   * name
   */
  public static final String SignedUnauthorizedAgentOid = "suoid";

  /**
   * Shared access signature signed correlation ID query parameter name
   */
  public static final String SignedCorrelationId = "scid";

  /**
   * VersionId parameter name
   */
  public static final String VersionId = "versionid";

  /**
   * depth used to calculate signed Dir path for directory SAS
   */
  public static final String SignedDirectoryDepth = "sdd";

  /**
   * Directory path query param name
   */
  public static final String DirectoryPath = "directory";

  public static final String EqualTo = "=";
  public static final String QuerySeperator = "&";

  /** SAS Permission values. **/
  public static final String ABFS_SAS_PERMISSION_READ = "r";
  public static final String ABFS_SAS_PERMISSION_WRITE = "w";
  public static final String ABFS_SAS_PERMISSION_EXECUTE = "e";
  public static final String ABFS_SAS_PERMISSION_LIST = "l";
  public static final String ABFS_SAS_PERMISSION_CHANGE_PERMISSION = "p";
  public static final String ABFS_SAS_PERMISSION_DELETE = "d";
  public static final String ABFS_SAS_PERMISSION_MOVE = "m";
  public static final String ABFS_SAS_PERMISSION_SETOWNER = "o";
  public static final String ABFS_SAS_PERMISSION_CREATE = "c";
  public static final String ABFS_SAS_PERMISSION_ADD = "d";

  /** ABFS Authorizer Actions. **/
  public static final String ABFS_RENAME_SOURCE_ACTION = "rename-source";
  public static final String ABFS_RENAME_DESTINATION_ACTION = "rename-destination";

  public static final String ABFS_CONCAT_SOURCE_ACTION = "concat-source";
  public static final String ABFS_CONCAT_TARGET_ACTION = "concat-target";

  public static final String ABFS_CHECKACCESS_ACTION_PREFIX_ACTION = "access-";

  public static final String ABFS_LISTSTATUS_ACTION = "liststatus";
  public static final String ABFS_DELETE_ACTION = "delete";
  public static final String ABFS_CREATEFILE_ACTION = "create";
  public static final String ABFS_MKDIR_ACTION = "mkdir";
  public static final String ABFS_GETACL_ACTION = "getaclstatus";
  public static final String ABFS_GETFILESTATUS_ACTION = "getfilestatus";
  // Action mapped to FS APIs such as ModifyAclEntries, RemoveAclEntries,
  // RemoveAcl, RemoveDefaultAcl, SetAclEntries
  public static final String ABFS_SETACL_ACTION = "setacl";
  public static final String ABFS_SETOWNER_ACTION = "setowner";
  public static final String ABFS_SETPERMISSION_ACTION = "setpermission";
  public static final String ABFS_APPEND_ACTION = "write";
  public static final String ABFS_READ_ACTION = "read";

  /** Mapping. **/
  public static final Map<String, String> AbfsAuthorizerActionsToPermissionsMap_PreDec19Version =
      new Hashtable<String, String>() {{
        put(ABFS_RENAME_SOURCE_ACTION, ABFS_SAS_PERMISSION_WRITE);
        put(ABFS_RENAME_DESTINATION_ACTION, ABFS_SAS_PERMISSION_WRITE);
        put(ABFS_CHECKACCESS_ACTION_PREFIX_ACTION, ABFS_SAS_PERMISSION_READ);
        put(ABFS_LISTSTATUS_ACTION, "r");
//            String.format("%s%s",ABFS_SAS_PERMISSION_READ,
//                ABFS_SAS_PERMISSION_EXECUTE));
        put(ABFS_DELETE_ACTION, ABFS_SAS_PERMISSION_DELETE);
        put(ABFS_CREATEFILE_ACTION, String.format(
            "%s%s%s",ABFS_SAS_PERMISSION_CREATE, ABFS_SAS_PERMISSION_WRITE,
            ABFS_SAS_PERMISSION_DELETE));
        put(ABFS_MKDIR_ACTION, ABFS_SAS_PERMISSION_CREATE);
        put(ABFS_GETACL_ACTION, ABFS_SAS_PERMISSION_READ);
        put(ABFS_GETFILESTATUS_ACTION, ABFS_SAS_PERMISSION_READ);
//        put(ABFS_SETACL_ACTION, String.format(
//            "%s%s%s",ABFS_SAS_PERMISSION_READ, ABFS_SAS_PERMISSION_WRITE,
//            ABFS_SAS_PERMISSION_EXECUTE));
        put(ABFS_SETACL_ACTION, "rwxd");
        put(ABFS_APPEND_ACTION, ABFS_SAS_PERMISSION_WRITE);
        put(ABFS_READ_ACTION, ABFS_SAS_PERMISSION_READ);
        put(ABFS_SETPERMISSION_ACTION, String.format(
            "%s%s%s",ABFS_SAS_PERMISSION_READ, ABFS_SAS_PERMISSION_WRITE,
            ABFS_SAS_PERMISSION_EXECUTE));
        put(ABFS_SETOWNER_ACTION, String.format(
            "%s%s%s",ABFS_SAS_PERMISSION_READ, ABFS_SAS_PERMISSION_WRITE,
            ABFS_SAS_PERMISSION_EXECUTE));
      }};

  public static final Map<String, String> AbfsAuthorizerActionsToPermissionsMap =
      new Hashtable<String, String>() {{
        put(ABFS_RENAME_SOURCE_ACTION, ABFS_SAS_PERMISSION_MOVE);
        put(ABFS_RENAME_DESTINATION_ACTION, ABFS_SAS_PERMISSION_DELETE);
        put(ABFS_CHECKACCESS_ACTION_PREFIX_ACTION, ABFS_SAS_PERMISSION_EXECUTE);
        put(ABFS_LISTSTATUS_ACTION, ABFS_SAS_PERMISSION_LIST);
        put(ABFS_DELETE_ACTION, ABFS_SAS_PERMISSION_DELETE);
        put(ABFS_CREATEFILE_ACTION, ABFS_SAS_PERMISSION_WRITE);
        put(ABFS_MKDIR_ACTION, ABFS_SAS_PERMISSION_WRITE);
        put(ABFS_GETACL_ACTION, ABFS_SAS_PERMISSION_EXECUTE);
        put(ABFS_GETFILESTATUS_ACTION, ABFS_SAS_PERMISSION_WRITE);
        put(ABFS_SETACL_ACTION, ABFS_SAS_PERMISSION_CHANGE_PERMISSION);
        put(ABFS_APPEND_ACTION, String.format("%s%s",
            ABFS_SAS_PERMISSION_WRITE,
            ABFS_SAS_PERMISSION_EXECUTE));
        put(ABFS_READ_ACTION, ABFS_SAS_PERMISSION_READ);
        put(ABFS_SETPERMISSION_ACTION, ABFS_SAS_PERMISSION_CHANGE_PERMISSION);
        put(ABFS_SETOWNER_ACTION, ABFS_SAS_PERMISSION_SETOWNER);
      }};

  public static final Set<String> VALID_SAS_REST_API_VERSIONS =
      new HashSet<String>() {{
        add("2018-11-09");
        //add("2019-12-12");
      }};
}

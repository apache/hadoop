/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Set of constants used in Ozone implementation.
 */
@InterfaceAudience.Private
public final class OzoneConsts {


  public static final String STORAGE_DIR = "scm";
  public static final String SCM_ID = "scmUuid";

  public static final String OZONE_SIMPLE_ROOT_USER = "root";
  public static final String OZONE_SIMPLE_HDFS_USER = "hdfs";

  public static final String STORAGE_ID = "storageID";
  public static final String DATANODE_UUID = "datanodeUuid";
  public static final String CLUSTER_ID = "clusterID";
  public static final String LAYOUTVERSION = "layOutVersion";
  public static final String CTIME = "ctime";
  /*
   * BucketName length is used for both buckets and volume lengths
   */
  public static final int OZONE_MIN_BUCKET_NAME_LENGTH = 3;
  public static final int OZONE_MAX_BUCKET_NAME_LENGTH = 63;

  public static final String OZONE_ACL_USER_TYPE = "user";
  public static final String OZONE_ACL_GROUP_TYPE = "group";
  public static final String OZONE_ACL_WORLD_TYPE = "world";

  public static final String OZONE_ACL_READ = "r";
  public static final String OZONE_ACL_WRITE = "w";
  public static final String OZONE_ACL_READ_WRITE = "rw";
  public static final String OZONE_ACL_WRITE_READ = "wr";

  public static final String OZONE_DATE_FORMAT =
      "EEE, dd MMM yyyy HH:mm:ss zzz";
  public static final String OZONE_TIME_ZONE = "GMT";

  public static final String OZONE_COMPONENT = "component";
  public static final String OZONE_FUNCTION  = "function";
  public static final String OZONE_RESOURCE = "resource";
  public static final String OZONE_USER = "user";
  public static final String OZONE_REQUEST = "request";

  public static final String OZONE_URI_SCHEME = "o3";
  public static final String OZONE_HTTP_SCHEME = "http";
  public static final String OZONE_URI_DELIMITER = "/";

  public static final String CONTAINER_EXTENSION = ".container";
  public static final String CONTAINER_META = ".meta";

  // Refer to {@link ContainerReader} for container storage layout on disk.
  public static final String CONTAINER_PREFIX  = "containers";
  public static final String CONTAINER_META_PATH = "metadata";
  public static final String CONTAINER_TEMPORARY_CHUNK_PREFIX = "tmp";
  public static final String CONTAINER_CHUNK_NAME_DELIMITER = ".";
  public static final String CONTAINER_ROOT_PREFIX = "repository";

  public static final String FILE_HASH = "SHA-256";
  public final static String CHUNK_OVERWRITE = "OverWriteRequested";

  public static final int CHUNK_SIZE = 1 * 1024 * 1024; // 1 MB
  public static final long KB = 1024L;
  public static final long MB = KB * 1024L;
  public static final long GB = MB * 1024L;
  public static final long TB = GB * 1024L;

  /**
   * level DB names used by SCM and data nodes.
   */
  public static final String CONTAINER_DB_SUFFIX = "container.db";
  public static final String SCM_CONTAINER_DB = "scm-" + CONTAINER_DB_SUFFIX;
  public static final String DN_CONTAINER_DB = "-dn-"+ CONTAINER_DB_SUFFIX;
  public static final String BLOCK_DB = "block.db";
  public static final String OPEN_CONTAINERS_DB = "openContainers.db";
  public static final String DELETED_BLOCK_DB = "deletedBlock.db";
  public static final String OM_DB_NAME = "om.db";

  public static final String STORAGE_DIR_CHUNKS = "chunks";

  /**
   * Supports Bucket Versioning.
   */
  public enum Versioning {
    NOT_DEFINED, ENABLED, DISABLED;

    public static Versioning getVersioning(boolean versioning) {
      return versioning ? ENABLED : DISABLED;
    }
  }

  /**
   * Ozone handler types.
   */
  public static final String OZONE_HANDLER_DISTRIBUTED = "distributed";
  public static final String OZONE_HANDLER_LOCAL = "local";

  public static final String DELETING_KEY_PREFIX = "#deleting#";
  public static final String DELETED_KEY_PREFIX = "#deleted#";
  public static final String DELETE_TRANSACTION_KEY_PREFIX = "#delTX#";
  public static final String OPEN_KEY_PREFIX = "#open#";
  public static final String OPEN_KEY_ID_DELIMINATOR = "#";

  /**
   * OM LevelDB prefixes.
   *
   * OM DB stores metadata as KV pairs with certain prefixes,
   * prefix is used to improve the performance to get related
   * metadata.
   *
   * OM DB Schema:
   *  ----------------------------------------------------------
   *  |  KEY                                     |     VALUE   |
   *  ----------------------------------------------------------
   *  | $userName                                |  VolumeList |
   *  ----------------------------------------------------------
   *  | /#volumeName                             |  VolumeInfo |
   *  ----------------------------------------------------------
   *  | /#volumeName/#bucketName                 |  BucketInfo |
   *  ----------------------------------------------------------
   *  | /volumeName/bucketName/keyName           |  KeyInfo    |
   *  ----------------------------------------------------------
   *  | #deleting#/volumeName/bucketName/keyName |  KeyInfo    |
   *  ----------------------------------------------------------
   */
  public static final String OM_VOLUME_PREFIX = "/#";
  public static final String OM_BUCKET_PREFIX = "/#";
  public static final String OM_KEY_PREFIX = "/";
  public static final String OM_USER_PREFIX = "$";

  /**
   * Max OM Quota size of 1024 PB.
   */
  public static final long MAX_QUOTA_IN_BYTES = 1024L * 1024 * TB;

  /**
   * Max number of keys returned per list buckets operation.
   */
  public static final int MAX_LISTBUCKETS_SIZE  = 1024;

  /**
   * Max number of keys returned per list keys operation.
   */
  public static final int MAX_LISTKEYS_SIZE  = 1024;

  /**
   * Max number of volumes returned per list volumes operation.
   */
  public static final int MAX_LISTVOLUMES_SIZE = 1024;

  public static final int INVALID_PORT = -1;


  // The ServiceListJSONServlet context attribute where OzoneManager
  // instance gets stored.
  public static final String OM_CONTEXT_ATTRIBUTE = "ozone.om";

  private OzoneConsts() {
    // Never Constructed
  }

  // YAML fields for .container files
  public static final String CONTAINER_ID = "containerID";
  public static final String CONTAINER_TYPE = "containerType";
  public static final String STATE = "state";
  public static final String METADATA = "metadata";
  public static final String MAX_SIZE_GB = "maxSizeGB";
  public static final String METADATA_PATH = "metadataPath";
  public static final String CHUNKS_PATH = "chunksPath";
  public static final String CONTAINER_DB_TYPE = "containerDBType";
  public static final String CHECKSUM = "checksum";
}

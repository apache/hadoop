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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.thirdparty.io.grpc.Context;
import org.apache.ratis.thirdparty.io.grpc.Metadata;

import static org.apache.ratis.thirdparty.io.grpc.Metadata.ASCII_STRING_MARSHALLER;

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
  public static final String OZONE_ACL_IP_TYPE = "ip";

  public static final String OZONE_ACL_READ = "r";
  public static final String OZONE_ACL_WRITE = "w";
  public static final String OZONE_ACL_READ_WRITE = "rw";
  public static final String OZONE_ACL_WRITE_READ = "wr";
  public static final String OZONE_ACL_DELETE = "d";
  public static final String OZONE_ACL_LIST = "l";
  public static final String OZONE_ACL_ALL = "a";
  public static final String OZONE_ACL_NONE = "n";
  public static final String OZONE_ACL_CREATE = "c";
  public static final String OZONE_ACL_READ_ACL = "x";
  public static final String OZONE_ACL_WRITE_ACL = "y";


  public static final String OZONE_DATE_FORMAT =
      "EEE, dd MMM yyyy HH:mm:ss zzz";
  public static final String OZONE_TIME_ZONE = "GMT";

  public static final String OZONE_COMPONENT = "component";
  public static final String OZONE_FUNCTION  = "function";
  public static final String OZONE_RESOURCE = "resource";
  public static final String OZONE_USER = "user";
  public static final String OZONE_REQUEST = "request";

  // Ozone File System scheme
  public static final String OZONE_URI_SCHEME = "o3fs";

  public static final String OZONE_RPC_SCHEME = "o3";
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
  public static final String MD5_HASH = "MD5";
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
  public static final String PIPELINE_DB_SUFFIX = "pipeline.db";
  public static final String SCM_CONTAINER_DB = "scm-" + CONTAINER_DB_SUFFIX;
  public static final String SCM_PIPELINE_DB = "scm-" + PIPELINE_DB_SUFFIX;
  public static final String DN_CONTAINER_DB = "-dn-"+ CONTAINER_DB_SUFFIX;
  public static final String DELETED_BLOCK_DB = "deletedBlock.db";
  public static final String OM_DB_NAME = "om.db";
  public static final String OM_DB_CHECKPOINTS_DIR_NAME = "om.db.checkpoints";
  public static final String OZONE_MANAGER_TOKEN_DB_NAME = "om-token.db";
  public static final String SCM_DB_NAME = "scm.db";

  public static final String STORAGE_DIR_CHUNKS = "chunks";
  public static final String OZONE_DB_CHECKPOINT_REQUEST_FLUSH =
      "flushBeforeCheckpoint";

  /**
   * Supports Bucket Versioning.
   */
  public enum Versioning {
    NOT_DEFINED, ENABLED, DISABLED;

    public static Versioning getVersioning(boolean versioning) {
      return versioning ? ENABLED : DISABLED;
    }
  }

  public static final String DELETING_KEY_PREFIX = "#deleting#";
  public static final String DELETED_KEY_PREFIX = "#deleted#";
  public static final String DELETE_TRANSACTION_KEY_PREFIX = "#delTX#";
  public static final String BLOCK_COMMIT_SEQUENCE_ID_PREFIX = "#BCSID";

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

  public static final String OM_KEY_PREFIX = "/";
  public static final String OM_USER_PREFIX = "$";
  public static final String OM_S3_PREFIX ="S3:";
  public static final String OM_S3_VOLUME_PREFIX = "s3";

  /**
   *   Max chunk size limit.
   */
  public static final int OZONE_SCM_CHUNK_MAX_SIZE = 32 * 1024 * 1024;


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
  public static final String MAX_SIZE = "maxSize";
  public static final String METADATA_PATH = "metadataPath";
  public static final String CHUNKS_PATH = "chunksPath";
  public static final String CONTAINER_DB_TYPE = "containerDBType";
  public static final String CHECKSUM = "checksum";
  public static final String ORIGIN_PIPELINE_ID = "originPipelineId";
  public static final String ORIGIN_NODE_ID = "originNodeId";

  // Supported store types.
  public static final String OZONE = "ozone";
  public static final String S3 = "s3";

  // For OM Audit usage
  public static final String VOLUME = "volume";
  public static final String BUCKET = "bucket";
  public static final String KEY = "key";
  public static final String QUOTA = "quota";
  public static final String QUOTA_IN_BYTES = "quotaInBytes";
  public static final String CLIENT_ID = "clientID";
  public static final String OWNER = "owner";
  public static final String ADMIN = "admin";
  public static final String USERNAME = "username";
  public static final String PREV_KEY = "prevKey";
  public static final String START_KEY = "startKey";
  public static final String MAX_KEYS = "maxKeys";
  public static final String PREFIX = "prefix";
  public static final String KEY_PREFIX = "keyPrefix";
  public static final String ACLS = "acls";
  public static final String USER_ACL = "userAcl";
  public static final String ADD_ACLS = "addAcls";
  public static final String REMOVE_ACLS = "removeAcls";
  public static final String MAX_NUM_OF_BUCKETS = "maxNumOfBuckets";
  public static final String TO_KEY_NAME = "toKeyName";
  public static final String STORAGE_TYPE = "storageType";
  public static final String IS_VERSION_ENABLED = "isVersionEnabled";
  public static final String CREATION_TIME = "creationTime";
  public static final String DATA_SIZE = "dataSize";
  public static final String REPLICATION_TYPE = "replicationType";
  public static final String REPLICATION_FACTOR = "replicationFactor";
  public static final String KEY_LOCATION_INFO = "keyLocationInfo";
  public static final String MULTIPART_LIST = "multipartList";
  public static final String UPLOAD_ID = "uploadID";
  public static final String PART_NUMBER_MARKER = "partNumberMarker";
  public static final String MAX_PARTS = "maxParts";



  // For OM metrics saving to a file
  public static final String OM_METRICS_FILE = "omMetrics";
  public static final String OM_METRICS_TEMP_FILE = OM_METRICS_FILE + ".tmp";

  // For Multipart upload
  public static final int OM_MULTIPART_MIN_SIZE = 5 * 1024 * 1024;

  // GRPC block token metadata header and context key
  public static final String OZONE_BLOCK_TOKEN = "blocktoken";
  public static final Context.Key<UserGroupInformation> UGI_CTX_KEY =
      Context.key("UGI");

  public static final Metadata.Key<String> OBT_METADATA_KEY =
      Metadata.Key.of(OZONE_BLOCK_TOKEN, ASCII_STRING_MARSHALLER);
  public static final Metadata.Key<String> USER_METADATA_KEY =
      Metadata.Key.of(OZONE_USER, ASCII_STRING_MARSHALLER);

  public static final String RPC_PORT = "RPC";

  // Default OMServiceID for OM Ratis servers to use as RaftGroupId
  public static final String OM_SERVICE_ID_DEFAULT = "omServiceIdDefault";

  // Dummy OMNodeID for OM Clients to use for a non-HA OM setup
  public static final String OM_NODE_ID_DUMMY = "omNodeIdDummy";

  // OM Ratis snapshot file to store the last applied index
  public static final String OM_RATIS_SNAPSHOT_INDEX = "ratisSnapshotIndex";
}

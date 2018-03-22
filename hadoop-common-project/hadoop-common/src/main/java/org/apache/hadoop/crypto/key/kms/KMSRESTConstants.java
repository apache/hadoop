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
package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * KMS REST and JSON constants and utility methods for the KMSServer.
 */
@InterfaceAudience.Private
public class KMSRESTConstants {

  public static final String SERVICE_VERSION = "/v1";
  public static final String KEY_RESOURCE = "key";
  public static final String KEYS_RESOURCE = "keys";
  public static final String KEYS_METADATA_RESOURCE = KEYS_RESOURCE +
      "/metadata";
  public static final String KEYS_NAMES_RESOURCE = KEYS_RESOURCE + "/names";
  public static final String KEY_VERSION_RESOURCE = "keyversion";
  public static final String METADATA_SUB_RESOURCE = "_metadata";
  public static final String VERSIONS_SUB_RESOURCE = "_versions";
  public static final String EEK_SUB_RESOURCE = "_eek";
  public static final String CURRENT_VERSION_SUB_RESOURCE = "_currentversion";
  public static final String INVALIDATECACHE_RESOURCE = "_invalidatecache";
  public static final String REENCRYPT_BATCH_SUB_RESOURCE = "_reencryptbatch";

  public static final String KEY = "key";
  public static final String EEK_OP = "eek_op";
  public static final String EEK_GENERATE = "generate";
  public static final String EEK_DECRYPT = "decrypt";
  public static final String EEK_NUM_KEYS = "num_keys";
  public static final String EEK_REENCRYPT = "reencrypt";

  public static final String IV_FIELD = "iv";
  public static final String NAME_FIELD = "name";
  public static final String CIPHER_FIELD = "cipher";
  public static final String LENGTH_FIELD = "length";
  public static final String DESCRIPTION_FIELD = "description";
  public static final String ATTRIBUTES_FIELD = "attributes";
  public static final String CREATED_FIELD = "created";
  public static final String VERSIONS_FIELD = "versions";
  public static final String MATERIAL_FIELD = "material";
  public static final String VERSION_NAME_FIELD = "versionName";
  public static final String ENCRYPTED_KEY_VERSION_FIELD =
      "encryptedKeyVersion";

  public static final String ERROR_EXCEPTION_JSON = "exception";
  public static final String ERROR_MESSAGE_JSON = "message";

}

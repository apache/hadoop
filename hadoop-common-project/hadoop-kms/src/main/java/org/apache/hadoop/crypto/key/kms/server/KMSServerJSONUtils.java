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
package org.apache.hadoop.crypto.key.kms.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.KMSRESTConstants;
import org.apache.hadoop.util.KMSUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON utility methods for the KMS.
 */
@InterfaceAudience.Private
public class KMSServerJSONUtils {

  @SuppressWarnings("unchecked")
  public static List toJSON(List<KeyProvider.KeyVersion> keyVersions) {
    List json = new ArrayList();
    if (keyVersions != null) {
      for (KeyProvider.KeyVersion version : keyVersions) {
        json.add(KMSUtil.toJSON(version));
      }
    }
    return json;
  }

  @SuppressWarnings("unchecked")
  public static Map toJSON(String keyName, KeyProvider.Metadata meta) {
    Map json = new LinkedHashMap();
    if (meta != null) {
      json.put(KMSRESTConstants.NAME_FIELD, keyName);
      json.put(KMSRESTConstants.CIPHER_FIELD, meta.getCipher());
      json.put(KMSRESTConstants.LENGTH_FIELD, meta.getBitLength());
      json.put(KMSRESTConstants.DESCRIPTION_FIELD, meta.getDescription());
      json.put(KMSRESTConstants.ATTRIBUTES_FIELD, meta.getAttributes());
      json.put(KMSRESTConstants.CREATED_FIELD,
          meta.getCreated().getTime());
      json.put(KMSRESTConstants.VERSIONS_FIELD,
          (long) meta.getVersions());
    }
    return json;
  }

  @SuppressWarnings("unchecked")
  public static List toJSON(String[] keyNames, KeyProvider.Metadata[] metas) {
    List json = new ArrayList();
    for (int i = 0; i < keyNames.length; i++) {
      json.add(toJSON(keyNames[i], metas[i]));
    }
    return json;
  }
}

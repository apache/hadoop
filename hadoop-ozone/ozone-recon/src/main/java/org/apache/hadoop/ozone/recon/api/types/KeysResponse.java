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
package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Class that represents the API Response structure of Keys within a container.
 */
public class KeysResponse {
  /**
   * Contains a map with total count of keys inside the given container and a
   * list of keys with metadata.
   */
  @JsonProperty("data")
  private KeysResponseData keysResponseData;

  public KeysResponse() {
    this(0, new ArrayList<>());
  }

  public KeysResponse(long totalCount,
                      Collection<KeyMetadata> keys) {
    this.keysResponseData =
        new KeysResponseData(totalCount, keys);
  }

  public String toJsonString() {
    try {
      return JsonUtils.toJsonString(this);
    } catch (IOException ignored) {
      return null;
    }
  }

  public KeysResponseData getKeysResponseData() {
    return keysResponseData;
  }

  public void setKeysResponseData(KeysResponseData keysResponseData) {
    this.keysResponseData = keysResponseData;
  }

  /**
   * Class that encapsulates the data presented in Keys API Response.
   */
  public static class KeysResponseData {
    /**
     * Total count of the keys.
     */
    @JsonProperty("totalCount")
    private long totalCount;

    /**
     * An array of keys.
     */
    @JsonProperty("keys")
    private Collection<KeyMetadata> keys;

    KeysResponseData(long totalCount, Collection<KeyMetadata> keys) {
      this.totalCount = totalCount;
      this.keys = keys;
    }

    public long getTotalCount() {
      return totalCount;
    }

    public Collection<KeyMetadata> getKeys() {
      return keys;
    }
  }
}

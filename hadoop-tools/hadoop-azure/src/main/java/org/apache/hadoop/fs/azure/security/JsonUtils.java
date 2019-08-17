/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azure.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.util.JsonSerialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Utility class to parse JSON.
 */
public final class JsonUtils {
  public static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);

  private JsonUtils() {
  }

  public static Map<?, ?> parse(final String jsonString) throws IOException {
    try {
      return JsonSerialization.mapReader().readValue(jsonString);
    } catch (Exception e) {
      LOG.debug("JSON Parsing exception: {} while parsing {}", e.getMessage(),
          jsonString);
      if (jsonString.toLowerCase(Locale.ENGLISH).contains("server error")) {
        LOG.error(
            "Internal Server Error was encountered while making a request");
      }
      throw new IOException("JSON Parsing Error: " + e.getMessage(), e);
    }
  }
}

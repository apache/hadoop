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
package org.apache.hadoop.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Json serialization util class based on {@link Gson}
 */
@InterfaceAudience.Private
public class GsonSerialization {

  private static final Gson GSON =
      new GsonBuilder().setPrettyPrinting().create();

  /**
   * Returns a {@link Gson} object which enables pretty printing.
   */
  public static Gson prettyWriter() {
    return GSON;
  }

  /**
   * Returns a {@link Gson} object which can be used to convert json to object.
   */
  public static Gson reader() {
    return GSON;
  }
}

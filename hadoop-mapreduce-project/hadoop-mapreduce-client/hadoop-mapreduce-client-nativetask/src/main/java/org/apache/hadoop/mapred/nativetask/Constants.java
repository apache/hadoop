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
package org.apache.hadoop.mapred.nativetask;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class Constants {

  public static final String MAP_SORT_CLASS = "map.sort.class";
  public static final String MAPRED_COMBINER_CLASS = "mapred.combiner.class";

  public static final String NATIVE_LOG_DEVICE = "native.log.device";
  public static final String NATIVE_HADOOP_VERSION = "native.hadoop.version";

  public static final String NATIVE_PROCESSOR_BUFFER_KB = "native.processor.buffer.kb";
  public static final int NATIVE_PROCESSOR_BUFFER_KB_DEFAULT = 64;
  public static final int NATIVE_ASYNC_PROCESSOR_BUFFER_KB_DEFAULT = 1024;

  public static final String NATIVE_STATUS_UPDATE_INTERVAL = "native.update.interval";
  public static final int NATIVE_STATUS_UPDATE_INTERVAL_DEFVAL = 3000;

  public static final String SERIALIZATION_FRAMEWORK = "SerializationFramework";
  public static final int SIZEOF_PARTITION_LENGTH = 4;
  public static final int SIZEOF_KEY_LENGTH = 4;
  public static final int SIZEOF_VALUE_LENGTH = 4;
  public static final int SIZEOF_KV_LENGTH = SIZEOF_KEY_LENGTH + SIZEOF_VALUE_LENGTH;
  
  public static final String NATIVE_CLASS_LIBRARY = "native.class.library";
  public static final String NATIVE_CLASS_LIBRARY_CUSTOM = "native.class.library.custom";
  public static final String NATIVE_CLASS_LIBRARY_BUILDIN = "native.class.library.buildin";
  public static final String NATIVE_MAPOUT_KEY_COMPARATOR = "native.map.output.key.comparator";
}

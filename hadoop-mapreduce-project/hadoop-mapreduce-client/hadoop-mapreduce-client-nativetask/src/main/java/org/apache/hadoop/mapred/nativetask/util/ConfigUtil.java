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
package org.apache.hadoop.mapred.nativetask.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public abstract class ConfigUtil {
  public static byte[][] toBytes(Configuration conf) {
    List<byte[]> nativeConfigs = new ArrayList<byte[]>();
    for (Map.Entry<String, String> e : conf) {
      nativeConfigs.add(e.getKey().getBytes(Charsets.UTF_8));
      nativeConfigs.add(e.getValue().getBytes(Charsets.UTF_8));
    }
    return nativeConfigs.toArray(new byte[nativeConfigs.size()][]);
  }
  
  public static String booleansToString(boolean[] value) {
    StringBuilder sb = new StringBuilder();
    for (boolean b: value) {
      sb.append(b ? 1 : 0);
    }
    return sb.toString();
  }
}

/*
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

package org.apache.hadoop.metrics2.lib;

import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Generates predictable and user-friendly unique names
 */
@InterfaceAudience.Private
public class UniqueNames {

  static class Count {
    final String baseName;
    int value;

    Count(String name, int value) {
      baseName = name;
      this.value = value;
    }
  }

  static final Joiner joiner = Joiner.on('-');
  final Map<String, Count> map = Maps.newHashMap();

  public synchronized String uniqueName(String name) {
    Count c = map.get(name);
    if (c == null) {
      c = new Count(name, 0);
      map.put(name, c);
      return name;
    }
    if (!c.baseName.equals(name)) c = new Count(name, 0);
    do {
      String newName = joiner.join(name, ++c.value);
      Count c2 = map.get(newName);
      if (c2 == null) {
        map.put(newName, c);
        return newName;
      }
      // handle collisons, assume to be rare cases,
      // eg: people explicitly passed in name-\d+ names.
    } while (true);
  }
}

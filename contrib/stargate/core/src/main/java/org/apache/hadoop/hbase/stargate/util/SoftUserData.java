/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.stargate.util;

import java.util.Map;

import org.apache.hadoop.hbase.stargate.User;
import org.apache.hadoop.hbase.util.SoftValueMap;

/**
 * Provides a softmap backed collection of user data. The collection can be
 * reclaimed by the garbage collector at any time when under heap pressure.
 */
public class SoftUserData extends UserData {

  static final Map<User,UserData> map = new SoftValueMap<User,UserData>();

  public static synchronized UserData get(final User user) {
    UserData data = map.get(user);
    if (data == null) {
      data = new UserData();
      map.put(user, data);
    }
    return data;
  }

  public static synchronized UserData put(final User user,
      final UserData data) {
    return map.put(user, data);
  }

}

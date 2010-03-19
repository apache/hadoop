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

import java.util.ArrayList;

/**
 * Generic storage for per user information.
 */
public class UserData {

  public static final int TOKENBUCKET = 0;

  ArrayList<Object> data = new ArrayList<Object>();

  public synchronized boolean has(final int sel) {
    try {
      return data.get(sel) != null;
    } catch (IndexOutOfBoundsException e) {
      return false;
    }
  }

  public synchronized Object get(final int sel) {
    try {
      return data.get(sel);
    } catch (IndexOutOfBoundsException e) {
      return null;
    }
  }

  public synchronized Object put(final int sel, final Object o) {
    Object old = null;
    try {
      old = data.get(sel);
    } catch (IndexOutOfBoundsException e) {
      // do nothing
    }
    data.set(sel, o);
    return old;
  }

  public synchronized Object remove(int sel) {
    return put(sel, null);
  }

}

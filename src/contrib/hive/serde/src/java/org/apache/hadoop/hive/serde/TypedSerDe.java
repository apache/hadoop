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

package org.apache.hadoop.hive.serde;

import org.apache.hadoop.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;

public abstract class TypedSerDe implements SerDe {

  private Object cachedObj;
  protected Class<?> type;


  public TypedSerDe(Class<?> argType) throws SerDeException {
    try {
      type = argType;
      cachedObj = type.newInstance();
      //      hm = new HashMap<Enum, LongWritable> ();
      //      hm.put(Counter.READ_ERRORS, Long.valueOfWritable(0));
      //      hm.put(Counter.WRITE_ERRORS, Long.valueOfWritable(0));
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }
  /*
    public static enum Counter {READ_ERRORS, WRITE_ERRORS}
    HashMap<Enum, LongWritable> hm;
    public Map<Enum, Long> getStats() {
    HashMap<Enum, Long> ret = new HashMap<Enum, Long> ();
    for(Enum one: hm.keySet()) {
    ret.put(one, Long.valueOf(hm.get(one).get()));
    }
    return(ret);
    }
  */

  public Object deserialize(Writable field) throws SerDeException {
    // used to return the cached object. except, the cached
    // object has references that are hard/expensive to clear
    // unless all objects in hive support some kind of a 'clear' function
    try {
      return type.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Writable serialize(Object obj) throws SerDeException {
    // can insert type-checking here
    return (null);
  }
}

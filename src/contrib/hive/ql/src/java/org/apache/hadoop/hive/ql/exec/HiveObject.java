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

package org.apache.hadoop.hive.ql.exec;

import java.io.*;
import java.util.*;

import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.utils.ByteStream;

/**
 * Data for each row is passed around as HiveObjects in Hive
 */

public abstract class HiveObject {

  protected Object javaObject;

  protected boolean isNull;

  /**
   * @param expr a well formed expression nesting within this Hive Object
   * @return field handler that can be used in a subsequent get() call
   */
  public abstract SerDeField getFieldFromExpression(String expr) throws HiveException;

  /**
   * @param field obtained using call to getFieldFromExpression
   * @return another subObject
   */
  public abstract HiveObject get(SerDeField field) throws HiveException;

  /**
   * @return get the current HiveObject as a Java Object
   */
  public Object getJavaObject() throws HiveException {
    if (isNull) return null;
    return javaObject;
  }

  /**
   * @return get isNull
   */
  public boolean getIsNull() {
    return isNull;
  }

  public void setIsNull(boolean isNull) {
    this.isNull = isNull;
  }

  /**
   * @return list of top level fields in this Hive Object
   */
  public abstract List<SerDeField> getFields() throws HiveException;

  /**
   * Used to detect base case of object hierarchy
   * @return true if the Object encapsulates a Hive Primitive Object. False otherwise
   */
  public abstract boolean isPrimitive();

  public abstract int hashCode();
  
  public abstract boolean equals(Object other);
  
  public String toString () {
    try {
      HiveObjectSerializer hos = new NaiiveSerializer();
      ByteStream.Output bos = new ByteStream.Output ();
      hos.serialize(this, new DataOutputStream(bos));
      return new String(bos.getData(), 0, bos.getCount(), "UTF-8");
    } catch (Exception e) {
      return ("Exception:  "+e.getMessage());
    }
  }

  public static final ArrayList<SerDeField> nlist = new ArrayList<SerDeField> (0);
}

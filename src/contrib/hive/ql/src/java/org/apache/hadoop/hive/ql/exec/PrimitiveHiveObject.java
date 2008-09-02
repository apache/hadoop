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

import java.util.*;

import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Encapsulation for a primitive Java Object
 */

public final class PrimitiveHiveObject extends HiveObject {

  public PrimitiveHiveObject(Object javaObject) {

    this.javaObject = javaObject;
  }

  public SerDeField getFieldFromExpression(String expr) throws HiveException {
    throw new HiveException ("Illegal call getFieldFromExpression() on Primitive Object");
  }

  public HiveObject get(SerDeField field) throws HiveException {
    throw new HiveException ("Illegal call get() on Primitive Object");
  }

  public List<SerDeField> getFields() throws HiveException {
    throw new HiveException ("Illegal call getFields() on Primitive Object");
  }

  public boolean isPrimitive() { return true; }

  @Override
  public String toString () {
    return (javaObject == null ? "" : javaObject.toString());
  }

  @Override
  public int hashCode() {
    return (javaObject == null ? 0 : javaObject.hashCode());
  }

  @Override
  public boolean equals(Object other) {
    if (! (other instanceof PrimitiveHiveObject)) return false;
    return javaObject == null ? ((PrimitiveHiveObject)other).javaObject == null
        : javaObject.equals(((PrimitiveHiveObject)other).javaObject);
  }
}

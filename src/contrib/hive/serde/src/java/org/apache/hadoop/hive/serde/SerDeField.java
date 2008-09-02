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

import java.lang.reflect.*;

/**
 * A Hive Field knows how to get a component of a given object
 * Implementations are likely to be tied to the corresponding
 * SerDe implementation.
 *
 * Hive allows base types (Java numbers,string,boolean types), maps and lists
 * and complex types created out arbitrary nesting and composition of these.
 *
 */

public interface SerDeField {

  /**
   * Get the field from an object. The object must be of valid type
   * Will return null if the field is not defined.
   * @param obj The object from which to get the Field
   * @return the object corresponding to the Field
   *
   */
  public Object get(Object obj) throws SerDeException;

    /**
     * Does this Field represent a List?
     * @return true is the Field represents a list. False otherwise
     */
    public boolean isList();

    /**
     * Does this Field represent a Map?
     * @return true is the Field represents a map. False otherwise
     */
    public boolean isMap();

    /**
     * Does this Field represent a primitive object?
     * @return false if map or list. false if object has subfields (composition).
     * true otherwise
     */
    public boolean isPrimitive();

    /**
     * Get the type of the object represented by this Field. ie. all the
     * objects returned by the get() call will be of the returned type
     * @return Class of objects represented by this Field.
     */
    public Class getType();

    /**
     * Type of List member objects if List
     */
    public Class getListElementType();

    /**
     * Type of Map Key type if Map
     */
    public Class getMapKeyType();

    /**
     * Type of Map Value type if Map
     */
    public Class getMapValueType();

    /**
     * Name of this field
     */
    public String getName();
}

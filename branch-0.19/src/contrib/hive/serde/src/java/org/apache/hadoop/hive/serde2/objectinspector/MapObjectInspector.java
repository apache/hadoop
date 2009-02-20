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
package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.Map;


public interface MapObjectInspector extends ObjectInspector {

  // ** Methods that does not need a data object **
  // Map Type
  public ObjectInspector getMapKeyObjectInspector();

  public ObjectInspector getMapValueObjectInspector();

  // ** Methods that need a data object **
  // In this function, key has to be of the same structure as the Map expects.
  // Most cases key will be primitive type, so it's OK.
  // In rare cases that key is not primitive, the user is responsible for defining 
  // the hashCode() and equals() methods of the key class.
  public Object getMapValueElement(Object data, Object key);

  /** returns null for data = null.
   */
  public Map<?,?> getMap(Object data);

}

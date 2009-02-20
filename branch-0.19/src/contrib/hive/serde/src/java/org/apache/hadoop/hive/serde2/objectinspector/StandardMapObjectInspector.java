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

/**
 * StandardMapObjectInspector works on map data that is stored as a Java Map object.
 * Note: the key object of the map must support equals and hashCode by itself.
 * 
 * We also plan to have a GeneralMapObjectInspector which can work on map with 
 * key objects that does not support equals and hashCode.  That will require us to 
 * store InspectableObject as the key, which will have overridden equals and hashCode 
 * methods.
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects, instead
 * of directly creating an instance of this class. 
 */
class StandardMapObjectInspector implements MapObjectInspector {

  ObjectInspector mapKeyObjectInspector;
  ObjectInspector mapValueObjectInspector;
  
  /** Call ObjectInspectorFactory.getStandardMapObjectInspector instead.
   */
  protected StandardMapObjectInspector(ObjectInspector mapKeyObjectInspector, ObjectInspector mapValueObjectInspector) {
    this.mapKeyObjectInspector = mapKeyObjectInspector;
    this.mapValueObjectInspector = mapValueObjectInspector;
  }

  // without data
  public ObjectInspector getMapKeyObjectInspector() {
    return mapKeyObjectInspector;
  }
  public ObjectInspector getMapValueObjectInspector() {
    return mapValueObjectInspector;
  }

  // with data
  // TODO: Now we assume the key Object supports hashCode and equals functions.
  public Object getMapValueElement(Object data, Object key) {
    if (data == null || key == null) return null;
    Map<?,?> map = (Map<?,?>)data;
    return map.get(key);
  }
  int getMapSize(Object data) {
    if (data == null) return -1;
    Map<?,?> map = (Map<?,?>)data;
    return map.size();
  }
  public Map<?,?> getMap(Object data) {
    if (data == null) return null;
    Map<?,?> map = (Map<?,?>)data;
    return map;
  }

  public final Category getCategory() {
    return Category.MAP;
  }

  public String getTypeName() {
    return org.apache.hadoop.hive.serde.Constants.MAP_TYPE_NAME 
        + "<" + mapKeyObjectInspector.getTypeName() + "," 
        + mapValueObjectInspector.getTypeName() + ">";
  }

}

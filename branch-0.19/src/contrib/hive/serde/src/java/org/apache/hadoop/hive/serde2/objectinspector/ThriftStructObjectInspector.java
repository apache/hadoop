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

/**
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects, instead
 * of directly creating an instance of this class. 
 */
class ThriftStructObjectInspector extends
    ReflectionStructObjectInspector {

  public boolean shouldIgnoreField(String name) {
    return "__isset".equals(name);
  }
  
  public boolean equals(Object b) {
    if (this == b) return true;
    if (!b.getClass().equals(this.getClass())) {
      return false;
    }
    ThriftStructObjectInspector bInspector = (ThriftStructObjectInspector)b;
    return objectClass.equals(bInspector.objectClass);
  }

  public int hashCode() {
    return 7 * objectClass.hashCode(); 
  }
    
}

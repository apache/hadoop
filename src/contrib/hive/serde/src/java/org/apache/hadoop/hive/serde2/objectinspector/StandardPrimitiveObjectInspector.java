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
 * PrimitiveObjectInspector works on primitive data that is stored as a Primitive Java object.
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects, instead
 * of directly creating an instance of this class. 
 */
class StandardPrimitiveObjectInspector implements PrimitiveObjectInspector {

  Class<?> primitiveClass;
  
  /** Call ObjectInspectorFactory.getStandardPrimitiveObjectInspector instead.
   */
  protected StandardPrimitiveObjectInspector(Class<?> primitiveClass) {
    this.primitiveClass = primitiveClass;
  }

  public Class<?> getPrimitiveClass() {
    return primitiveClass;
  }

  public final Category getCategory() {
    return Category.PRIMITIVE;
  }

  public String getTypeName() {
    return ObjectInspectorUtils.getClassShortName(primitiveClass.getName());
  }

}

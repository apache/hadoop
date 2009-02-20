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
 * ObjectInspector helps us to look into the internal structure of a complex
 * object.
 *
 * A (probably configured) ObjectInspector instance stands for a specific type
 * and a specific way to store the data of that type in the memory.
 * 
 * For native java Object, we can directly access the internal structure through 
 * member fields and methods.  ObjectInspector is a way to delegate that functionality
 * away from the Object, so that we have more control on the behavior of those actions.
 * 
 * An efficient implementation of ObjectInspector should rely on factory, so that we can 
 * make sure the same ObjectInspector only has one instance.  That also makes sure
 * hashCode() and equals() methods of java.lang.Object directly works for ObjectInspector
 * as well.
 */
public interface ObjectInspector {

  public static enum Category {
    PRIMITIVE, LIST, MAP, STRUCT
  };

  /**
   * Returns the name of the data type that is inspected by this ObjectInspector.
   * This is used to display the type information to the user.
   * 
   * For primitive types, the type name is standardized.
   * For other types, the type name can be something like "list<int>", "map<int,string>",
   * java class names, or user-defined type names similar to typedef. 
   */
  public String getTypeName();
  
  /**
   * An ObjectInspector must inherit from one of the following interfaces
   * if getCategory() returns:
   * PRIMITIVE:  PrimitiveObjectInspector 
   * LIST:       ListObjectInspector 
   * MAP:        MapObjectInspector 
   * STRUCT:     StructObjectInspector 
   */
  public Category getCategory();

}

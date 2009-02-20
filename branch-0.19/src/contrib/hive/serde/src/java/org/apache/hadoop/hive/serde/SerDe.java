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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Writable;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import java.util.Properties;

/**
 * Generic Interface to be exported by all serialization/deserialization libraries
 * In addition to the interface below, all implementations are assume to have a ctor
 * that takes a single 'Table' object as argument.
 *
 */
public interface SerDe {

  // name of serialization scheme.
  //  public static final String SERIALIZATION_LIB = "serialization.lib";
  // what class structure is serialized
  //  public static final String SERIALIZATION_CLASS = "serialization.class";
  // additional info about serialization format
  //  public static final String SERIALIZATION_FORMAT = "serialization.format";

  public void initialize(Configuration job, Properties tbl) throws SerDeException;
  /**
   * Deserialize an object out of a Writable blob
   *
   * SerDe's can choose a serialization format of their choosing as long as it is of
   * type Writable. Two obvious examples are BytesWritable (binary serialized) and
   * Text.
   * @param blob The Writable object containing a serialized object
   * @return A Java object representing the contents in the blob.
   */
  public Object deserialize(Writable blob) throws SerDeException;

  /**
   * Serialize an object. Currently this is not required to be implemented
   */
  public Writable serialize(Object obj) throws SerDeException;

  /**
   * Emit as JSON for easy processing in scripting languages
   * @param obj The object to be emitted as JSON
   * @param hf  The field that the object corresponds to
   * @return a valid JSON string representing the contents of the object
   */
  public String toJSONString(Object obj, SerDeField hf)  throws SerDeException;

  /**
   * Get a collection of top level SerDeFields.
   * A SerDeFields allows Hive to extract a subcomponent of an object that may
   * be returned by this SerDe (from the deseriaize method).
   *
   * @param parentField the Field relative to which we want to get the subfields
   * If parentField is null - then the top level subfields are returned
   */
  public List<SerDeField> getFields(SerDeField parentField) throws SerDeException;

  /**
   * Get a Field Handler that corresponds to the field expression
   *
   * @param parentField The Field relative to which the expression is defined.
   * parentField is null if there is no parent field (evaluating expression
   * relative to root object)
   * @param fieldExpression A FieldExpression can be a fieldname with some
   * associated modifiers. Each SerDe can define it's own expression syntax
   * for getting access to fields. Hive provides helper class called ComplexSerDeField
   * that provides support for a default field expression syntax that is XPath
   * oriented.
   * @return A SerDeField that extract a subcomponent of an object denoted by
   * the fieldExpression
   */
  public SerDeField getFieldFromExpression(SerDeField parentField, String fieldExpression)
    throws SerDeException;

}

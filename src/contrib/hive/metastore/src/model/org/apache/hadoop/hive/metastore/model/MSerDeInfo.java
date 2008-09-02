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

package org.apache.hadoop.hive.metastore.model;

import java.util.Map;

public class MSerDeInfo {
  private String name;
  private String serializationFormat;
  private String serializationClass;
  private String serializationLib;
  private String fieldDelim;
  private String collectionItemDelim;
  private String mapKeyDelim;
  private String lineDelim;
  private Map<String, String> parameters;

  /**
   * @param name
   * @param serializationFormat
   * @param serializationClass
   * @param serializationLib
   * @param fieldDelim
   * @param collectionItemDelim
   * @param mapKeyDelim
   * @param lineDelim
   * @param parameters
   */
  public MSerDeInfo(String name, String serializationFormat, String serializationClass,
      String serializationLib, String fieldDelim, String collectionItemDelim, String mapKeyDelim,
      String lineDelim, Map<String, String> parameters) {
    this.name = name;
    this.serializationFormat = serializationFormat;
    this.serializationClass = serializationClass;
    this.serializationLib = serializationLib;
    this.fieldDelim = fieldDelim;
    this.collectionItemDelim = collectionItemDelim;
    this.mapKeyDelim = mapKeyDelim;
    this.lineDelim = lineDelim;
    this.parameters = parameters;
  }

  /**
   * @return the serializationFormat
   */
  public String getSerializationFormat() {
    return serializationFormat;
  }

  /**
   * @param serializationFormat the serializationFormat to set
   */
  public void setSerializationFormat(String serializationFormat) {
    this.serializationFormat = serializationFormat;
  }

  /**
   * @return the serializationClass
   */
  public String getSerializationClass() {
    return serializationClass;
  }

  /**
   * @param serializationClass the serializationClass to set
   */
  public void setSerializationClass(String serializationClass) {
    this.serializationClass = serializationClass;
  }

  /**
   * @return the serializationLib
   */
  public String getSerializationLib() {
    return serializationLib;
  }

  /**
   * @param serializationLib the serializationLib to set
   */
  public void setSerializationLib(String serializationLib) {
    this.serializationLib = serializationLib;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the fieldDelim
   */
  public String getFieldDelim() {
    return fieldDelim;
  }

  /**
   * @param fieldDelim the fieldDelim to set
   */
  public void setFieldDelim(String fieldDelim) {
    this.fieldDelim = fieldDelim;
  }

  /**
   * @return the collectionItemDelim
   */
  public String getCollectionItemDelim() {
    return collectionItemDelim;
  }

  /**
   * @param collectionItemDelim the collectionItemDelim to set
   */
  public void setCollectionItemDelim(String collectionItemDelim) {
    this.collectionItemDelim = collectionItemDelim;
  }

  /**
   * @return the mapKeyDelim
   */
  public String getMapKeyDelim() {
    return mapKeyDelim;
  }

  /**
   * @param mapKeyDelim the mapKeyDelim to set
   */
  public void setMapKeyDelim(String mapKeyDelim) {
    this.mapKeyDelim = mapKeyDelim;
  }

  /**
   * @return the lineDelim
   */
  public String getLineDelim() {
    return lineDelim;
  }

  /**
   * @param lineDelim the lineDelim to set
   */
  public void setLineDelim(String lineDelim) {
    this.lineDelim = lineDelim;
  }

  /**
   * @return the parameters
   */
  public Map<String, String> getParameters() {
    return parameters;
  }

  /**
   * @param parameters the parameters to set
   */
  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

}

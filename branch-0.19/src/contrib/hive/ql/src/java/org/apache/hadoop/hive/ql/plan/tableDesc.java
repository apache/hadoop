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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;

public class tableDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private Class<? extends Deserializer> deserializerClass;
  private Class<? extends InputFormat> inputFileFormatClass;
  private Class<? extends OutputFormat> outputFileFormatClass;
  private java.util.Properties properties;
  private String serdeClassName;
  public tableDesc() { }
  public tableDesc(
      final Class<? extends Deserializer> serdeClass,
      final Class<? extends InputFormat> inputFileFormatClass,
      final Class<? extends OutputFormat> class1,
      final java.util.Properties properties) {
    this.deserializerClass = serdeClass;
    this.inputFileFormatClass = inputFileFormatClass;
    this.outputFileFormatClass = class1;
    this.properties = properties;
    this.serdeClassName = properties.getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB);;
  }
  public Class<? extends Deserializer> getDeserializerClass() {
    return this.deserializerClass;
  }
  public void setDeserializerClass(final Class<? extends SerDe> serdeClass) {
    this.deserializerClass = serdeClass;
  }
  public Class<? extends InputFormat> getInputFileFormatClass() {
    return this.inputFileFormatClass;
  }
  public void setInputFileFormatClass(final Class<? extends InputFormat> inputFileFormatClass) {
    this.inputFileFormatClass=inputFileFormatClass;
  }
  public Class<? extends OutputFormat> getOutputFileFormatClass() {
    return this.outputFileFormatClass;
  }
  public void setOutputFileFormatClass(final Class<? extends OutputFormat> outputFileFormatClass) {
    this.outputFileFormatClass=outputFileFormatClass;
  }
  
  @explain(displayName="properties", normalExplain=false)
  public java.util.Properties getProperties() {
    return this.properties;
  }
  public void setProperties(final java.util.Properties properties) {
    this.properties = properties;
  }
  /**
   * @return the serdeClassName
   */
  @explain(displayName="serde")
  public String getSerdeClassName() {
    return this.serdeClassName;
  }
  /**
   * @param serdeClassName the serde Class Name to set
   */
  public void setSerdeClassName(String serdeClassName) {
    this.serdeClassName = serdeClassName;
  }
  
  @explain(displayName="name")
  public String getTableName() {
    return this.properties.getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME);
  }
  
  @explain(displayName="input format")
  public String getInputFileFormatClassName() {
    return getInputFileFormatClass().getName();
  }
  
  @explain(displayName="output format")
  public String getOutputFileFormatClassName() {
    return getOutputFileFormatClass().getName();
  }
}

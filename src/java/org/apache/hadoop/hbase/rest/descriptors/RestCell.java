/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.rest.descriptors;

import org.apache.hadoop.hbase.io.Cell;

import agilejson.TOJSON;

/**
 * 
 */
public class RestCell extends Cell {

  byte[] name;
  
  

  /**
   * 
   */
  public RestCell() {
    super();
    // TODO Auto-generated constructor stub
  }
  
  /**
   * 
   */
  public RestCell(byte[] name, Cell cell) {
    super(cell.getValue(), cell.getTimestamp());
    this.name = name;
  }

  /**
   * @param value
   * @param timestamp
   */
  public RestCell(byte[] value, long timestamp) {
    super(value, timestamp);
    // TODO Auto-generated constructor stub
  }

  /**
   * @param vals
   * @param ts
   */
  public RestCell(byte[][] vals, long[] ts) {
    super(vals, ts);
    // TODO Auto-generated constructor stub
  }

  /**
   * @param value
   * @param timestamp
   */
  public RestCell(String value, long timestamp) {
    super(value, timestamp);
    // TODO Auto-generated constructor stub
  }

  /**
   * @param vals
   * @param ts
   */
  public RestCell(String[] vals, long[] ts) {
    super(vals, ts);
    // TODO Auto-generated constructor stub
  }

  /**
   * @return the name
   */
  @TOJSON(base64=true)
  public byte[] getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(byte[] name) {
    this.name = name;
  }
  
  
}

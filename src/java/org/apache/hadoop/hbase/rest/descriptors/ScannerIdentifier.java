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

import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.serializer.IRestSerializer;
import org.apache.hadoop.hbase.rest.serializer.ISerializable;

import agilejson.TOJSON;

/**
 * 
 */
public class ScannerIdentifier implements ISerializable {
  Integer id;
  Long numRows;

  /**
   * @param id
   */
  public ScannerIdentifier(Integer id) {
    super();
    this.id = id;
  }

  /**
   * @param id
   * @param numRows
   */
  public ScannerIdentifier(Integer id, Long numRows) {
    super();
    this.id = id;
    this.numRows = numRows;
  }

  /**
   * @return the id
   */
  @TOJSON
  public Integer getId() {
    return id;
  }

  /**
   * @param id
   *          the id to set
   */
  public void setId(Integer id) {
    this.id = id;
  }

  /**
   * @return the numRows
   */
  public Long getNumRows() {
    return numRows;
  }

  /**
   * @param numRows
   *          the numRows to set
   */
  public void setNumRows(Long numRows) {
    this.numRows = numRows;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.xml.IOutputXML#toXML(org.apache.hadoop.hbase
   * .rest.serializer.IRestSerializer)
   */
  public void restSerialize(IRestSerializer serializer)
      throws HBaseRestException {
    serializer.serializeScannerIdentifier(this);
  }

}

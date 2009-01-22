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
package org.apache.hadoop.hbase.rest.serializer;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.rest.DatabaseModel.DatabaseMetadata;
import org.apache.hadoop.hbase.rest.Status.StatusMessage;
import org.apache.hadoop.hbase.rest.TableModel.Regions;
import org.apache.hadoop.hbase.rest.descriptors.ScannerIdentifier;
import org.apache.hadoop.hbase.rest.descriptors.TimestampsDescriptor;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;

import agilejson.JSON;

/**
 * 
 * Serializes objects into JSON strings and prints them back out on the output
 * stream. It should be noted that this JSON implementation uses annotations on
 * the objects to be serialized.
 * 
 * Since these annotations are used to describe the serialization of the objects
 * the only method that is implemented is writeOutput(Object o). The other
 * methods in the interface do not need to be implemented.
 */
public class JSONSerializer extends AbstractRestSerializer {

  /**
   * @param response
   */
  public JSONSerializer(HttpServletResponse response) {
    super(response, false);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#writeOutput(java
   * .lang.Object, javax.servlet.http.HttpServletResponse)
   */
  public void writeOutput(Object o) throws HBaseRestException {
    response.setContentType("application/json");

    try {
      // LOG.debug("At top of send data");
      String data = JSON.toJSON(o);
      response.setContentLength(data.length());
      response.getWriter().println(data);
    } catch (Exception e) {
      // LOG.debug("Error sending data: " + e.toString());
      throw new HBaseRestException(e);
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @seeorg.apache.hadoop.hbase.rest.serializer.IRestSerializer#
   * serializeColumnDescriptor(org.apache.hadoop.hbase.HColumnDescriptor)
   */
  public void serializeColumnDescriptor(HColumnDescriptor column)
      throws HBaseRestException {
    // No implementation needed for the JSON serializer

  }

  /*
   * (non-Javadoc)
   * 
   * @seeorg.apache.hadoop.hbase.rest.serializer.IRestSerializer#
   * serializeDatabaseMetadata
   * (org.apache.hadoop.hbase.rest.DatabaseModel.DatabaseMetadata)
   */
  public void serializeDatabaseMetadata(DatabaseMetadata databaseMetadata)
      throws HBaseRestException {
    // No implementation needed for the JSON serializer

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeRegionData
   * (org.apache.hadoop.hbase.rest.TableModel.Regions)
   */
  public void serializeRegionData(Regions regions) throws HBaseRestException {
    // No implementation needed for the JSON serializer

  }

  /*
   * (non-Javadoc)
   * 
   * @seeorg.apache.hadoop.hbase.rest.serializer.IRestSerializer#
   * serializeTableDescriptor(org.apache.hadoop.hbase.HTableDescriptor)
   */
  public void serializeTableDescriptor(HTableDescriptor tableDescriptor)
      throws HBaseRestException {
    // No implementation needed for the JSON serializer

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeStatusMessage
   * (org.apache.hadoop.hbase.rest.Status.StatusMessage)
   */
  public void serializeStatusMessage(StatusMessage message)
      throws HBaseRestException {
    // No implementation needed for the JSON serializer

  }

  /*
   * (non-Javadoc)
   * 
   * @seeorg.apache.hadoop.hbase.rest.serializer.IRestSerializer#
   * serializeScannerIdentifier(org.apache.hadoop.hbase.rest.ScannerIdentifier)
   */
  public void serializeScannerIdentifier(ScannerIdentifier scannerIdentifier)
      throws HBaseRestException {
    // No implementation needed for the JSON serializer

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeRowResult
   * (org.apache.hadoop.hbase.io.RowResult)
   */
  public void serializeRowResult(RowResult rowResult) throws HBaseRestException {
    // No implementation needed for the JSON serializer

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeRowResultArray
   * (org.apache.hadoop.hbase.io.RowResult[])
   */
  public void serializeRowResultArray(RowResult[] rows)
      throws HBaseRestException {
    // No implementation needed for the JSON serializer

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeCell(org
   * .apache.hadoop.hbase.io.Cell)
   */
  public void serializeCell(Cell cell) throws HBaseRestException {
    // No implementation needed for the JSON serializer

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeCellArray
   * (org.apache.hadoop.hbase.io.Cell[])
   */
  public void serializeCellArray(Cell[] cells) throws HBaseRestException {
    // No implementation needed for the JSON serializer

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeTimestamps
   * (org.apache.hadoop.hbase.rest.RowModel.TimestampsDescriptor)
   */
  public void serializeTimestamps(TimestampsDescriptor timestampsDescriptor)
      throws HBaseRestException {
    // No implementation needed for the JSON serializer
  }

}

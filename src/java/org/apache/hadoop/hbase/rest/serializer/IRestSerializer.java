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

/**
 * 
 *         Interface that is implemented to return serialized objects back to
 *         the output stream.
 */
public interface IRestSerializer {
  /**
   * Serializes an object into the appropriate format and writes it to the
   * output stream.
   * 
   * This is the main point of entry when for an object to be serialized to the
   * output stream.
   * 
   * @param o
   * @throws HBaseRestException
   */
  public void writeOutput(Object o) throws HBaseRestException;

  /**
   * serialize the database metadata
   * 
   * Implementation of this method is optional, IF all the work is done in the
   * writeOutput(Object o) method
   * 
   * @param databaseMetadata
   * @throws HBaseRestException
   */
  public void serializeDatabaseMetadata(DatabaseMetadata databaseMetadata)
      throws HBaseRestException;

  /**
   * serialize the HTableDescriptor object
   * 
   * Implementation of this method is optional, IF all the work is done in the
   * writeOutput(Object o) method
   * 
   * @param tableDescriptor
   * @throws HBaseRestException
   */
  public void serializeTableDescriptor(HTableDescriptor tableDescriptor)
      throws HBaseRestException;

  /**
   * serialize an HColumnDescriptor to the output stream.
   * 
   * Implementation of this method is optional, IF all the work is done in the
   * writeOutput(Object o) method
   * 
   * @param column
   * @throws HBaseRestException
   */
  public void serializeColumnDescriptor(HColumnDescriptor column)
      throws HBaseRestException;

  /**
   * serialize the region data for a table to the output stream
   * 
   * Implementation of this method is optional, IF all the work is done in the
   * writeOutput(Object o) method
   * 
   * @param regions
   * @throws HBaseRestException
   */
  public void serializeRegionData(Regions regions) throws HBaseRestException;

  /**
   * serialize the status message object to the output stream
   * 
   * Implementation of this method is optional, IF all the work is done in the
   * writeOutput(Object o) method
   * 
   * @param message
   * @throws HBaseRestException
   */
  public void serializeStatusMessage(StatusMessage message)
      throws HBaseRestException;

  /**
   * serialize the ScannerIdentifier object to the output stream
   * 
   * Implementation of this method is optional, IF all the work is done in the
   * writeOutput(Object o) method
   * 
   * @param scannerIdentifier
   * @throws HBaseRestException
   */
  public void serializeScannerIdentifier(ScannerIdentifier scannerIdentifier)
      throws HBaseRestException;

  /**
   * serialize a RowResult object to the output stream
   * 
   * Implementation of this method is optional, IF all the work is done in the
   * writeOutput(Object o) method
   * 
   * @param rowResult
   * @throws HBaseRestException
   */
  public void serializeRowResult(RowResult rowResult) throws HBaseRestException;

  /**
   * serialize a RowResult array to the output stream
   * 
   * Implementation of this method is optional, IF all the work is done in the
   * writeOutput(Object o) method
   * 
   * @param rows
   * @throws HBaseRestException
   */
  public void serializeRowResultArray(RowResult[] rows)
      throws HBaseRestException;

  /**
   * serialize a cell object to the output stream
   * 
   * Implementation of this method is optional, IF all the work is done in the
   * writeOutput(Object o) method
   * 
   * @param cell
   * @throws HBaseRestException
   */
  public void serializeCell(Cell cell) throws HBaseRestException;
  
  /**
   * serialize a Cell array to the output stream
   * 
   * @param cells
   * @throws HBaseRestException
   */
  public void serializeCellArray(Cell[] cells) throws HBaseRestException;
  
  
  /**
   * serialize a description of the timestamps available for a row 
   * to the output stream.
   * 
   * @param timestampsDescriptor
   * @throws HBaseRestException
   */
  public void serializeTimestamps(TimestampsDescriptor timestampsDescriptor) throws HBaseRestException;
}

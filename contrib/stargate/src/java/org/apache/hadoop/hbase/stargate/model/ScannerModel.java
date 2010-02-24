/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.stargate.model;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.stargate.ProtobufMessageHandler;
import org.apache.hadoop.hbase.stargate.protobuf.generated.ScannerMessage.Scanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;

/**
 * A representation of Scanner parameters.
 * 
 * <pre>
 * &lt;complexType name="Scanner"&gt;
 *   &lt;sequence>
 *     &lt;element name="column" type="base64Binary" minOccurs="0" maxOccurs="unbounded"/&gt;
 *   &lt;/sequence&gt;
 *   &lt;attribute name="startRow" type="base64Binary"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="endRow" type="base64Binary"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="batch" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="startTime" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="endTime" type="int"&gt;&lt;/attribute&gt;
 * &lt;/complexType&gt;
 * </pre>
 */
@XmlRootElement(name="Scanner")
public class ScannerModel implements ProtobufMessageHandler, Serializable {
  private static final long serialVersionUID = 1L;

  private byte[] startRow = HConstants.EMPTY_START_ROW;
  private byte[] endRow = HConstants.EMPTY_END_ROW;;
  private List<byte[]> columns = new ArrayList<byte[]>();
  private int batch = 1;
  private long startTime = 0;
  private long endTime = Long.MAX_VALUE;

  /**
   * Default constructor
   */
  public ScannerModel() {}

  /**
   * Constructor
   * @param startRow the start key of the row-range
   * @param endRow the end key of the row-range
   * @param columns the columns to scan
   * @param batch the number of values to return in batch
   * @param endTime the upper bound on timestamps of values of interest
   * (values with timestamps later than this are excluded)
   */
  public ScannerModel(byte[] startRow, byte[] endRow, List<byte[]> columns,
      int batch, long endTime) {
    super();
    this.startRow = startRow;
    this.endRow = endRow;
    this.columns = columns;
    this.batch = batch;
    this.endTime = endTime;
  }

  /**
   * Constructor 
   * @param startRow the start key of the row-range
   * @param endRow the end key of the row-range
   * @param columns the columns to scan
   * @param batch the number of values to return in batch
   * @param startTime the lower bound on timestamps of values of interest
   * (values with timestamps earlier than this are excluded)
   * @param endTime the upper bound on timestamps of values of interest
   * (values with timestamps later than this are excluded)
   */
  public ScannerModel(byte[] startRow, byte[] endRow, List<byte[]> columns,
      int batch, long startTime, long endTime) {
    super();
    this.startRow = startRow;
    this.endRow = endRow;
    this.columns = columns;
    this.batch = batch;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  /**
   * Add a column to the column set
   * @param column the column name, as &lt;column&gt;(:&lt;qualifier&gt;)?
   */
  public void addColumn(byte[] column) {
    columns.add(column);
  }

  /**
   * @return true if a start row was specified
   */
  public boolean hasStartRow() {
    return !Bytes.equals(startRow, HConstants.EMPTY_START_ROW);
  }

  /**
   * @return start row
   */
  @XmlAttribute
  public byte[] getStartRow() {
    return startRow;
  }

  /**
   * @return true if an end row was specified
   */
  public boolean hasEndRow() {
    return !Bytes.equals(endRow, HConstants.EMPTY_END_ROW);
  }

  /**
   * @return end row
   */
  @XmlAttribute
  public byte[] getEndRow() {
    return endRow;
  }

  /**
   * @return list of columns of interest in column:qualifier format, or empty for all
   */
  @XmlElement(name="column")
  public List<byte[]> getColumns() {
    return columns;
  }
  
  /**
   * @return the number of cells to return in batch
   */
  @XmlAttribute
  public int getBatch() {
    return batch;
  }

  /**
   * @return the lower bound on timestamps of items of interest
   */
  @XmlAttribute
  public long getStartTime() {
    return startTime;
  }

  /**
   * @return the upper bound on timestamps of items of interest
   */
  @XmlAttribute
  public long getEndTime() {
    return endTime;
  }

  /**
   * @param startRow start row
   */
  public void setStartRow(byte[] startRow) {
    this.startRow = startRow;
  }

  /**
   * @param endRow end row
   */
  public void setEndRow(byte[] endRow) {
    this.endRow = endRow;
  }

  /**
   * @param columns list of columns of interest in column:qualifier format, or empty for all
   */
  public void setColumns(List<byte[]> columns) {
    this.columns = columns;
  }

  /**
   * @param batch the number of cells to return in batch
   */
  public void setBatch(int batch) {
    this.batch = batch;
  }

  /**
   * @param startTime the lower bound on timestamps of values of interest
   */
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  /**
   * @param endTime the upper bound on timestamps of values of interest
   */
  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  @Override
  public byte[] createProtobufOutput() {
    Scanner.Builder builder = Scanner.newBuilder();
    if (!Bytes.equals(startRow, HConstants.EMPTY_START_ROW)) {
      builder.setStartRow(ByteString.copyFrom(startRow));
    }
    if (!Bytes.equals(endRow, HConstants.EMPTY_START_ROW)) {
      builder.setEndRow(ByteString.copyFrom(endRow));
    }
    for (byte[] column: columns) {
      builder.addColumns(ByteString.copyFrom(column));
    }
    builder.setBatch(batch);
    if (startTime != 0) {
      builder.setStartTime(startTime);
    }
    if (endTime != 0) {
      builder.setEndTime(endTime);
    }
    builder.setBatch(getBatch());
    return builder.build().toByteArray();
  }

  @Override
  public ProtobufMessageHandler getObjectFromMessage(byte[] message)
      throws IOException {
    Scanner.Builder builder = Scanner.newBuilder();
    builder.mergeFrom(message);
    if (builder.hasStartRow()) {
      startRow = builder.getStartRow().toByteArray();
    }
    if (builder.hasEndRow()) {
      endRow = builder.getEndRow().toByteArray();
    }
    for (ByteString column: builder.getColumnsList()) {
      addColumn(column.toByteArray());
    }
    if (builder.hasBatch()) {
      batch = builder.getBatch();
    }
    if (builder.hasStartTime()) {
      startTime = builder.getStartTime();
    }
    if (builder.hasEndTime()) {
      endTime = builder.getEndTime();
    }
    return this;
  }
}

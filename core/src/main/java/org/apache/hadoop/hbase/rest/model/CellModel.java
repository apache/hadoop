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

package org.apache.hadoop.hbase.rest.model;

import java.io.IOException;
import java.io.Serializable;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.rest.ProtobufMessageHandler;
import org.apache.hadoop.hbase.rest.protobuf.generated.CellMessage.Cell;

import com.google.protobuf.ByteString;

/**
 * Representation of a cell. A cell is a single value associated a column and
 * optional qualifier, and either the timestamp when it was stored or the user-
 * provided timestamp if one was explicitly supplied.
 *
 * <pre>
 * &lt;complexType name="Cell"&gt;
 *   &lt;sequence&gt;
 *     &lt;element name="value" maxOccurs="1" minOccurs="1"&gt;
 *       &lt;simpleType&gt;
 *         &lt;restriction base="base64Binary"/&gt;
 *       &lt;/simpleType&gt;
 *     &lt;/element&gt;
 *   &lt;/sequence&gt;
 *   &lt;attribute name="column" type="base64Binary" /&gt;
 *   &lt;attribute name="timestamp" type="int" /&gt;
 * &lt;/complexType&gt;
 * </pre>
 */
@XmlRootElement(name="Cell")
public class CellModel implements ProtobufMessageHandler, Serializable {
  private static final long serialVersionUID = 1L;
  
  private long timestamp = HConstants.LATEST_TIMESTAMP;
  private byte[] column;
  private byte[] value;

  /**
   * Default constructor
   */
  public CellModel() {}

  /**
   * Constructor
   * @param column
   * @param value
   */
  public CellModel(byte[] column, byte[] value) {
    this(column, HConstants.LATEST_TIMESTAMP, value);
  }

  /**
   * Constructor
   * @param column
   * @param qualifier
   * @param value
   */
  public CellModel(byte[] column, byte[] qualifier, byte[] value) {
    this(column, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /**
   * Constructor from KeyValue
   * @param kv
   */
  public CellModel(KeyValue kv) {
    this(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(), kv.getValue());
  }

  /**
   * Constructor
   * @param column
   * @param timestamp
   * @param value
   */
  public CellModel(byte[] column, long timestamp, byte[] value) {
    this.column = column;
    this.timestamp = timestamp;
    this.value = value;
  }

  /**
   * Constructor
   * @param column
   * @param qualifier
   * @param timestamp
   * @param value
   */
  public CellModel(byte[] column, byte[] qualifier, long timestamp,
      byte[] value) {
    this.column = KeyValue.makeColumn(column, qualifier);
    this.timestamp = timestamp;
    this.value = value;
  }
  
  /**
   * @return the column
   */
  @XmlAttribute
  public byte[] getColumn() {
    return column;
  }

  /**
   * @param column the column to set
   */
  public void setColumn(byte[] column) {
    this.column = column;
  }

  /**
   * @return true if the timestamp property has been specified by the
   * user
   */
  public boolean hasUserTimestamp() {
    return timestamp != HConstants.LATEST_TIMESTAMP;
  }

  /**
   * @return the timestamp
   */
  @XmlAttribute
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @param timestamp the timestamp to set
   */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * @return the value
   */
  @XmlValue
  public byte[] getValue() {
    return value;
  }

  /**
   * @param value the value to set
   */
  public void setValue(byte[] value) {
    this.value = value;
  }

  @Override
  public byte[] createProtobufOutput() {
    Cell.Builder builder = Cell.newBuilder();
    builder.setColumn(ByteString.copyFrom(getColumn()));
    builder.setData(ByteString.copyFrom(getValue()));
    if (hasUserTimestamp()) {
      builder.setTimestamp(getTimestamp());
    }
    return builder.build().toByteArray();
  }

  @Override
  public ProtobufMessageHandler getObjectFromMessage(byte[] message)
      throws IOException {
    Cell.Builder builder = Cell.newBuilder();
    builder.mergeFrom(message);
    setColumn(builder.getColumn().toByteArray());
    setValue(builder.getData().toByteArray());
    if (builder.hasTimestamp()) {
      setTimestamp(builder.getTimestamp());
    }
    return this;
  }
}

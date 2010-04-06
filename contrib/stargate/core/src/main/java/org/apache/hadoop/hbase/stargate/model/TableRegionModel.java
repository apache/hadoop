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

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Representation of a region of a table and its current location on the
 * storage cluster.
 * 
 * <pre>
 * &lt;complexType name="TableRegion"&gt;
 *   &lt;attribute name="name" type="string"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="id" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="startKey" type="base64Binary"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="endKey" type="base64Binary"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="location" type="string"&gt;&lt;/attribute&gt;
 *  &lt;/complexType&gt;
 * </pre>
 */
@XmlRootElement(name="Region")
@XmlType(propOrder = {"name","id","startKey","endKey","location"})
public class TableRegionModel implements Serializable {

  private static final long serialVersionUID = 1L;

  private String table;
  private long id;
  private byte[] startKey; 
  private byte[] endKey;
  private String location;

  /**
   * Constructor
   */
  public TableRegionModel() {}

  /**
   * Constructor
   * @param table the table name
   * @param id the encoded id of the region
   * @param startKey the start key of the region
   * @param endKey the end key of the region
   */
  public TableRegionModel(String table, long id, byte[] startKey,
      byte[] endKey) {
    this(table, id, startKey, endKey, null);
  }

  /**
   * Constructor
   * @param table the table name
   * @param id the encoded id of the region
   * @param startKey the start key of the region
   * @param endKey the end key of the region
   * @param location the name and port of the region server hosting the region
   */
  public TableRegionModel(String table, long id, byte[] startKey,
      byte[] endKey, String location) {
    this.table = table;
    this.id = id;
    this.startKey = startKey;
    this.endKey = endKey;
    this.location = location;
  }

  /**
   * @return the region name
   */
  @XmlAttribute
  public String getName() {
    StringBuilder sb = new StringBuilder();
    sb.append(table);
    sb.append(',');
    sb.append(Bytes.toString(startKey));
    sb.append(',');
    sb.append(id);
    return sb.toString();
  }

  /**
   * @return the encoded region id
   */
  @XmlAttribute 
  public long getId() {
    return id;
  }

  /**
   * @return the start key
   */
  @XmlAttribute 
  public byte[] getStartKey() {
    return startKey;
  }

  /**
   * @return the end key
   */
  @XmlAttribute 
  public byte[] getEndKey() {
    return endKey;
  }

  /**
   * @return the name and port of the region server hosting the region
   */
  @XmlAttribute 
  public String getLocation() {
    return location;
  }

  /**
   * @param name region printable name
   */
  public void setName(String name) {
    String split[] = name.split(",");
    table = split[0];
    startKey = Bytes.toBytes(split[1]);
    id = Long.valueOf(split[2]);
  }

  /**
   * @param id the region's encoded id
   */
  public void setId(long id) {
    this.id = id;
  }

  /**
   * @param startKey the start key
   */
  public void setStartKey(byte[] startKey) {
    this.startKey = startKey;
  }

  /**
   * @param endKey the end key
   */
  public void setEndKey(byte[] endKey) {
    this.endKey = endKey;
  }

  /**
   * @param location the name and port of the region server hosting the region
   */
  public void setLocation(String location) {
    this.location = location;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getName());
    sb.append(" [\n  id=");
    sb.append(id);
    sb.append("\n  startKey='");
    sb.append(Bytes.toString(startKey));
    sb.append("'\n  endKey='");
    sb.append(Bytes.toString(endKey));
    if (location != null) {
      sb.append("'\n  location='");
      sb.append(location);
    }
    sb.append("'\n]\n");
    return sb.toString();
  }
}

/*
 * Copyright 2009 The Apache Software Foundation
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
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAnyAttribute;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.namespace.QName;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;

@XmlRootElement(name="ColumnSchema")
@XmlType(propOrder = {"name"})
public class ColumnSchemaModel implements Serializable {
  private static final long serialVersionUID = 1L;
  private static QName BLOCKCACHE = new QName(HColumnDescriptor.BLOCKCACHE);
  private static QName BLOCKSIZE = new QName(HColumnDescriptor.BLOCKSIZE);
  private static QName BLOOMFILTER = new QName(HColumnDescriptor.BLOOMFILTER);
  private static QName COMPRESSION = new QName(HColumnDescriptor.COMPRESSION);
  private static QName IN_MEMORY = new QName(HConstants.IN_MEMORY);
  private static QName TTL = new QName(HColumnDescriptor.TTL);
  private static QName VERSIONS = new QName(HConstants.VERSIONS);

  private String name;
  private Map<QName,Object> attrs = new HashMap<QName,Object>();

  public ColumnSchemaModel() {}

  public void addAttribute(String name, Object value) {
    attrs.put(new QName(name), value);
  }

  public String getAttribute(String name) {
    return attrs.get(new QName(name)).toString();
  }

  /**
   * @return the column name
   */
  @XmlAttribute
  public String getName() {
    return name;
  }

  /**
   * @return the map for holding unspecified (user) attributes
   */
  @XmlAnyAttribute
  public Map<QName,Object> getAny() {
    return attrs;
  }

  /**
   * @param the table name
   */
  public void setName(String name) {
    this.name = name;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ NAME => '");
    sb.append(name);
    sb.append('\'');
    for (Map.Entry<QName,Object> e: attrs.entrySet()) {
      sb.append(", ");
      sb.append(e.getKey().getLocalPart());
      sb.append(" => '");
      sb.append(e.getValue().toString());
      sb.append('\'');
    }
    sb.append(" }");
    return sb.toString();
  }

  // getters and setters for common schema attributes

  // cannot be standard bean type getters and setters, otherwise this would
  // confuse JAXB

  public boolean __getBlockcache() {
    Object o = attrs.get(BLOCKCACHE);
    return o != null ? 
      Boolean.valueOf(o.toString()) : HColumnDescriptor.DEFAULT_BLOCKCACHE;
  }

  public int __getBlocksize() {
    Object o = attrs.get(BLOCKSIZE);
    return o != null ? 
      Integer.valueOf(o.toString()) : HColumnDescriptor.DEFAULT_BLOCKSIZE;
  }

  public boolean __getBloomfilter() {
    Object o = attrs.get(BLOOMFILTER);
    return o != null ? 
      Boolean.valueOf(o.toString()) : HColumnDescriptor.DEFAULT_BLOOMFILTER;
  }

  public String __getCompression() {
    Object o = attrs.get(COMPRESSION);
    return o != null ? o.toString() : HColumnDescriptor.DEFAULT_COMPRESSION;
  }

  public boolean __getInMemory() {
    Object o = attrs.get(IN_MEMORY);
    return o != null ? 
      Boolean.valueOf(o.toString()) : HColumnDescriptor.DEFAULT_IN_MEMORY;
  }

  public int __getTTL() {
    Object o = attrs.get(TTL);
    return o != null ? 
      Integer.valueOf(o.toString()) : HColumnDescriptor.DEFAULT_TTL;
  }

  public int __getVersions() {
    Object o = attrs.get(VERSIONS);
    return o != null ? 
      Integer.valueOf(o.toString()) : HColumnDescriptor.DEFAULT_VERSIONS;
  }

  public void __setBlocksize(int value) {
    attrs.put(BLOCKSIZE, Integer.toString(value));
  }

  public void __setBlockcache(boolean value) {
    attrs.put(BLOCKCACHE, Boolean.toString(value));
  }

  public void __setBloomfilter(boolean value) {
    attrs.put(BLOOMFILTER, Boolean.toString(value));
  }

  public void __setCompression(String value) {
    attrs.put(COMPRESSION, value); 
  }

  public void __setInMemory(boolean value) {
    attrs.put(IN_MEMORY, Boolean.toString(value));
  }

  public void __setTTL(int value) {
    attrs.put(TTL, Integer.toString(value));
  }

  public void __setVersions(int value) {
    attrs.put(VERSIONS, Integer.toString(value));
  }
}

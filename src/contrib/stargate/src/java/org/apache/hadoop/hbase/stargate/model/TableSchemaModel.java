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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAnyAttribute;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.namespace.QName;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.stargate.protobuf.generated.ColumnSchemaMessage.ColumnSchema;
import org.apache.hadoop.hbase.stargate.protobuf.generated.TableSchemaMessage.TableSchema;

@XmlRootElement(name="TableSchema")
@XmlType(propOrder = {"name","columns"})
public class TableSchemaModel implements Serializable, IProtobufWrapper {
  private static final long serialVersionUID = 1L;
  private static final QName IN_MEMORY = new QName(HConstants.IN_MEMORY);
  private static final QName IS_META = new QName(HTableDescriptor.IS_META);
  private static final QName IS_ROOT = new QName(HTableDescriptor.IS_ROOT);
  private static final QName READONLY = new QName(HTableDescriptor.READONLY);
  private static final QName TTL = new QName(HColumnDescriptor.TTL);
  private static final QName VERSIONS = new QName(HConstants.VERSIONS);
  private static final QName COMPRESSION = 
    new QName(HColumnDescriptor.COMPRESSION);

  private String name;
  private Map<QName,Object> attrs = new HashMap<QName,Object>();
  private List<ColumnSchemaModel> columns = new ArrayList<ColumnSchemaModel>();
  
  public TableSchemaModel() {}
  
  public void addAttribute(String name, Object value) {
    attrs.put(new QName(name), value);
  }

  public String getAttribute(String name) {
    return attrs.get(new QName(name)).toString();
  }

  public void addColumnFamily(ColumnSchemaModel object) {
    columns.add(object);
  }
  
  public ColumnSchemaModel getColumnFamily(int index) {
    return columns.get(index);
  }

  /**
   * @return the table name
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
   * @return the columns
   */
  @XmlElement(name="ColumnSchema")
  public List<ColumnSchemaModel> getColumns() {
    return columns;
  }

  /**
   * @param name the table name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @param columns the columns to set
   */
  public void setColumns(List<ColumnSchemaModel> columns) {
    this.columns = columns;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ NAME=> '");
    sb.append(name);
    sb.append('\'');
    for (Map.Entry<QName,Object> e: attrs.entrySet()) {
      sb.append(", ");
      sb.append(e.getKey().getLocalPart());
      sb.append(" => '");
      sb.append(e.getValue().toString());
      sb.append('\'');
    }
    sb.append(", COLUMNS => [ ");
    Iterator<ColumnSchemaModel> i = columns.iterator();
    while (i.hasNext()) {
      ColumnSchemaModel family = i.next();
      sb.append(family.toString());
      if (i.hasNext()) {
        sb.append(',');
      }
      sb.append(' ');
    }
    sb.append("] }");
    return sb.toString();
  }

  // getters and setters for common schema attributes

  // cannot be standard bean type getters and setters, otherwise this would
  // confuse JAXB

  public boolean __getInMemory() {
    Object o = attrs.get(IN_MEMORY);
    return o != null ? 
      Boolean.valueOf(o.toString()) : HTableDescriptor.DEFAULT_IN_MEMORY;
  }

  public boolean __getIsMeta() {
    Object o = attrs.get(IS_META);
    return o != null ? Boolean.valueOf(o.toString()) : false;
  }

  public boolean __getIsRoot() {
    Object o = attrs.get(IS_ROOT);
    return o != null ? Boolean.valueOf(o.toString()) : false;
  }

  public boolean __getReadOnly() {
    Object o = attrs.get(READONLY);
    return o != null ? 
      Boolean.valueOf(o.toString()) : HTableDescriptor.DEFAULT_READONLY;
  }

  public void __setInMemory(boolean value) {
    attrs.put(IN_MEMORY, Boolean.toString(value));
  }

  public void __setIsMeta(boolean value) {
    attrs.put(IS_META, Boolean.toString(value));
  }

  public void __setIsRoot(boolean value) {
    attrs.put(IS_ROOT, Boolean.toString(value));
  }

  public void __setReadOnly(boolean value) {
    attrs.put(READONLY, Boolean.toString(value));
  }

  @Override
  public byte[] createProtobufOutput() {
    TableSchema.Builder builder = TableSchema.newBuilder();
    builder.setName(name);
    for (Map.Entry<QName, Object> e: attrs.entrySet()) {
      TableSchema.Attribute.Builder attrBuilder = 
        TableSchema.Attribute.newBuilder();
      attrBuilder.setName(e.getKey().getLocalPart());
      attrBuilder.setValue(e.getValue().toString());
      builder.addAttrs(attrBuilder);
    }
    for (ColumnSchemaModel family: columns) {
      Map<QName, Object> familyAttrs = family.getAny();
      ColumnSchema.Builder familyBuilder = ColumnSchema.newBuilder();
      familyBuilder.setName(family.getName());
      for (Map.Entry<QName, Object> e: familyAttrs.entrySet()) {
        ColumnSchema.Attribute.Builder attrBuilder = 
          ColumnSchema.Attribute.newBuilder();
        attrBuilder.setName(e.getKey().getLocalPart());
        attrBuilder.setValue(e.getValue().toString());
        familyBuilder.addAttrs(attrBuilder);
      }
      if (familyAttrs.containsKey(TTL)) {
        familyBuilder.setTtl(
          Integer.valueOf(familyAttrs.get(TTL).toString()));
      }
      if (familyAttrs.containsKey(VERSIONS)) {
        familyBuilder.setMaxVersions(
          Integer.valueOf(familyAttrs.get(VERSIONS).toString()));
      }
      if (familyAttrs.containsKey(COMPRESSION)) {
        familyBuilder.setCompression(familyAttrs.get(COMPRESSION).toString());
      }
      builder.addColumns(familyBuilder);
    }
    if (attrs.containsKey(IN_MEMORY)) {
      builder.setInMemory(
        Boolean.valueOf(attrs.get(IN_MEMORY).toString()));
    }
    if (attrs.containsKey(READONLY)) {
      builder.setReadOnly(
        Boolean.valueOf(attrs.get(READONLY).toString()));
    }
    return builder.build().toByteArray();
  }

  @Override
  public IProtobufWrapper getObjectFromMessage(byte[] message) 
      throws IOException {
    TableSchema.Builder builder = TableSchema.newBuilder();
    builder.mergeFrom(message);
    this.setName(builder.getName());
    for (TableSchema.Attribute attr: builder.getAttrsList()) {
      this.addAttribute(attr.getName(), attr.getValue());
    }
    if (builder.hasInMemory()) {
      this.addAttribute(HConstants.IN_MEMORY, builder.getInMemory());
    }
    if (builder.hasReadOnly()) {
      this.addAttribute(HTableDescriptor.READONLY, builder.getReadOnly());
    }
    for (ColumnSchema family: builder.getColumnsList()) {
      ColumnSchemaModel familyModel = new ColumnSchemaModel();
      familyModel.setName(family.getName());
      for (ColumnSchema.Attribute attr: family.getAttrsList()) {
        familyModel.addAttribute(attr.getName(), attr.getValue());
      }
      if (family.hasTtl()) {
        familyModel.addAttribute(HColumnDescriptor.TTL, family.getTtl());
      }
      if (family.hasMaxVersions()) {
        familyModel.addAttribute(HConstants.VERSIONS,
          family.getMaxVersions());
      }
      if (family.hasCompression()) {
        familyModel.addAttribute(HColumnDescriptor.COMPRESSION,
          family.getCompression());
      }
      this.addColumnFamily(familyModel);
    }
    return this;
  }
}

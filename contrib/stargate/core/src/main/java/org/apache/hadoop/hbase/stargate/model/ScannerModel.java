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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.stargate.ProtobufMessageHandler;
import org.apache.hadoop.hbase.stargate.protobuf.generated.ScannerMessage.Scanner;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONStringer;

import com.google.protobuf.ByteString;

/**
 * A representation of Scanner parameters.
 * 
 * <pre>
 * &lt;complexType name="Scanner"&gt;
 *   &lt;sequence>
 *     &lt;element name="column" type="base64Binary" minOccurs="0" maxOccurs="unbounded"/&gt;
 *   &lt;/sequence&gt;
 *   &lt;element name="filter" type="string" minOccurs="0" maxOccurs="1"&gt;&lt;/element&gt;
 *   &lt;attribute name="startRow" type="base64Binary"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="endRow" type="base64Binary"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="batch" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="startTime" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="endTime" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="maxVersions" type="int"&gt;&lt;/attribute&gt;
 * &lt;/complexType&gt;
 * </pre>
 */
@XmlRootElement(name="Scanner")
public class ScannerModel implements ProtobufMessageHandler, Serializable {

  static enum FilterType {
    ColumnCountGetFilter,
    FilterList,
    FirstKeyOnlyFilter,
    InclusiveStopFilter,
    PageFilter,
    PrefixFilter,
    QualifierFilter,
    RowFilter,
    SingleColumnValueFilter,
    SkipFilter,
    ValueFilter,
    WhileMatchFilter    
  }

  static enum ComparatorType {
    BinaryComparator,
    BinaryPrefixComparator,
    RegexStringComparator,
    SubstringComparator    
  }

  private static final long serialVersionUID = 1L;

  private byte[] startRow = HConstants.EMPTY_START_ROW;
  private byte[] endRow = HConstants.EMPTY_END_ROW;;
  private List<byte[]> columns = new ArrayList<byte[]>();
  private int batch = Integer.MAX_VALUE;
  private long startTime = 0;
  private long endTime = Long.MAX_VALUE;
  private String filter = null;
  private int maxVersions = Integer.MAX_VALUE;

  /**
   * @param o the JSONObject under construction
   * @return the JSONObject under construction
   * @throws Exception
   */
  public static WritableByteArrayComparable 
  buildWritableByteArrayComparable(final JSONObject o) throws Exception {
    String type = o.getString("type");
    String value = o.getString("value");
    WritableByteArrayComparable comparator;
    switch (ComparatorType.valueOf(type)) {
      case BinaryComparator: {
        comparator = new BinaryComparator(Base64.decode(value));
      } break;
      case BinaryPrefixComparator: {
        comparator = new BinaryPrefixComparator(Base64.decode(value));
      } break;
      case RegexStringComparator: {
        comparator = new RegexStringComparator(value);
      } break;
      case SubstringComparator: {
        comparator = new SubstringComparator(value);
      } break;
      default: {
        throw new RuntimeException("unhandled comparator type: " + type);
      }
    }
    return comparator;
  }

  /**
   * @param o the JSONObject under construction
   * @return the JSONObject under construction
   * @throws Exception
   */
  public static Filter buildFilter(final JSONObject o) throws Exception {
    String type = o.getString("type");
    Filter filter;
    switch (FilterType.valueOf(type)) {
      case ColumnCountGetFilter: {
        filter = new ColumnCountGetFilter(o.getInt("limit"));
      } break;
      case FilterList: {
        JSONArray arr = o.getJSONArray("filters");
        List<Filter> filters = new ArrayList<Filter>(arr.length());
        for (int i = 0; i < arr.length(); i++) {
          filters.add(buildFilter(arr.getJSONObject(i)));
        }
        filter = new FilterList(
          FilterList.Operator.valueOf(o.getString("op")),
          filters);
      } break;
      case FirstKeyOnlyFilter: {
        filter = new FirstKeyOnlyFilter();
      } break;
      case InclusiveStopFilter: {
        filter = new InclusiveStopFilter(Base64.decode(o.getString("value")));
      } break;
      case PageFilter: {
        filter = new PageFilter(o.getLong("value"));
      } break;
      case PrefixFilter: {
        filter = new PrefixFilter(Base64.decode(o.getString("value")));
      } break;
      case QualifierFilter: {
        filter = new QualifierFilter(CompareOp.valueOf(o.getString("op")),
          buildWritableByteArrayComparable(o.getJSONObject("comparator")));
      } break;
      case RowFilter: {
        filter = new RowFilter(CompareOp.valueOf(o.getString("op")),
          buildWritableByteArrayComparable(o.getJSONObject("comparator")));
      } break;
      case SingleColumnValueFilter: {
        filter = new SingleColumnValueFilter(
          Base64.decode(o.getString("family")),
          o.has("qualifier") ? Base64.decode(o.getString("qualifier")) : null,
          CompareOp.valueOf(o.getString("op")),
          buildWritableByteArrayComparable(o.getJSONObject("comparator")));
        if (o.has("ifMissing")) {
          ((SingleColumnValueFilter)filter)
            .setFilterIfMissing(o.getBoolean("ifMissing"));
        }
        if (o.has("latestVersion")) {
          ((SingleColumnValueFilter)filter)
            .setLatestVersionOnly(o.getBoolean("latestVersion"));
        }
      } break;
      case SkipFilter: {
        filter = new SkipFilter(buildFilter(o.getJSONObject("filter")));
      } break;
      case ValueFilter: {
        filter = new ValueFilter(CompareOp.valueOf(o.getString("op")),
          buildWritableByteArrayComparable(o.getJSONObject("comparator")));
      } break;
      case WhileMatchFilter: {
        filter = new WhileMatchFilter(buildFilter(o.getJSONObject("filter")));
      } break;
      default: {
        throw new RuntimeException("unhandled filter type: " + type);
      }
    }
    return filter;
  }

  /**
   * @param s the JSONStringer
   * @param comparator the comparator
   * @return the JSONStringer
   * @throws Exception
   */
  public static JSONStringer stringifyComparator(final JSONStringer s, 
      final WritableByteArrayComparable comparator) throws Exception {
    String typeName = comparator.getClass().getSimpleName();
    ComparatorType type = ComparatorType.valueOf(typeName);
    s.object();
    s.key("type").value(typeName);
    switch (type) {
      case BinaryComparator:
      case BinaryPrefixComparator:
        s.key("value").value(Base64.encodeBytes(comparator.getValue()));
        break;
      case RegexStringComparator:
      case SubstringComparator:
        s.key("value").value(Bytes.toString(comparator.getValue()));
        break;
      default:
        throw new RuntimeException("unhandled filter type: " + type);
    }
    s.endObject();
    return s;
  }

  /**
   * @param s the JSONStringer
   * @param filter the filter
   * @return the JSONStringer
   * @throws Exception
   */
  public static JSONStringer stringifyFilter(final JSONStringer s, 
      final Filter filter) throws Exception {
    String typeName = filter.getClass().getSimpleName();
    FilterType type;
    try { 
      type = FilterType.valueOf(typeName);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("filter type " + typeName + " not supported");
    }
    s.object();
    s.key("type").value(typeName);
    switch (type) {
      case ColumnCountGetFilter:
        s.key("limit").value(((ColumnCountGetFilter)filter).getLimit());
        break;
      case FilterList:
        s.key("op").value(((FilterList)filter).getOperator().toString());
        s.key("filters").array();
        for (Filter child: ((FilterList)filter).getFilters()) {
          stringifyFilter(s, child);
        }
        s.endArray();
        break;
      case FirstKeyOnlyFilter:
        break;
      case InclusiveStopFilter:
        s.key("value").value(
          Base64.encodeBytes(((InclusiveStopFilter)filter).getStopRowKey()));
        break;
      case PageFilter:
        s.key("value").value(((PageFilter)filter).getPageSize());
        break;
      case PrefixFilter:
        s.key("value")
          .value(Base64.encodeBytes(((PrefixFilter)filter).getPrefix()));
        break;
      case QualifierFilter:
      case RowFilter:
      case ValueFilter:
        s.key("op").value(((CompareFilter)filter).getOperator().toString());
        s.key("comparator");
        stringifyComparator(s, ((CompareFilter)filter).getComparator());
        break;
      case SingleColumnValueFilter: {
        SingleColumnValueFilter scvf = (SingleColumnValueFilter) filter;
        s.key("family").value(scvf.getFamily());
        byte[] qualifier = scvf.getQualifier();
        if (qualifier != null) {
          s.key("qualifier").value(qualifier);
        }
        s.key("op").value(scvf.getOperator().toString());
        s.key("comparator");
        stringifyComparator(s, scvf.getComparator());
        if (scvf.getFilterIfMissing()) {
          s.key("ifMissing").value(true);
        }
        if (scvf.getLatestVersionOnly()) {
          s.key("latestVersion").value(true);
        }
      } break;
      case SkipFilter:
        s.key("filter");
        stringifyFilter(s, ((SkipFilter)filter).getFilter());
        break;
      case WhileMatchFilter:
        s.key("filter");
        stringifyFilter(s, ((WhileMatchFilter)filter).getFilter());
        break;
    }
    s.endObject();
    return s;
  }

  /**
   * @param scan the scan specification
   * @throws Exception 
   */
  public static ScannerModel fromScan(Scan scan) throws Exception {
    ScannerModel model = new ScannerModel();
    model.setStartRow(scan.getStartRow());
    model.setEndRow(scan.getStopRow());
    byte[][] families = scan.getFamilies();
    if (families != null) {
      for (byte[] column: families) {
        model.addColumn(column);
      }
    }
    model.setStartTime(scan.getTimeRange().getMin());
    model.setEndTime(scan.getTimeRange().getMax());
    int caching = scan.getCaching();
    if (caching > 0) {
      model.setBatch(caching);
    }
    int maxVersions = scan.getMaxVersions();
    if (maxVersions > 0) {
      model.setMaxVersions(maxVersions);
    }
    Filter filter = scan.getFilter();
    if (filter != null) {
      model.setFilter(stringifyFilter(new JSONStringer(), filter).toString());
    }
    return model;
  }

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
   * @param maxVersions the maximum number of versions to return
   * @param filter a filter specification
   * (values with timestamps later than this are excluded)
   */
  public ScannerModel(byte[] startRow, byte[] endRow, List<byte[]> columns,
      int batch, long endTime, int maxVersions, String filter) {
    super();
    this.startRow = startRow;
    this.endRow = endRow;
    this.columns = columns;
    this.batch = batch;
    this.endTime = endTime;
    this.maxVersions = maxVersions;
    this.filter = filter;
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
   * @param filter a filter specification
   */
  public ScannerModel(byte[] startRow, byte[] endRow, List<byte[]> columns,
      int batch, long startTime, long endTime, String filter) {
    super();
    this.startRow = startRow;
    this.endRow = endRow;
    this.columns = columns;
    this.batch = batch;
    this.startTime = startTime;
    this.endTime = endTime;
    this.filter = filter;
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
   * @return maximum number of versions to return
   */
  @XmlAttribute
  public int getMaxVersions() {
    return maxVersions;
  }

  /**
   * @return the filter specification
   */
  @XmlElement
  public String getFilter() {
    return filter;
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
   * @param maxVersions maximum number of versions to return
   */
  public void setMaxVersions(int maxVersions) {
    this.maxVersions = maxVersions;
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

  /**
   * @param filter the filter specification
   */
  public void setFilter(String filter) {
    this.filter = filter;
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
    builder.setMaxVersions(maxVersions);
    if (filter != null) {
      builder.setFilter(filter);
    }
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
    if (builder.hasMaxVersions()) {
      maxVersions = builder.getMaxVersions();
    }
    if (builder.hasFilter()) {
      filter = builder.getFilter();
    }
    return this;
  }

}

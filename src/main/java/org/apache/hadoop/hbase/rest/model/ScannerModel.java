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
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.rest.ProtobufMessageHandler;
import org.apache.hadoop.hbase.rest.protobuf.generated.ScannerMessage.Scanner;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import com.sun.jersey.api.json.JSONUnmarshaller;

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

  private static final long serialVersionUID = 1L;

  private byte[] startRow = HConstants.EMPTY_START_ROW;
  private byte[] endRow = HConstants.EMPTY_END_ROW;;
  private List<byte[]> columns = new ArrayList<byte[]>();
  private int batch = Integer.MAX_VALUE;
  private long startTime = 0;
  private long endTime = Long.MAX_VALUE;
  private String filter = null;
  private int maxVersions = Integer.MAX_VALUE;

  @XmlRootElement
  static class FilterModel {
    
    @XmlRootElement
    static class WritableByteArrayComparableModel {
      @XmlAttribute public String type;
      @XmlAttribute public String value;
      @XmlAttribute public String op;

      static enum ComparatorType {
        BinaryComparator,
        BinaryPrefixComparator,
        BitComparator,
        NullComparator,
        RegexStringComparator,
        SubstringComparator    
      }

      public WritableByteArrayComparableModel() { }

      public WritableByteArrayComparableModel(
          WritableByteArrayComparable comparator) {
        String typeName = comparator.getClass().getSimpleName();
        ComparatorType type = ComparatorType.valueOf(typeName);
        this.type = typeName;
        switch (type) {
          case BinaryComparator:
          case BinaryPrefixComparator:
            this.value = Base64.encodeBytes(comparator.getValue());
            break;
          case BitComparator:
            this.value = Base64.encodeBytes(comparator.getValue());
            this.op = ((BitComparator)comparator).getOperator().toString();
            break;
          case NullComparator:
            break;
          case RegexStringComparator:
          case SubstringComparator:
            this.value = Bytes.toString(comparator.getValue());
            break;
          default:
            throw new RuntimeException("unhandled filter type: " + type);
        }
      }

      public WritableByteArrayComparable build() {
        WritableByteArrayComparable comparator;
        switch (ComparatorType.valueOf(type)) {
          case BinaryComparator:
            comparator = new BinaryComparator(Base64.decode(value));
            break;
          case BinaryPrefixComparator:
            comparator = new BinaryPrefixComparator(Base64.decode(value));
            break;
          case BitComparator:
            comparator = new BitComparator(Base64.decode(value),
                BitComparator.BitwiseOp.valueOf(op));
            break;
          case NullComparator:
            comparator = new NullComparator();
            break;
          case RegexStringComparator:
            comparator = new RegexStringComparator(value);
            break;
          case SubstringComparator:
            comparator = new SubstringComparator(value);
            break;
          default:
            throw new RuntimeException("unhandled comparator type: " + type);
        }
        return comparator;
      }

    }

    // A grab bag of fields, would have been a union if this were C.
    // These are null by default and will only be serialized if set (non null).
    @XmlAttribute public String type;
    @XmlAttribute public String op;
    @XmlElement WritableByteArrayComparableModel comparator;
    @XmlAttribute public String value;
    @XmlElement public List<FilterModel> filters;
    @XmlAttribute public Integer limit;
    @XmlAttribute public Integer offset;
    @XmlAttribute public String family;
    @XmlAttribute public String qualifier;
    @XmlAttribute public Boolean ifMissing;
    @XmlAttribute public Boolean latestVersion;
    @XmlAttribute public String minColumn;
    @XmlAttribute public Boolean minColumnInclusive;
    @XmlAttribute public String maxColumn;
    @XmlAttribute public Boolean maxColumnInclusive;
    @XmlAttribute public Boolean dropDependentColumn;
    @XmlAttribute public Float chance;
    @XmlElement public List<String> prefixes;
    @XmlElement public List<Long> timestamps;

    static enum FilterType {
      ColumnCountGetFilter,
      ColumnPaginationFilter,
      ColumnPrefixFilter,
      ColumnRangeFilter,
      DependentColumnFilter,
      FamilyFilter,
      FilterList,
      FirstKeyOnlyFilter,
      InclusiveStopFilter,
      KeyOnlyFilter,
      MultipleColumnPrefixFilter,
      PageFilter,
      PrefixFilter,
      QualifierFilter,
      RandomRowFilter,
      RowFilter,
      SingleColumnValueExcludeFilter,
      SingleColumnValueFilter,
      SkipFilter,
      TimestampsFilter,
      ValueFilter,
      WhileMatchFilter    
    }

    public FilterModel() { }
    
    public FilterModel(Filter filter) { 
      String typeName = filter.getClass().getSimpleName();
      FilterType type = FilterType.valueOf(typeName);
      this.type = typeName;
      switch (type) {
        case ColumnCountGetFilter:
          this.limit = ((ColumnCountGetFilter)filter).getLimit();
          break;
        case ColumnPaginationFilter:
          this.limit = ((ColumnPaginationFilter)filter).getLimit();
          this.offset = ((ColumnPaginationFilter)filter).getOffset();
          break;
        case ColumnPrefixFilter:
          this.value = Base64.encodeBytes(((ColumnPrefixFilter)filter).getPrefix());
          break;
        case ColumnRangeFilter:
          this.minColumn = Base64.encodeBytes(((ColumnRangeFilter)filter).getMinColumn());
          this.minColumnInclusive = ((ColumnRangeFilter)filter).getMinColumnInclusive();
          this.maxColumn = Base64.encodeBytes(((ColumnRangeFilter)filter).getMaxColumn());
          this.maxColumnInclusive = ((ColumnRangeFilter)filter).getMaxColumnInclusive();
          break;
        case DependentColumnFilter: {
          DependentColumnFilter dcf = (DependentColumnFilter)filter;
          this.family = Base64.encodeBytes(dcf.getFamily());
          byte[] qualifier = dcf.getQualifier();
          if (qualifier != null) {
            this.qualifier = Base64.encodeBytes(qualifier);
          }
          this.op = dcf.getOperator().toString();
          this.comparator = new WritableByteArrayComparableModel(dcf.getComparator());
          this.dropDependentColumn = dcf.dropDependentColumn();
        } break;
        case FilterList:
          this.op = ((FilterList)filter).getOperator().toString();
          this.filters = new ArrayList<FilterModel>();
          for (Filter child: ((FilterList)filter).getFilters()) {
            this.filters.add(new FilterModel(child));
          }
          break;
        case FirstKeyOnlyFilter:
        case KeyOnlyFilter:
          break;
        case InclusiveStopFilter:
          this.value = 
            Base64.encodeBytes(((InclusiveStopFilter)filter).getStopRowKey());
          break;
        case MultipleColumnPrefixFilter:
          this.prefixes = new ArrayList<String>();
          for (byte[] prefix: ((MultipleColumnPrefixFilter)filter).getPrefix()) {
            this.prefixes.add(Base64.encodeBytes(prefix));
          }
          break;
        case PageFilter:
          this.value = Long.toString(((PageFilter)filter).getPageSize());
          break;
        case PrefixFilter:
          this.value = Base64.encodeBytes(((PrefixFilter)filter).getPrefix());
          break;
        case FamilyFilter:
        case QualifierFilter:
        case RowFilter:
        case ValueFilter:
          this.op = ((CompareFilter)filter).getOperator().toString();
          this.comparator = 
            new WritableByteArrayComparableModel(
              ((CompareFilter)filter).getComparator());
          break;
        case RandomRowFilter:
          this.chance = ((RandomRowFilter)filter).getChance();
          break;
        case SingleColumnValueExcludeFilter:
        case SingleColumnValueFilter: {
          SingleColumnValueFilter scvf = (SingleColumnValueFilter) filter;
          this.family = Base64.encodeBytes(scvf.getFamily());
          byte[] qualifier = scvf.getQualifier();
          if (qualifier != null) {
            this.qualifier = Base64.encodeBytes(qualifier);
          }
          this.op = scvf.getOperator().toString();
          this.comparator = 
            new WritableByteArrayComparableModel(scvf.getComparator());
          if (scvf.getFilterIfMissing()) {
            this.ifMissing = true;
          }
          if (scvf.getLatestVersionOnly()) {
            this.latestVersion = true;
          }
        } break;
        case SkipFilter:
          this.filters = new ArrayList<FilterModel>();
          this.filters.add(new FilterModel(((SkipFilter)filter).getFilter()));
          break;
        case TimestampsFilter:
          this.timestamps = ((TimestampsFilter)filter).getTimestamps();
          break;
        case WhileMatchFilter:
          this.filters = new ArrayList<FilterModel>();
          this.filters.add(
            new FilterModel(((WhileMatchFilter)filter).getFilter()));
          break;
        default:
          throw new RuntimeException("unhandled filter type " + type);
      }
    }

    public Filter build() {
      Filter filter;
      switch (FilterType.valueOf(type)) {
      case ColumnCountGetFilter:
        filter = new ColumnCountGetFilter(limit);
        break;
      case ColumnPaginationFilter:
        filter = new ColumnPaginationFilter(limit, offset);
        break;
      case ColumnPrefixFilter:
        filter = new ColumnPrefixFilter(Base64.decode(value));
        break;
      case ColumnRangeFilter:
        filter = new ColumnRangeFilter(Base64.decode(minColumn),
            minColumnInclusive, Base64.decode(maxColumn),
            maxColumnInclusive);
        break;
      case DependentColumnFilter:
        filter = new DependentColumnFilter(Base64.decode(family),
            qualifier != null ? Base64.decode(qualifier) : null,
            dropDependentColumn, CompareOp.valueOf(op), comparator.build());
        break;
      case FamilyFilter:
        filter = new FamilyFilter(CompareOp.valueOf(op), comparator.build());
        break;
      case FilterList: {
        List<Filter> list = new ArrayList<Filter>();
        for (FilterModel model: filters) {
          list.add(model.build());
        }
        filter = new FilterList(FilterList.Operator.valueOf(op), list);
      } break;
      case FirstKeyOnlyFilter:
        filter = new FirstKeyOnlyFilter();
        break;
      case InclusiveStopFilter:
        filter = new InclusiveStopFilter(Base64.decode(value));
        break;
      case KeyOnlyFilter:
        filter = new KeyOnlyFilter();
        break;
      case MultipleColumnPrefixFilter: {
        byte[][] values = new byte[prefixes.size()][];
        for (int i = 0; i < prefixes.size(); i++) {
          values[i] = Base64.decode(prefixes.get(i));
        }
        filter = new MultipleColumnPrefixFilter(values);
      } break;
      case PageFilter:
        filter = new PageFilter(Long.valueOf(value));
        break;
      case PrefixFilter:
        filter = new PrefixFilter(Base64.decode(value));
        break;
      case QualifierFilter:
        filter = new QualifierFilter(CompareOp.valueOf(op), comparator.build());
        break;
      case RandomRowFilter:
        filter = new RandomRowFilter(chance);
        break;
      case RowFilter:
        filter = new RowFilter(CompareOp.valueOf(op), comparator.build());
        break;
      case SingleColumnValueFilter:
        filter = new SingleColumnValueFilter(Base64.decode(family),
          qualifier != null ? Base64.decode(qualifier) : null,
          CompareOp.valueOf(op), comparator.build());
        if (ifMissing != null) {
          ((SingleColumnValueFilter)filter).setFilterIfMissing(ifMissing);
        }
        if (latestVersion != null) {
          ((SingleColumnValueFilter)filter).setLatestVersionOnly(latestVersion);
        }
        break;
      case SingleColumnValueExcludeFilter:
        filter = new SingleColumnValueExcludeFilter(Base64.decode(family),
          qualifier != null ? Base64.decode(qualifier) : null,
          CompareOp.valueOf(op), comparator.build());
        if (ifMissing != null) {
          ((SingleColumnValueExcludeFilter)filter).setFilterIfMissing(ifMissing);
        }
        if (latestVersion != null) {
          ((SingleColumnValueExcludeFilter)filter).setLatestVersionOnly(latestVersion);
        }
        break;
      case SkipFilter:
        filter = new SkipFilter(filters.get(0).build());
        break;
      case TimestampsFilter:
        filter = new TimestampsFilter(timestamps);
        break;
      case ValueFilter:
        filter = new ValueFilter(CompareOp.valueOf(op), comparator.build());
        break;
      case WhileMatchFilter:
        filter = new WhileMatchFilter(filters.get(0).build());
        break;
      default:
        throw new RuntimeException("unhandled filter type: " + type);
      }
      return filter;
    }

  }

  /**
   * @param s the JSON representation of the filter
   * @return the filter
   * @throws Exception
   */
  public static Filter buildFilter(String s) throws Exception {
    JSONJAXBContext context =
      new JSONJAXBContext(JSONConfiguration.natural().build(),
        FilterModel.class);
    JSONUnmarshaller unmarshaller = context.createJSONUnmarshaller();
    FilterModel model = unmarshaller.unmarshalFromJSON(new StringReader(s),
      FilterModel.class);
    return model.build();
  }

  /**
   * @param filter the filter
   * @return the JSON representation of the filter
   * @throws Exception 
   */
  public static String stringifyFilter(final Filter filter) throws Exception {
    JSONJAXBContext context =
      new JSONJAXBContext(JSONConfiguration.natural().build(),
        FilterModel.class);
    JSONMarshaller marshaller = context.createJSONMarshaller();
    StringWriter writer = new StringWriter();
    marshaller.marshallToJSON(new FilterModel(filter), writer);
    return writer.toString();
  }

  private static final byte[] COLUMN_DIVIDER = Bytes.toBytes(":");

  /**
   * @param scan the scan specification
   * @throws Exception 
   */
  public static ScannerModel fromScan(Scan scan) throws Exception {
    ScannerModel model = new ScannerModel();
    model.setStartRow(scan.getStartRow());
    model.setEndRow(scan.getStopRow());
    Map<byte [], NavigableSet<byte []>> families = scan.getFamilyMap();
    if (families != null) {
      for (Map.Entry<byte [], NavigableSet<byte []>> entry : families.entrySet()) {
        if (entry.getValue() != null) {
          for (byte[] qualifier: entry.getValue()) {
            model.addColumn(Bytes.add(entry.getKey(), COLUMN_DIVIDER, qualifier));
          }
        } else {
          model.addColumn(entry.getKey());
        }
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
      model.setFilter(stringifyFilter(filter));
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

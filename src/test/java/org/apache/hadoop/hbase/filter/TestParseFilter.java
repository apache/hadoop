/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests ParseFilter.java
 * It tests the entire work flow from when a string is given by the user
 * and how it is parsed to construct the corresponding Filter object
 */
public class TestParseFilter {

  ParseFilter f;
  Filter filter;

  @Before
  public void setUp() throws Exception {
    f = new ParseFilter();
  }

  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  @Test
  public void testKeyOnlyFilter() throws IOException {
    String filterString = " KeyOnlyFilter( ) ";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof KeyOnlyFilter);

    String filterString2 = "KeyOnlyFilter ('') ";
    byte [] filterStringAsByteArray2 = Bytes.toBytes(filterString2);
    try {
      filter = f.parseFilterString(filterStringAsByteArray2);
      assertTrue(filter instanceof KeyOnlyFilter);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testFirstKeyOnlyFilter() throws IOException {
    String filterString = " FirstKeyOnlyFilter( ) ";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof FirstKeyOnlyFilter);

    String filterString2 = " FirstKeyOnlyFilter ('') ";
    byte [] filterStringAsByteArray2 = Bytes.toBytes(filterString2);
    try {
      filter = f.parseFilterString(filterStringAsByteArray2);
      assertTrue(filter instanceof FirstKeyOnlyFilter);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testPrefixFilter() throws IOException {
    String filterString = " PrefixFilter('row' ) ";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof PrefixFilter);

    PrefixFilter prefixFilter = (PrefixFilter) filter;
    byte [] prefix = prefixFilter.getPrefix();
    assertEquals(new String(prefix), "row");

    filterString = " PrefixFilter(row)";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof PrefixFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("PrefixFilter needs a quoted string");
    }

  }

  @Test
  public void testColumnPrefixFilter() throws IOException {
    String filterString = " ColumnPrefixFilter('qualifier' ) ";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof ColumnPrefixFilter);
    byte [] columnPrefix = ((ColumnPrefixFilter)filter).getPrefix();
    assertEquals(new String(columnPrefix), "qualifier");
  }

  @Test
  public void testMultipleColumnPrefixFilter() throws IOException {
    String filterString = " MultipleColumnPrefixFilter('qualifier1', 'qualifier2' ) ";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof MultipleColumnPrefixFilter);
    byte [][] prefixes = ((MultipleColumnPrefixFilter)filter).getPrefix();
    assertEquals(new String(prefixes[0]), "qualifier1");
    assertEquals(new String(prefixes[1]), "qualifier2");
  }

  @Test
  public void testColumnCountGetFilter() throws IOException {
    String filterString = " ColumnCountGetFilter(4)";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof ColumnCountGetFilter);
    int limit = ((ColumnCountGetFilter)filter).getLimit();
    assertEquals(limit, 4);

    filterString = " ColumnCountGetFilter('abc')";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof ColumnCountGetFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("ColumnCountGetFilter needs an int as an argument");
    }

    filterString = " ColumnCountGetFilter(2147483648)";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof ColumnCountGetFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("Integer argument too large");
    }
  }

  @Test
  public void testPageFilter() throws IOException {
    String filterString = " PageFilter(4)";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof PageFilter);

    long pageSize = ((PageFilter)filter).getPageSize();
    assertEquals(pageSize, 4);

    filterString = " PageFilter('123')";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof PageFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("PageFilter needs an int as an argument");
    }
  }

  @Test
  public void testColumnPaginationFilter() throws IOException {
    String filterString = "ColumnPaginationFilter(4, 6)";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof ColumnPaginationFilter);

    int limit = ((ColumnPaginationFilter)filter).getLimit();
    assertEquals(limit, 4);
    int offset = ((ColumnPaginationFilter)filter).getOffset();
    assertEquals(offset, 6);

    filterString = " ColumnPaginationFilter('124')";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof ColumnPaginationFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("ColumnPaginationFilter needs two arguments");
    }

    filterString = " ColumnPaginationFilter('4' , '123a')";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof ColumnPaginationFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("ColumnPaginationFilter needs two ints as arguments");
    }

    filterString = " ColumnPaginationFilter('4' , '-123')";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof ColumnPaginationFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("ColumnPaginationFilter arguments should not be negative");
    }
  }

  @Test
  public void testInclusiveStopFilter() throws IOException {
    String filterString = "InclusiveStopFilter ('row 3')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof InclusiveStopFilter);

    byte [] stopRowKey = ((InclusiveStopFilter)filter).getStopRowKey();
    assertEquals(new String(stopRowKey), "row 3");
  }


  @Test
  public void testTimestampsFilter() throws IOException {
    String filterString = "TimestampsFilter(9223372036854775806, 6)";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof TimestampsFilter);

    TreeSet<Long> timestamps = ((TimestampsFilter)filter).getTimestamps();
    assertEquals(timestamps.size(), 2);
    assertTrue(timestamps.contains(new Long(6)));

    filterString = "TimestampsFilter()";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof TimestampsFilter);

    timestamps = ((TimestampsFilter)filter).getTimestamps();
    assertEquals(timestamps.size(), 0);

    filterString = "TimestampsFilter(9223372036854775808, 6)";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof TimestampsFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("Long Argument was too large");
    }

    filterString = "TimestampsFilter(-45, 6)";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof TimestampsFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("Timestamp Arguments should not be negative");
    }
  }

  @Test
  public void testRowFilter() throws IOException {
    String filterString = "RowFilter ( =,   'binary:regionse')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof RowFilter);

    RowFilter rowFilter = (RowFilter)filter;
    assertEquals(CompareFilter.CompareOp.EQUAL, rowFilter.getOperator());
    assertTrue(rowFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) rowFilter.getComparator();
    assertEquals("regionse", new String(binaryComparator.getValue()));
  }

  @Test
  public void testFamilyFilter() throws IOException {
    String filterString = "FamilyFilter(>=, 'binaryprefix:pre')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof FamilyFilter);

    FamilyFilter familyFilter = (FamilyFilter)filter;
    assertEquals(CompareFilter.CompareOp.GREATER_OR_EQUAL, familyFilter.getOperator());
    assertTrue(familyFilter.getComparator() instanceof BinaryPrefixComparator);
    BinaryPrefixComparator binaryPrefixComparator =
      (BinaryPrefixComparator) familyFilter.getComparator();
    assertEquals("pre", new String(binaryPrefixComparator.getValue()));
  }

  @Test
  public void testQualifierFilter() throws IOException {
    String filterString = "QualifierFilter(=, 'regexstring:pre*')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof QualifierFilter);

    QualifierFilter qualifierFilter = (QualifierFilter) filter;
    assertEquals(CompareFilter.CompareOp.EQUAL, qualifierFilter.getOperator());
    assertTrue(qualifierFilter.getComparator() instanceof RegexStringComparator);
    RegexStringComparator regexStringComparator =
      (RegexStringComparator) qualifierFilter.getComparator();
    assertEquals("pre*", new String(regexStringComparator.getValue()));
  }

  @Test
  public void testValueFilter() throws IOException {
    String filterString = "ValueFilter(!=, 'substring:pre')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof ValueFilter);

    ValueFilter valueFilter = (ValueFilter) filter;
    assertEquals(CompareFilter.CompareOp.NOT_EQUAL, valueFilter.getOperator());
    assertTrue(valueFilter.getComparator() instanceof SubstringComparator);
    SubstringComparator substringComparator =
      (SubstringComparator) valueFilter.getComparator();
    assertEquals("pre", new String(substringComparator.getValue()));
  }

  @Test
  public void testColumnRangeFilter() throws IOException {
    String filterString = "ColumnRangeFilter('abc', true, 'xyz', false)";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof ColumnRangeFilter);

    ColumnRangeFilter columnRangeFilter = (ColumnRangeFilter) filter;
    assertEquals("abc", new String(columnRangeFilter.getMinColumn()));
    assertEquals("xyz", new String(columnRangeFilter.getMaxColumn()));
    assertTrue(columnRangeFilter.isMinColumnInclusive());
    assertFalse(columnRangeFilter.isMaxColumnInclusive());
  }

  @Test
  public void testDependentColumnFilter() throws IOException {
    String filterString = "DependentColumnFilter('family', 'qualifier', true, =, 'binary:abc')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof DependentColumnFilter);

    DependentColumnFilter dependentColumnFilter = (DependentColumnFilter) filter;
    assertEquals("family", new String(dependentColumnFilter.getFamily()));
    assertEquals("qualifier", new String(dependentColumnFilter.getQualifier()));
    assertTrue(dependentColumnFilter.getDropDependentColumn());
    assertEquals(CompareFilter.CompareOp.EQUAL, dependentColumnFilter.getOperator());
    assertTrue(dependentColumnFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator)dependentColumnFilter.getComparator();
    assertEquals("abc", new String(binaryComparator.getValue()));
  }

  @Test
  public void testSingleColumnValueFilter() throws IOException {

    String filterString = "SingleColumnValueFilter " +
      "('family', 'qualifier', >=, 'binary:a', true, false)";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);

    assertTrue(filter instanceof SingleColumnValueFilter);

    SingleColumnValueFilter singleColumnValueFilter = (SingleColumnValueFilter)filter;
    assertEquals("family", new String(singleColumnValueFilter.getFamily()));
    assertEquals("qualifier", new String(singleColumnValueFilter.getQualifier()));
    assertEquals(singleColumnValueFilter.getOperator(), CompareFilter.CompareOp.GREATER_OR_EQUAL);
    assertTrue(singleColumnValueFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) singleColumnValueFilter.getComparator();
    assertEquals(new String(binaryComparator.getValue()), "a");
    assertTrue(singleColumnValueFilter.getFilterIfMissing());
    assertFalse(singleColumnValueFilter.getLatestVersionOnly());


    filterString = "SingleColumnValueFilter ('family', 'qualifier', >, 'binaryprefix:a')";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);

    assertTrue(filter instanceof SingleColumnValueFilter);

    singleColumnValueFilter = (SingleColumnValueFilter)filter;
    assertEquals("family", new String(singleColumnValueFilter.getFamily()));
    assertEquals("qualifier", new String(singleColumnValueFilter.getQualifier()));
    assertEquals(singleColumnValueFilter.getOperator(), CompareFilter.CompareOp.GREATER);
    assertTrue(singleColumnValueFilter.getComparator() instanceof BinaryPrefixComparator);
    BinaryPrefixComparator binaryPrefixComparator =
      (BinaryPrefixComparator) singleColumnValueFilter.getComparator();
    assertEquals(new String(binaryPrefixComparator.getValue()), "a");
    assertFalse(singleColumnValueFilter.getFilterIfMissing());
    assertTrue(singleColumnValueFilter.getLatestVersionOnly());
  }

  @Test
  public void testSingleColumnValueExcludeFilter() throws IOException {

    String filterString =
      "SingleColumnValueExcludeFilter ('family', 'qualifier', <, 'binaryprefix:a')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof SingleColumnValueExcludeFilter);

    SingleColumnValueExcludeFilter singleColumnValueExcludeFilter =
      (SingleColumnValueExcludeFilter)filter;
    assertEquals(singleColumnValueExcludeFilter.getOperator(), CompareFilter.CompareOp.LESS);
    assertEquals("family", new String(singleColumnValueExcludeFilter.getFamily()));
    assertEquals("qualifier", new String(singleColumnValueExcludeFilter.getQualifier()));
    assertEquals(new String(singleColumnValueExcludeFilter.getComparator().getValue()), "a");
    assertFalse(singleColumnValueExcludeFilter.getFilterIfMissing());
    assertTrue(singleColumnValueExcludeFilter.getLatestVersionOnly());

    filterString = "SingleColumnValueExcludeFilter " +
      "('family', 'qualifier', <=, 'binaryprefix:a', true, false)";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof SingleColumnValueExcludeFilter);

    singleColumnValueExcludeFilter = (SingleColumnValueExcludeFilter)filter;
    assertEquals("family", new String(singleColumnValueExcludeFilter.getFamily()));
    assertEquals("qualifier", new String(singleColumnValueExcludeFilter.getQualifier()));
    assertEquals(singleColumnValueExcludeFilter.getOperator(),
                 CompareFilter.CompareOp.LESS_OR_EQUAL);
    assertTrue(singleColumnValueExcludeFilter.getComparator() instanceof BinaryPrefixComparator);
    BinaryPrefixComparator binaryPrefixComparator =
      (BinaryPrefixComparator) singleColumnValueExcludeFilter.getComparator();
    assertEquals(new String(binaryPrefixComparator.getValue()), "a");
    assertTrue(singleColumnValueExcludeFilter.getFilterIfMissing());
    assertFalse(singleColumnValueExcludeFilter.getLatestVersionOnly());
  }

  @Test
  public void testSkipFilter() throws IOException {
    String filterString = "SKIP ValueFilter( =,  'binary:0')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof SkipFilter);
    SkipFilter skipFilter = (SkipFilter) filter;
    assertTrue(skipFilter.getFilter() instanceof ValueFilter);
    ValueFilter valueFilter = (ValueFilter) skipFilter.getFilter();

    assertEquals(CompareFilter.CompareOp.EQUAL, valueFilter.getOperator());
    assertTrue(valueFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) valueFilter.getComparator();
    assertEquals("0", new String(binaryComparator.getValue()));
  }

  @Test
  public void testWhileFilter() throws IOException {
    String filterString = " WHILE   RowFilter ( !=, 'binary:row1')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof WhileMatchFilter);
    WhileMatchFilter whileMatchFilter = (WhileMatchFilter) filter;
    assertTrue(whileMatchFilter.getFilter() instanceof RowFilter);
    RowFilter rowFilter = (RowFilter) whileMatchFilter.getFilter();

    assertEquals(CompareFilter.CompareOp.NOT_EQUAL, rowFilter.getOperator());
    assertTrue(rowFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) rowFilter.getComparator();
    assertEquals("row1", new String(binaryComparator.getValue()));
  }

  @Test
  public void testCompoundFilter1() throws IOException {
    String filterString = " (PrefixFilter ('realtime')AND  FirstKeyOnlyFilter())";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    FilterList filterList = (FilterList)filter;
    ArrayList<Filter> filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof PrefixFilter);
    assertTrue(filters.get(1) instanceof FirstKeyOnlyFilter);

    PrefixFilter PrefixFilter = (PrefixFilter) filters.get(0);
    byte [] prefix = PrefixFilter.getPrefix();
    assertEquals(new String(prefix), "realtime");
    FirstKeyOnlyFilter firstKeyOnlyFilter = (FirstKeyOnlyFilter) filters.get(1);
  }

  @Test
  public void testCompoundFilter2() throws IOException {
    String filterString = "(PrefixFilter('realtime') AND QualifierFilter (>=, 'binary:e'))" +
      "OR FamilyFilter (=, 'binary:qualifier') ";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);

    FilterList filterList = (FilterList)filter;
    ArrayList<Filter> filterListFilters = (ArrayList<Filter>) filterList.getFilters();
    assertTrue(filterListFilters.get(0) instanceof FilterList);
    assertTrue(filterListFilters.get(1) instanceof FamilyFilter);
    assertEquals(filterList.getOperator(), FilterList.Operator.MUST_PASS_ONE);

    filterList = (FilterList) filterListFilters.get(0);
    FamilyFilter familyFilter = (FamilyFilter) filterListFilters.get(1);

    filterListFilters = (ArrayList<Filter>)filterList.getFilters();
    assertTrue(filterListFilters.get(0) instanceof PrefixFilter);
    assertTrue(filterListFilters.get(1) instanceof QualifierFilter);
    assertEquals(filterList.getOperator(), FilterList.Operator.MUST_PASS_ALL);

    assertEquals(CompareFilter.CompareOp.EQUAL, familyFilter.getOperator());
    assertTrue(familyFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) familyFilter.getComparator();
    assertEquals("qualifier", new String(binaryComparator.getValue()));

    PrefixFilter prefixFilter = (PrefixFilter) filterListFilters.get(0);
    byte [] prefix = prefixFilter.getPrefix();
    assertEquals(new String(prefix), "realtime");

    QualifierFilter qualifierFilter = (QualifierFilter) filterListFilters.get(1);
    assertEquals(CompareFilter.CompareOp.GREATER_OR_EQUAL, qualifierFilter.getOperator());
    assertTrue(qualifierFilter.getComparator() instanceof BinaryComparator);
    binaryComparator = (BinaryComparator) qualifierFilter.getComparator();
    assertEquals("e", new String(binaryComparator.getValue()));
  }

  @Test
  public void testCompoundFilter3() throws IOException {
    String filterString = " ColumnPrefixFilter ('realtime')AND  " +
      "FirstKeyOnlyFilter() OR SKIP FamilyFilter(=, 'substring:hihi')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    FilterList filterList = (FilterList)filter;
    ArrayList<Filter> filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof FilterList);
    assertTrue(filters.get(1) instanceof SkipFilter);

    filterList = (FilterList) filters.get(0);
    SkipFilter skipFilter = (SkipFilter) filters.get(1);

    filters = (ArrayList<Filter>) filterList.getFilters();
    assertTrue(filters.get(0) instanceof ColumnPrefixFilter);
    assertTrue(filters.get(1) instanceof FirstKeyOnlyFilter);

    ColumnPrefixFilter columnPrefixFilter = (ColumnPrefixFilter) filters.get(0);
    byte [] columnPrefix = columnPrefixFilter.getPrefix();
    assertEquals(new String(columnPrefix), "realtime");

    FirstKeyOnlyFilter firstKeyOnlyFilter = (FirstKeyOnlyFilter) filters.get(1);

    assertTrue(skipFilter.getFilter() instanceof FamilyFilter);
    FamilyFilter familyFilter = (FamilyFilter) skipFilter.getFilter();

    assertEquals(CompareFilter.CompareOp.EQUAL, familyFilter.getOperator());
    assertTrue(familyFilter.getComparator() instanceof SubstringComparator);
    SubstringComparator substringComparator =
      (SubstringComparator) familyFilter.getComparator();
    assertEquals("hihi", new String(substringComparator.getValue()));
  }

  @Test
  public void testCompoundFilter4() throws IOException {
    String filterString = " ColumnPrefixFilter ('realtime') OR " +
      "FirstKeyOnlyFilter() OR SKIP FamilyFilter(=, 'substring:hihi')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    FilterList filterList = (FilterList)filter;
    ArrayList<Filter> filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof ColumnPrefixFilter);
    assertTrue(filters.get(1) instanceof FirstKeyOnlyFilter);
    assertTrue(filters.get(2) instanceof SkipFilter);

    ColumnPrefixFilter columnPrefixFilter = (ColumnPrefixFilter) filters.get(0);
    FirstKeyOnlyFilter firstKeyOnlyFilter = (FirstKeyOnlyFilter) filters.get(1);
    SkipFilter skipFilter = (SkipFilter) filters.get(2);

    byte [] columnPrefix = columnPrefixFilter.getPrefix();
    assertEquals(new String(columnPrefix), "realtime");

    assertTrue(skipFilter.getFilter() instanceof FamilyFilter);
    FamilyFilter familyFilter = (FamilyFilter) skipFilter.getFilter();

    assertEquals(CompareFilter.CompareOp.EQUAL, familyFilter.getOperator());
    assertTrue(familyFilter.getComparator() instanceof SubstringComparator);
    SubstringComparator substringComparator =
      (SubstringComparator) familyFilter.getComparator();
    assertEquals("hihi", new String(substringComparator.getValue()));
  }

  @Test
  public void testIncorrectCompareOperator() throws IOException {
    String filterString = "RowFilter ('>>' , 'binary:region')";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof RowFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("Incorrect compare operator >>");
    }
  }

  @Test
  public void testIncorrectComparatorType () throws IOException {
    String  filterString = "RowFilter ('>=' , 'binaryoperator:region')";
    byte []  filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof RowFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("Incorrect comparator type: binaryoperator");
    }

    filterString = "RowFilter ('>=' 'regexstring:pre*')";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof RowFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("RegexStringComparator can only be used with EQUAL or NOT_EQUAL");
    }

    filterString = "SingleColumnValueFilter" +
      " ('family', 'qualifier', '>=', 'substring:a', 'true', 'false')')";
    filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(filter instanceof RowFilter);
    } catch (IllegalArgumentException e) {
      System.out.println("SubtringComparator can only be used with EQUAL or NOT_EQUAL");
    }
  }

  @Test
  public void testPrecedence1() throws IOException {
    String filterString = " (PrefixFilter ('realtime')AND  FirstKeyOnlyFilter()" +
      " OR KeyOnlyFilter())";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    FilterList filterList = (FilterList)filter;
    ArrayList<Filter> filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof FilterList);
    assertTrue(filters.get(1) instanceof KeyOnlyFilter);

    filterList = (FilterList) filters.get(0);
    filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof PrefixFilter);
    assertTrue(filters.get(1) instanceof FirstKeyOnlyFilter);

    PrefixFilter prefixFilter = (PrefixFilter)filters.get(0);
    byte [] prefix = prefixFilter.getPrefix();
    assertEquals(new String(prefix), "realtime");
  }

  @Test
  public void testPrecedence2() throws IOException {
    String filterString = " PrefixFilter ('realtime')AND  SKIP FirstKeyOnlyFilter()" +
      "OR KeyOnlyFilter()";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    FilterList filterList = (FilterList)filter;
    ArrayList<Filter> filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof FilterList);
    assertTrue(filters.get(1) instanceof KeyOnlyFilter);

    filterList = (FilterList) filters.get(0);
    filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof PrefixFilter);
    assertTrue(filters.get(1) instanceof SkipFilter);

    PrefixFilter prefixFilter = (PrefixFilter)filters.get(0);
    byte [] prefix = prefixFilter.getPrefix();
    assertEquals(new String(prefix), "realtime");

    SkipFilter skipFilter = (SkipFilter)filters.get(1);
    assertTrue(skipFilter.getFilter() instanceof FirstKeyOnlyFilter);
  }

  @Test
  public void testUnescapedQuote1 () throws IOException {
    String  filterString = "InclusiveStopFilter ('row''3')";
    System.out.println(filterString);
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof InclusiveStopFilter);

    byte [] stopRowKey = ((InclusiveStopFilter)filter).getStopRowKey();
    assertEquals(new String(stopRowKey), "row'3");
  }

  @Test
  public void testUnescapedQuote2 () throws IOException {
    String  filterString = "InclusiveStopFilter ('row''3''')";
    System.out.println(filterString);
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof InclusiveStopFilter);

    byte [] stopRowKey = ((InclusiveStopFilter)filter).getStopRowKey();
    assertEquals(new String(stopRowKey), "row'3'");
  }

  @Test
  public void testUnescapedQuote3 () throws IOException {
    String  filterString = "	InclusiveStopFilter ('''')";
    System.out.println(filterString);
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof InclusiveStopFilter);

    byte [] stopRowKey = ((InclusiveStopFilter)filter).getStopRowKey();
    assertEquals(new String(stopRowKey), "'");
  }

  @Test
  public void testIncorrectFilterString () throws IOException {
    String  filterString = "()";
    System.out.println(filterString);
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }

    filterString = " OR KeyOnlyFilter() FirstKeyOnlyFilter()";
    System.out.println(filterString);
    filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
  }

  @Test
  public void testCorrectFilterString () throws IOException {
    String  filterString = "(FirstKeyOnlyFilter())";
    System.out.println(filterString);
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertTrue(filter instanceof FirstKeyOnlyFilter);
  }
}

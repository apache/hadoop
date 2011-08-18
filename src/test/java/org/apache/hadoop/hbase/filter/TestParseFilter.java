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
    String filterString = "KeyOnlyFilter()";
    doTestFilter(filterString, KeyOnlyFilter.class);

    String filterString2 = "KeyOnlyFilter ('') ";
    byte [] filterStringAsByteArray2 = Bytes.toBytes(filterString2);
    try {
      filter = f.parseFilterString(filterStringAsByteArray2);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testFirstKeyOnlyFilter() throws IOException {
    String filterString = " FirstKeyOnlyFilter( ) ";
    doTestFilter(filterString, FirstKeyOnlyFilter.class);

    String filterString2 = " FirstKeyOnlyFilter ('') ";
    byte [] filterStringAsByteArray2 = Bytes.toBytes(filterString2);
    try {
      filter = f.parseFilterString(filterStringAsByteArray2);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testPrefixFilter() throws IOException {
    String filterString = " PrefixFilter('row' ) ";
    PrefixFilter prefixFilter = doTestFilter(filterString, PrefixFilter.class);
    byte [] prefix = prefixFilter.getPrefix();
    assertEquals(new String(prefix), "row");


    filterString = " PrefixFilter(row)";
    try {
      doTestFilter(filterString, PrefixFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testColumnPrefixFilter() throws IOException {
    String filterString = " ColumnPrefixFilter('qualifier' ) ";
    ColumnPrefixFilter columnPrefixFilter =
      doTestFilter(filterString, ColumnPrefixFilter.class);
    byte [] columnPrefix = columnPrefixFilter.getPrefix();
    assertEquals(new String(columnPrefix), "qualifier");
  }

  @Test
  public void testMultipleColumnPrefixFilter() throws IOException {
    String filterString = " MultipleColumnPrefixFilter('qualifier1', 'qualifier2' ) ";
    MultipleColumnPrefixFilter multipleColumnPrefixFilter =
      doTestFilter(filterString, MultipleColumnPrefixFilter.class);
    byte [][] prefixes = multipleColumnPrefixFilter.getPrefix();
    assertEquals(new String(prefixes[0]), "qualifier1");
    assertEquals(new String(prefixes[1]), "qualifier2");
  }

  @Test
  public void testColumnCountGetFilter() throws IOException {
    String filterString = " ColumnCountGetFilter(4)";
    ColumnCountGetFilter columnCountGetFilter =
      doTestFilter(filterString, ColumnCountGetFilter.class);
    int limit = columnCountGetFilter.getLimit();
    assertEquals(limit, 4);

    filterString = " ColumnCountGetFilter('abc')";
    try {
      doTestFilter(filterString, ColumnCountGetFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }

    filterString = " ColumnCountGetFilter(2147483648)";
    try {
      doTestFilter(filterString, ColumnCountGetFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testPageFilter() throws IOException {
    String filterString = " PageFilter(4)";
    PageFilter pageFilter =
      doTestFilter(filterString, PageFilter.class);
    long pageSize = pageFilter.getPageSize();
    assertEquals(pageSize, 4);

    filterString = " PageFilter('123')";
    try {
      doTestFilter(filterString, PageFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("PageFilter needs an int as an argument");
    }
  }

  @Test
  public void testColumnPaginationFilter() throws IOException {
    String filterString = "ColumnPaginationFilter(4, 6)";
    ColumnPaginationFilter columnPaginationFilter =
      doTestFilter(filterString, ColumnPaginationFilter.class);
    int limit = columnPaginationFilter.getLimit();
    assertEquals(limit, 4);
    int offset = columnPaginationFilter.getOffset();
    assertEquals(offset, 6);

    filterString = " ColumnPaginationFilter('124')";
    try {
      doTestFilter(filterString, ColumnPaginationFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("ColumnPaginationFilter needs two arguments");
    }

    filterString = " ColumnPaginationFilter('4' , '123a')";
    try {
      doTestFilter(filterString, ColumnPaginationFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("ColumnPaginationFilter needs two ints as arguments");
    }

    filterString = " ColumnPaginationFilter('4' , '-123')";
    try {
      doTestFilter(filterString, ColumnPaginationFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("ColumnPaginationFilter arguments should not be negative");
    }
  }

  @Test
  public void testInclusiveStopFilter() throws IOException {
    String filterString = "InclusiveStopFilter ('row 3')";
    InclusiveStopFilter inclusiveStopFilter =
      doTestFilter(filterString, InclusiveStopFilter.class);
    byte [] stopRowKey = inclusiveStopFilter.getStopRowKey();
    assertEquals(new String(stopRowKey), "row 3");
  }


  @Test
  public void testTimestampsFilter() throws IOException {
    String filterString = "TimestampsFilter(9223372036854775806, 6)";
    TimestampsFilter timestampsFilter =
      doTestFilter(filterString, TimestampsFilter.class);
    List<Long> timestamps = timestampsFilter.getTimestamps();
    assertEquals(timestamps.size(), 2);
    assertEquals(timestamps.get(0), new Long(6));

    filterString = "TimestampsFilter()";
    timestampsFilter = doTestFilter(filterString, TimestampsFilter.class);
    timestamps = timestampsFilter.getTimestamps();
    assertEquals(timestamps.size(), 0);

    filterString = "TimestampsFilter(9223372036854775808, 6)";
    try {
      doTestFilter(filterString, ColumnPaginationFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("Long Argument was too large");
    }

    filterString = "TimestampsFilter(-45, 6)";
    try {
      doTestFilter(filterString, ColumnPaginationFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("Timestamp Arguments should not be negative");
    }
  }

  @Test
  public void testRowFilter() throws IOException {
    String filterString = "RowFilter ( =,   'binary:regionse')";
    RowFilter rowFilter =
      doTestFilter(filterString, RowFilter.class);
    assertEquals(CompareFilter.CompareOp.EQUAL, rowFilter.getOperator());
    assertTrue(rowFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) rowFilter.getComparator();
    assertEquals("regionse", new String(binaryComparator.getValue()));
  }

  @Test
  public void testFamilyFilter() throws IOException {
    String filterString = "FamilyFilter(>=, 'binaryprefix:pre')";
    FamilyFilter familyFilter =
      doTestFilter(filterString, FamilyFilter.class);
    assertEquals(CompareFilter.CompareOp.GREATER_OR_EQUAL, familyFilter.getOperator());
    assertTrue(familyFilter.getComparator() instanceof BinaryPrefixComparator);
    BinaryPrefixComparator binaryPrefixComparator =
      (BinaryPrefixComparator) familyFilter.getComparator();
    assertEquals("pre", new String(binaryPrefixComparator.getValue()));
  }

  @Test
  public void testQualifierFilter() throws IOException {
    String filterString = "QualifierFilter(=, 'regexstring:pre*')";
    QualifierFilter qualifierFilter =
      doTestFilter(filterString, QualifierFilter.class);
    assertEquals(CompareFilter.CompareOp.EQUAL, qualifierFilter.getOperator());
    assertTrue(qualifierFilter.getComparator() instanceof RegexStringComparator);
    RegexStringComparator regexStringComparator =
      (RegexStringComparator) qualifierFilter.getComparator();
    assertEquals("pre*", new String(regexStringComparator.getValue()));
  }

  @Test
  public void testValueFilter() throws IOException {
    String filterString = "ValueFilter(!=, 'substring:pre')";
    ValueFilter valueFilter =
      doTestFilter(filterString, ValueFilter.class);
    assertEquals(CompareFilter.CompareOp.NOT_EQUAL, valueFilter.getOperator());
    assertTrue(valueFilter.getComparator() instanceof SubstringComparator);
    SubstringComparator substringComparator =
      (SubstringComparator) valueFilter.getComparator();
    assertEquals("pre", new String(substringComparator.getValue()));
  }

  @Test
  public void testColumnRangeFilter() throws IOException {
    String filterString = "ColumnRangeFilter('abc', true, 'xyz', false)";
    ColumnRangeFilter columnRangeFilter =
      doTestFilter(filterString, ColumnRangeFilter.class);
    assertEquals("abc", new String(columnRangeFilter.getMinColumn()));
    assertEquals("xyz", new String(columnRangeFilter.getMaxColumn()));
    assertTrue(columnRangeFilter.isMinColumnInclusive());
    assertFalse(columnRangeFilter.isMaxColumnInclusive());
  }

  @Test
  public void testDependentColumnFilter() throws IOException {
    String filterString = "DependentColumnFilter('family', 'qualifier', true, =, 'binary:abc')";
    DependentColumnFilter dependentColumnFilter =
      doTestFilter(filterString, DependentColumnFilter.class);
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
    SingleColumnValueFilter singleColumnValueFilter =
      doTestFilter(filterString, SingleColumnValueFilter.class);
    assertEquals("family", new String(singleColumnValueFilter.getFamily()));
    assertEquals("qualifier", new String(singleColumnValueFilter.getQualifier()));
    assertEquals(singleColumnValueFilter.getOperator(), CompareFilter.CompareOp.GREATER_OR_EQUAL);
    assertTrue(singleColumnValueFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) singleColumnValueFilter.getComparator();
    assertEquals(new String(binaryComparator.getValue()), "a");
    assertTrue(singleColumnValueFilter.getFilterIfMissing());
    assertFalse(singleColumnValueFilter.getLatestVersionOnly());


    filterString = "SingleColumnValueFilter ('family', 'qualifier', >, 'binaryprefix:a')";
    singleColumnValueFilter = doTestFilter(filterString, SingleColumnValueFilter.class);
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
    SingleColumnValueExcludeFilter singleColumnValueExcludeFilter =
      doTestFilter(filterString, SingleColumnValueExcludeFilter.class);
    assertEquals(singleColumnValueExcludeFilter.getOperator(), CompareFilter.CompareOp.LESS);
    assertEquals("family", new String(singleColumnValueExcludeFilter.getFamily()));
    assertEquals("qualifier", new String(singleColumnValueExcludeFilter.getQualifier()));
    assertEquals(new String(singleColumnValueExcludeFilter.getComparator().getValue()), "a");
    assertFalse(singleColumnValueExcludeFilter.getFilterIfMissing());
    assertTrue(singleColumnValueExcludeFilter.getLatestVersionOnly());

    filterString = "SingleColumnValueExcludeFilter " +
      "('family', 'qualifier', <=, 'binaryprefix:a', true, false)";
    singleColumnValueExcludeFilter =
      doTestFilter(filterString, SingleColumnValueExcludeFilter.class);
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
    SkipFilter skipFilter =
      doTestFilter(filterString, SkipFilter.class);
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
    WhileMatchFilter whileMatchFilter =
      doTestFilter(filterString, WhileMatchFilter.class);
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
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);
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
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);
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
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);
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
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);
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
    try {
      doTestFilter(filterString, RowFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("Incorrect compare operator >>");
    }
  }

  @Test
  public void testIncorrectComparatorType () throws IOException {
    String  filterString = "RowFilter ('>=' , 'binaryoperator:region')";
    try {
      doTestFilter(filterString, RowFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("Incorrect comparator type: binaryoperator");
    }

    filterString = "RowFilter ('>=' 'regexstring:pre*')";
    try {
      doTestFilter(filterString, RowFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("RegexStringComparator can only be used with EQUAL or NOT_EQUAL");
    }

    filterString = "SingleColumnValueFilter" +
      " ('family', 'qualifier', '>=', 'substring:a', 'true', 'false')')";
    try {
      doTestFilter(filterString, RowFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("SubtringComparator can only be used with EQUAL or NOT_EQUAL");
    }
  }

  @Test
  public void testPrecedence1() throws IOException {
    String filterString = " (PrefixFilter ('realtime')AND  FirstKeyOnlyFilter()" +
      " OR KeyOnlyFilter())";
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);

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
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);
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
    String filterString = "InclusiveStopFilter ('row''3')";
    InclusiveStopFilter inclusiveStopFilter =
      doTestFilter(filterString, InclusiveStopFilter.class);
    byte [] stopRowKey = inclusiveStopFilter.getStopRowKey();
    assertEquals(new String(stopRowKey), "row'3");
  }

  @Test
  public void testUnescapedQuote2 () throws IOException {
    String filterString = "InclusiveStopFilter ('row''3''')";
    InclusiveStopFilter inclusiveStopFilter =
      doTestFilter(filterString, InclusiveStopFilter.class);
    byte [] stopRowKey = inclusiveStopFilter.getStopRowKey();
    assertEquals(new String(stopRowKey), "row'3'");
  }

  @Test
  public void testUnescapedQuote3 () throws IOException {
    String filterString = "	InclusiveStopFilter ('''')";
    InclusiveStopFilter inclusiveStopFilter =
      doTestFilter(filterString, InclusiveStopFilter.class);
    byte [] stopRowKey = inclusiveStopFilter.getStopRowKey();
    assertEquals(new String(stopRowKey), "'");
  }

  @Test
  public void testIncorrectFilterString () throws IOException {
    String filterString = "()";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testCorrectFilterString () throws IOException {
    String filterString = "(FirstKeyOnlyFilter())";
    FirstKeyOnlyFilter firstKeyOnlyFilter =
      doTestFilter(filterString, FirstKeyOnlyFilter.class);
  }

  private <T extends Filter> T doTestFilter(String filterString, Class<T> clazz) throws IOException {
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertEquals(clazz, filter.getClass());
    return clazz.cast(filter);
  }
}

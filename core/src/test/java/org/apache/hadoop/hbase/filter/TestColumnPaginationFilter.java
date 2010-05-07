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
package org.apache.hadoop.hbase.filter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

/**
 * Test for the ColumnPaginationFilter, used mainly to test the successful serialization of the filter.
 * More test functionality can be found within {@link org.apache.hadoop.hbase.filter.TestFilter#testColumnPaginationFilter()}
 */
public class TestColumnPaginationFilter extends TestCase
{
    private static final byte[] ROW = Bytes.toBytes("row_1_test");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("test");
    private static final byte[] VAL_1 = Bytes.toBytes("a");
    private static final byte [] COLUMN_QUALIFIER = Bytes.toBytes("foo");

    private Filter columnPaginationFilter;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        columnPaginationFilter = getColumnPaginationFilter();

    }
    private Filter getColumnPaginationFilter() {
        return new ColumnPaginationFilter(1,0);
    }

    private Filter serializationTest(Filter filter) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(stream);
        filter.write(out);
        out.close();
        byte[] buffer = stream.toByteArray();

        DataInputStream in =
            new DataInputStream(new ByteArrayInputStream(buffer));
        Filter newFilter = new ColumnPaginationFilter();
        newFilter.readFields(in);

        return newFilter;
    }


    /**
     * The more specific functionality tests are contained within the TestFilters class.  This class is mainly for testing
     * serialization
     *
     * @param filter
     * @throws Exception
     */
    private void basicFilterTests(ColumnPaginationFilter filter) throws Exception
    {
      KeyValue kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_1);
      assertTrue("basicFilter1", filter.filterKeyValue(kv) == Filter.ReturnCode.INCLUDE);
    }

    /**
     * Tests serialization
     * @throws Exception
     */
    public void testSerialization() throws Exception {
      Filter newFilter = serializationTest(columnPaginationFilter);
      basicFilterTests((ColumnPaginationFilter)newFilter);
    }


}

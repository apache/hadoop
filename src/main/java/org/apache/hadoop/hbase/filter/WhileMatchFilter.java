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

package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * A wrapper filter that returns true from {@link #filterAllRemaining()} as soon
 * as the wrapped filters {@link Filter#filterRowKey(byte[], int, int)},
 * {@link Filter#filterKeyValue(org.apache.hadoop.hbase.KeyValue)},
 * {@link org.apache.hadoop.hbase.filter.Filter#filterRow()} or
 * {@link org.apache.hadoop.hbase.filter.Filter#filterAllRemaining()} methods
 * returns true.
 */
public class WhileMatchFilter extends FilterBase {
  private boolean filterAllRemaining = false;
  private Filter filter;

  public WhileMatchFilter() {
    super();
  }

  public WhileMatchFilter(Filter filter) {
    this.filter = filter;
  }

  public Filter getFilter() {
    return filter;
  }

  public void reset() {
    this.filter.reset();
  }

  private void changeFAR(boolean value) {
    filterAllRemaining = filterAllRemaining || value;
  }

  public boolean filterAllRemaining() {
    return this.filterAllRemaining || this.filter.filterAllRemaining();
  }

  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    boolean value = filter.filterRowKey(buffer, offset, length);
    changeFAR(value);
    return value;
  }

  public ReturnCode filterKeyValue(KeyValue v) {
    ReturnCode c = filter.filterKeyValue(v);
    changeFAR(c != ReturnCode.INCLUDE);
    return c;
  }

  @Override
  public KeyValue transform(KeyValue v) {
    return filter.transform(v);
  }

  public boolean filterRow() {
    boolean filterRow = this.filter.filterRow();
    changeFAR(filterRow);
    return filterRow;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.filter.getClass().getName());
    this.filter.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    String className = in.readUTF();
    try {
      this.filter = (Filter)(Class.forName(className).newInstance());
      this.filter.readFields(in);
    } catch (InstantiationException e) {
      throw new RuntimeException("Failed deserialize.", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed deserialize.", e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed deserialize.", e);
    }
  }
}

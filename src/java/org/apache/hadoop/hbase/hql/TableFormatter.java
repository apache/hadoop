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
package org.apache.hadoop.hbase.hql;

import java.io.IOException;
import java.io.Writer;

import org.apache.hadoop.hbase.hql.formatter.AsciiTableFormatter;

/**
 * Interface implemented by table formatters outputting select results.
 * Implementations must have a constructor that takes a Writer.
 * 
 * @see AsciiTableFormatter
 */
public interface TableFormatter {
  /**
   * Output header.
   * 
   * @param titles Titles to emit.
   * @throws IOException
   */
  public void header(final String[] titles) throws IOException;

  /**
   * Output footer.
   * 
   * @throws IOException
   */
  public void footer() throws IOException;

  /**
   * Output a row.
   * 
   * @param cells
   * @throws IOException
   */
  public void row(final String[] cells) throws IOException;

  /**
   * @return Output stream being used (This is in interface to enforce fact that
   *         formatters use Writers -- that they operate on character streams
   *         rather than on byte streams).
   */
  public Writer getOut();
}

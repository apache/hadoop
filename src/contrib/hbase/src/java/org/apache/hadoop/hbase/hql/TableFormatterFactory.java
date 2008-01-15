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

import java.io.Writer;
import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.hql.formatter.AsciiTableFormatter;

/**
 * Table formatter. Specify formatter by setting "hbaseshell.formatter" property
 * in <code>hbase-site.xml</code> or by setting system property
 * <code>hbaseshell.formatter</code>. System property setting prevails over
 * all other configurations. Outputs UTF-8 encoded Strings even if original data
 * is binary. On static initialization, changes System.out to be a UTF-8 output
 * stream. .
 * <p>
 * TODO: Mysql has --skip-column-names and --silent which inserts a tab as
 * separator. Also has --html and --xml.
 * <p>
 * To use the html formatter, currently set HBASE_OPTS as in:
 * <code>$ HBASE_OPTS="-Dhbaseshell.formatter=org.apache.hadoop.hbase.shell.formatter.HtmlTableFormatter" ./bin/hbase shell</code>
 * </p>
 */
public class TableFormatterFactory {
  private static final Log LOG = LogFactory.getLog(TableFormatterFactory.class
      .getName());
  private static final String FORMATTER_KEY = "hbaseshell.formatter";
  private final TableFormatter formatter;

  /**
   * Not instantiable
   */
  @SuppressWarnings( { "unchecked", "unused" })
  private TableFormatterFactory() {
    this(null, null);
  }

  @SuppressWarnings("unchecked")
  public TableFormatterFactory(final Writer out, final Configuration c) {
    String className = System.getProperty(FORMATTER_KEY);
    if (className == null) {
      className = c.get(FORMATTER_KEY, AsciiTableFormatter.class.getName());
    }
    LOG.debug("Table formatter class: " + className);
    try {
      Class<TableFormatter> clazz = (Class<TableFormatter>) Class
          .forName(className);
      Constructor<?> constructor = clazz.getConstructor(Writer.class);
      this.formatter = (TableFormatter) constructor.newInstance(out);
    } catch (Exception e) {
      throw new RuntimeException("Failed instantiation of " + className, e);
    }
  }

  /**
   * @return The table formatter instance
   */
  @SuppressWarnings("unchecked")
  public TableFormatter get() {
    return this.formatter;
  }
}

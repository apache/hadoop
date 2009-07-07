/**
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

package org.apache.hadoop.sqoop.orm;

import org.apache.hadoop.sqoop.ImportOptions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Reconciles the table name being imported with the class naming information
 * specified in ImportOptions to determine the actual package and class name
 * to use for a table.
 */
public class TableClassName {

  public static final Log LOG = LogFactory.getLog(TableClassName.class.getName());

  private final ImportOptions options;

  public TableClassName(final ImportOptions opts) {
    if (null == opts) {
      throw new NullPointerException("Cannot instantiate a TableClassName on null options.");
    } else {
      this.options = opts;
    }
  }

  /**
   * Taking into account --class-name and --package-name, return the actual
   * package-part which will be used for a class. The actual table name being
   * generated-for is irrelevant; so not an argument.
   *
   * @return the package where generated ORM classes go. Will be null for top-level.
   */
  public String getPackageForTable() {
    String predefinedClass = options.getClassName();
    if (null != predefinedClass) {
      // if the predefined classname contains a package-part, return that.
      int lastDot = predefinedClass.lastIndexOf('.');
      if (-1 == lastDot) {
        // no package part.
        return null;
      } else {
        // return the string up to but not including the last dot.
        return predefinedClass.substring(0, lastDot);
      }
    } else {
      // If the user has specified a package name, return it.
      // This will be null if the user hasn't specified one -- as we expect.
      return options.getPackageName();
    }
  }

  /**
   * @param tableName the name of the table being imported
   * @return the full name of the class to generate/use to import a table
   */
  public String getClassForTable(String tableName) {
    if (null == tableName) {
      return null;
    }

    String predefinedClass = options.getClassName();
    if (predefinedClass != null) {
      // The user's chosen a specific class name for this job.
      return predefinedClass;
    }

    String packageName = options.getPackageName();
    if (null != packageName) {
      // return packageName.tableName.
      return packageName + "." + tableName;
    }

    // no specific class; no specific package.
    return tableName;
  }

  /**
   * @return just the last spegment of the class name -- all package info stripped. 
   */
  public String getShortClassForTable(String tableName) {
    String fullClass = getClassForTable(tableName);
    if (null == fullClass) {
      return null;
    }

    int lastDot = fullClass.lastIndexOf('.');
    if (-1 == lastDot) {
      return fullClass;
    } else {
      return fullClass.substring(lastDot + 1, fullClass.length());
    }
  }
}

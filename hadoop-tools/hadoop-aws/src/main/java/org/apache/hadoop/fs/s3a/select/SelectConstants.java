/*
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

package org.apache.hadoop.fs.s3a.select;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Options related to S3 Select.
 *
 * These options are set for the entire filesystem unless overridden
 * as an option in the URI
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class SelectConstants {

  public static final String SELECT_UNSUPPORTED = "S3 Select is not supported";

  private SelectConstants() {
  }

  public static final String FS_S3A_SELECT = "fs.s3a.select.";


  /**
   * This is the big SQL expression: {@value}.
   * When used in an open() call, switch to a select operation.
   * This is only used in the open call, never in a filesystem configuration.
   */
  public static final String SELECT_SQL = FS_S3A_SELECT + "sql";

  /**
   * Does the FS Support S3 Select?
   * Value: {@value}.
   */
  public static final String S3_SELECT_CAPABILITY = "fs.s3a.capability.select.sql";

  /**
   * Flag: is S3 select enabled?
   * Value: {@value}.
   */
  public static final String FS_S3A_SELECT_ENABLED = FS_S3A_SELECT
      + "enabled";

  /**
   * Input format for data.
   * Value: {@value}.
   */
  public static final String SELECT_INPUT_FORMAT =
      "fs.s3a.select.input.format";

  /**
   * Output format for data -that is, what the results are generated
   * as.
   * Value: {@value}.
   */
  public static final String SELECT_OUTPUT_FORMAT =
      "fs.s3a.select.output.format";

  /**
   * CSV as an input or output format: {@value}.
   */
  public static final String SELECT_FORMAT_CSV = "csv";

  /**
   * JSON as an input or output format: {@value}.
   */
  public static final String SELECT_FORMAT_JSON = "json";

  /**
   * Should Select errors include the SQL statement?
   * It is easier to debug but a security risk if the exceptions
   * ever get printed/logged and the query contains secrets.
   */
  public static final String SELECT_ERRORS_INCLUDE_SQL =
      FS_S3A_SELECT + "errors.include.sql";

  /**
   * How is the input compressed? This applies to all formats.
   * Value: {@value}.
   */
  public static final String SELECT_INPUT_COMPRESSION = FS_S3A_SELECT
      + "input.compression";

  /**
   * No compression.
   * Value: {@value}.
   */
  public static final String COMPRESSION_OPT_NONE = "none";

  /**
   * Gzipped.
   * Value: {@value}.
   */
  public static final String COMPRESSION_OPT_GZIP = "gzip";

  /**
   * Prefix for all CSV input options.
   * Value: {@value}.
   */
  public static final String FS_S3A_SELECT_INPUT_CSV =
      "fs.s3a.select.input.csv.";

  /**
   * Prefix for all CSV output options.
   * Value: {@value}.
   */
  public static final String FS_S3A_SELECT_OUTPUT_CSV =
      "fs.s3a.select.output.csv.";

  /**
   * String which indicates the row is actually a comment.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_COMMENT_MARKER =
      FS_S3A_SELECT_INPUT_CSV + "comment.marker";

  /**
   * Default marker.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_COMMENT_MARKER_DEFAULT = "#";

  /**
   * Record delimiter. CR, LF, etc.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_RECORD_DELIMITER =
      FS_S3A_SELECT_INPUT_CSV + "record.delimiter";

  /**
   * Default delimiter
   * Value: {@value}.
   */
  public static final String CSV_INPUT_RECORD_DELIMITER_DEFAULT = "\n";

  /**
   * Field delimiter.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_INPUT_FIELD_DELIMITER =
      FS_S3A_SELECT_INPUT_CSV + "field.delimiter";

  /**
   * Default field delimiter.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_FIELD_DELIMITER_DEFAULT = ",";

  /**
   * Quote Character.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_QUOTE_CHARACTER =
      FS_S3A_SELECT_INPUT_CSV + "quote.character";

  /**
   * Default Quote Character.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_QUOTE_CHARACTER_DEFAULT = "\"";

  /**
   * Character to escape quotes.
   * If empty: no escaping.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_QUOTE_ESCAPE_CHARACTER =
      FS_S3A_SELECT_INPUT_CSV + "quote.escape.character";

  /**
   * Default quote escape character.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_QUOTE_ESCAPE_CHARACTER_DEFAULT = "\\";

  /**
   * How should headers be used?
   * Value: {@value}.
   */
  public static final String CSV_INPUT_HEADER =
      FS_S3A_SELECT_INPUT_CSV + "header";

  /**
   * No header: first row is data.
   * Value: {@value}.
   */
  public static final String CSV_HEADER_OPT_NONE = "none";

  /**
   * Ignore the header.
   * Value: {@value}.
   */
  public static final String CSV_HEADER_OPT_IGNORE = "ignore";

  /**
   * Use the header.
   * Value: {@value}.
   */
  public static final String CSV_HEADER_OPT_USE = "use";

  /**
   * Default header mode: {@value}.
   */
  public static final String CSV_INPUT_HEADER_OPT_DEFAULT =
      CSV_HEADER_OPT_IGNORE;

  /**
   * Record delimiter. CR, LF, etc.
   * Value: {@value}.
   */
  public static final String CSV_OUTPUT_RECORD_DELIMITER =
      FS_S3A_SELECT_OUTPUT_CSV + "record.delimiter";

  /**
   * Default delimiter
   * Value: {@value}.
   */
  public static final String CSV_OUTPUT_RECORD_DELIMITER_DEFAULT = "\n";

  /**
   * Field delimiter.
   * Value: {@value}.
   */
  public static final String CSV_OUTPUT_FIELD_DELIMITER =
      FS_S3A_SELECT_OUTPUT_CSV + "field.delimiter";

  /**
   * Default field delimiter.
   * Value: {@value}.
   */
  public static final String CSV_OUTPUT_FIELD_DELIMITER_DEFAULT = ",";

  /**
   * Quote Character.
   * Value: {@value}.
   */
  public static final String CSV_OUTPUT_QUOTE_CHARACTER =
      FS_S3A_SELECT_OUTPUT_CSV + "quote.character";

  /**
   * Default Quote Character.
   * Value: {@value}.
   */
  public static final String CSV_OUTPUT_QUOTE_CHARACTER_DEFAULT = "\"";

  /**
   * Should CSV fields be quoted?
   * One of : ALWAYS, ASNEEDED
   * Value: {@value}.
   */
  public static final String CSV_OUTPUT_QUOTE_FIELDS =
      FS_S3A_SELECT_OUTPUT_CSV + "quote.fields";

  /**
   * Output quotation policy (default): {@value}.
   */
  public static final String CSV_OUTPUT_QUOTE_FIELDS_ALWAYS = "always";

  /**
   * Output quotation policy: {@value}.
   */
  public static final String CSV_OUTPUT_QUOTE_FIELDS_AS_NEEEDED = "asneeded";

  /**
   * Character to escape quotes.
   * If empty: no escaping.
   * Value: {@value}.
   */
  public static final String CSV_OUTPUT_QUOTE_ESCAPE_CHARACTER =
      FS_S3A_SELECT_OUTPUT_CSV + "quote.escape.character";

  /**
   * Default quote escape character.
   * Value: {@value}.
   */
  public static final String CSV_OUTPUT_QUOTE_ESCAPE_CHARACTER_DEFAULT = "";

}

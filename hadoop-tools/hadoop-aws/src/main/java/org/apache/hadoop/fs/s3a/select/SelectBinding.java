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

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;

import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.QuoteFields;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.*;

/**
 * Class to do the S3 select binding and build a select request from the
 * supplied arguments/configuration.
 *
 * This class is intended to be instantiated by the owning S3AFileSystem
 * instance to handle the construction of requests: IO is still done exclusively
 * in the filesystem.
 */
public class SelectBinding {

  static final Logger LOG =
      LoggerFactory.getLogger(SelectBinding.class);

  /** Operations on the store. */
  private final WriteOperationHelper operations;

  /** Is S3 Select enabled? */
  private final boolean enabled;
  private final boolean errorsIncludeSql;

  /**
   * Constructor.
   * @param operations owning FS.
   */
  public SelectBinding(final WriteOperationHelper operations) {
    this.operations = checkNotNull(operations);
    Configuration conf = getConf();
    this.enabled = conf.getBoolean(FS_S3A_SELECT_ENABLED, true);
    this.errorsIncludeSql = conf.getBoolean(SELECT_ERRORS_INCLUDE_SQL, false);
  }

  Configuration getConf() {
    return operations.getConf();
  }

  /**
   * Is the service supported?
   * @return true iff select is enabled.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Build and execute a select request.
   * @param readContext the read context, which includes the source path.
   * @param expression the SQL expression.
   * @param builderOptions query options
   * @param sseKey optional SSE customer key
   * @param objectAttributes object attributes from a HEAD request
   * @return an FSDataInputStream whose wrapped stream is a SelectInputStream
   * @throws IllegalArgumentException argument failure
   * @throws IOException failure building, validating or executing the request.
   * @throws PathIOException source path is a directory.
   */
  @Retries.RetryTranslated
  public FSDataInputStream select(
      final S3AReadOpContext readContext,
      final String expression,
      final Configuration builderOptions,
      final Optional<SSECustomerKey> sseKey,
      final S3ObjectAttributes objectAttributes) throws IOException {

    return new FSDataInputStream(
        executeSelect(readContext,
            objectAttributes,
            builderOptions,
            buildSelectRequest(
                readContext.getPath(),
                expression,
                builderOptions,
                sseKey)));
  }

  /**
   * Build a select request.
   * @param path source path.
   * @param expression the SQL expression.
   * @param builderOptions config to extract other query options from
   * @param sseKey optional SSE customer key
   * @return the request to serve
   * @throws IllegalArgumentException argument failure
   * @throws IOException problem building/validating the request
   */
  public SelectObjectContentRequest buildSelectRequest(
      final Path path,
      final String expression,
      final Configuration builderOptions,
      final Optional<SSECustomerKey> sseKey)
      throws IOException {
    Preconditions.checkState(isEnabled(),
        "S3 Select is not enabled for %s", path);

    SelectObjectContentRequest request = operations.newSelectRequest(path);
    buildRequest(request, expression, builderOptions);
    // optionally set an SSE key in the input
    sseKey.ifPresent(request::withSSECustomerKey);
    return request;
  }

  /**
   * Execute the select request.
   * @param readContext read context
   * @param objectAttributes object attributes from a HEAD request
   * @param builderOptions the options which came in from the openFile builder.
   * @param request the built up select request.
   * @return a SelectInputStream
   * @throws IOException failure
   * @throws PathIOException source path is a directory.
   */
  @Retries.RetryTranslated
  private SelectInputStream executeSelect(
      final S3AReadOpContext readContext,
      final S3ObjectAttributes objectAttributes,
      final Configuration builderOptions,
      final SelectObjectContentRequest request) throws IOException {

    Path path = readContext.getPath();
    if (readContext.getDstFileStatus().isDirectory()) {
      throw new PathIOException(path.toString(),
          "Can't select " + path
          + " because it is a directory");
    }
    boolean sqlInErrors = builderOptions.getBoolean(SELECT_ERRORS_INCLUDE_SQL,
        errorsIncludeSql);
    String expression = request.getExpression();
    final String errorText = sqlInErrors ? expression : "Select";
    if (sqlInErrors) {
      LOG.info("Issuing SQL request {}", expression);
    }
    return new SelectInputStream(readContext,
        objectAttributes,
        operations.select(path, request, errorText));
  }

  /**
   * Build the select request from the configuration built up
   * in {@code S3AFileSystem.openFile(Path)} and the default
   * options in the cluster configuration.
   *
   * Options are picked up in the following order.
   * <ol>
   *   <li> Options in {@code openFileOptions}.</li>
   *   <li> Options in the owning filesystem configuration.</li>
   *   <li>The default values in {@link SelectConstants}</li>
   * </ol>
   *
   * @param request request to build up
   * @param expression SQL expression
   * @param builderOptions the options which came in from the openFile builder.
   * @throws IllegalArgumentException if an option is somehow invalid.
   * @throws IOException if an option is somehow invalid.
   */
  void buildRequest(
      final SelectObjectContentRequest request,
      final String expression,
      final Configuration builderOptions)
      throws IllegalArgumentException, IOException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(expression),
        "No expression provided in parameter " + SELECT_SQL);

    final Configuration ownerConf = operations.getConf();


    String inputFormat = builderOptions.get(SELECT_INPUT_FORMAT,
        SELECT_FORMAT_CSV).toLowerCase(Locale.ENGLISH);
    Preconditions.checkArgument(SELECT_FORMAT_CSV.equals(inputFormat),
        "Unsupported input format %s", inputFormat);
    String outputFormat = builderOptions.get(SELECT_OUTPUT_FORMAT,
        SELECT_FORMAT_CSV)
        .toLowerCase(Locale.ENGLISH);
    Preconditions.checkArgument(SELECT_FORMAT_CSV.equals(outputFormat),
        "Unsupported output format %s", outputFormat);

    request.setExpressionType(ExpressionType.SQL);
    request.setExpression(expandBackslashChars(expression));

    InputSerialization inputSerialization = buildCsvInputRequest(ownerConf,
        builderOptions);
    String compression = opt(builderOptions,
        ownerConf,
        SELECT_INPUT_COMPRESSION,
        COMPRESSION_OPT_NONE,
        true).toUpperCase(Locale.ENGLISH);
    if (isNotEmpty(compression)) {
      inputSerialization.setCompressionType(compression);
    }
    request.setInputSerialization(inputSerialization);

    request.setOutputSerialization(buildCSVOutput(ownerConf, builderOptions));

  }

  /**
   * Build the CSV input request.
   * @param ownerConf FS owner configuration
   * @param builderOptions options on the specific request
   * @return the constructed request
   * @throws IllegalArgumentException argument failure
   * @throws IOException validation failure
   */
  public InputSerialization buildCsvInputRequest(
      final Configuration ownerConf,
      final Configuration builderOptions)
      throws IllegalArgumentException, IOException {

    String headerInfo = opt(builderOptions,
        ownerConf,
        CSV_INPUT_HEADER,
        CSV_INPUT_HEADER_OPT_DEFAULT,
        true).toUpperCase(Locale.ENGLISH);
    String commentMarker = xopt(builderOptions,
        ownerConf,
        CSV_INPUT_COMMENT_MARKER,
        CSV_INPUT_COMMENT_MARKER_DEFAULT);
    String fieldDelimiter = xopt(builderOptions,
        ownerConf,
        CSV_INPUT_INPUT_FIELD_DELIMITER,
        CSV_INPUT_FIELD_DELIMITER_DEFAULT);
    String recordDelimiter = xopt(builderOptions,
        ownerConf,
        CSV_INPUT_RECORD_DELIMITER,
        CSV_INPUT_RECORD_DELIMITER_DEFAULT);
    String quoteCharacter = xopt(builderOptions,
        ownerConf,
        CSV_INPUT_QUOTE_CHARACTER,
        CSV_INPUT_QUOTE_CHARACTER_DEFAULT);
    String quoteEscapeCharacter = xopt(builderOptions,
        ownerConf,
        CSV_INPUT_QUOTE_ESCAPE_CHARACTER,
        CSV_INPUT_QUOTE_ESCAPE_CHARACTER_DEFAULT);

    // CSV input
    CSVInput csv = new CSVInput();
    csv.setFieldDelimiter(fieldDelimiter);
    csv.setRecordDelimiter(recordDelimiter);
    csv.setComments(commentMarker);
    csv.setQuoteCharacter(quoteCharacter);
    if (StringUtils.isNotEmpty(quoteEscapeCharacter)) {
      csv.setQuoteEscapeCharacter(quoteEscapeCharacter);
    }
    csv.setFileHeaderInfo(headerInfo);

    InputSerialization inputSerialization = new InputSerialization();
    inputSerialization.setCsv(csv);

    return inputSerialization;

  }

  /**
   * Build CSV output for a request.
   * @param ownerConf FS owner configuration
   * @param builderOptions options on the specific request
   * @return the constructed request
   * @throws IllegalArgumentException argument failure
   * @throws IOException validation failure
   */
  public OutputSerialization buildCSVOutput(
      final Configuration ownerConf,
      final Configuration builderOptions)
      throws IllegalArgumentException, IOException {
    String fieldDelimiter = xopt(builderOptions,
        ownerConf,
        CSV_OUTPUT_FIELD_DELIMITER,
        CSV_OUTPUT_FIELD_DELIMITER_DEFAULT);
    String recordDelimiter = xopt(builderOptions,
        ownerConf,
        CSV_OUTPUT_RECORD_DELIMITER,
        CSV_OUTPUT_RECORD_DELIMITER_DEFAULT);
    String quoteCharacter = xopt(builderOptions,
        ownerConf,
        CSV_OUTPUT_QUOTE_CHARACTER,
        CSV_OUTPUT_QUOTE_CHARACTER_DEFAULT);
    String quoteEscapeCharacter = xopt(builderOptions,
        ownerConf,
        CSV_OUTPUT_QUOTE_ESCAPE_CHARACTER,
        CSV_OUTPUT_QUOTE_ESCAPE_CHARACTER_DEFAULT);
    String quoteFields = xopt(builderOptions,
        ownerConf,
        CSV_OUTPUT_QUOTE_FIELDS,
        CSV_OUTPUT_QUOTE_FIELDS_ALWAYS).toUpperCase(Locale.ENGLISH);

    // output is CSV, always
    OutputSerialization outputSerialization
        = new OutputSerialization();
    CSVOutput csvOut = new CSVOutput();
    csvOut.setQuoteCharacter(quoteCharacter);
    csvOut.setQuoteFields(
        QuoteFields.fromValue(quoteFields));
    csvOut.setFieldDelimiter(fieldDelimiter);
    csvOut.setRecordDelimiter(recordDelimiter);
    if (!quoteEscapeCharacter.isEmpty()) {
      csvOut.setQuoteEscapeCharacter(quoteEscapeCharacter);
    }

    outputSerialization.setCsv(csvOut);
    return outputSerialization;
  }

  /**
   * Stringify the given SelectObjectContentRequest, as its
   * toString() operator doesn't.
   * @param request request to convert to a string
   * @return a string to print. Does not contain secrets.
   */
  public static String toString(final SelectObjectContentRequest request) {
    StringBuilder sb = new StringBuilder();
    sb.append("SelectObjectContentRequest{")
        .append("bucket name=").append(request.getBucketName())
        .append("; key=").append(request.getKey())
        .append("; expressionType=").append(request.getExpressionType())
        .append("; expression=").append(request.getExpression());
    InputSerialization input = request.getInputSerialization();
    if (input != null) {
      sb.append("; Input")
          .append(input.toString());
    } else {
      sb.append("; Input Serialization: none");
    }
    OutputSerialization out = request.getOutputSerialization();
    if (out != null) {
      sb.append("; Output")
          .append(out.toString());
    } else {
      sb.append("; Output Serialization: none");
    }
    return sb.append("}").toString();
  }

  /**
   * Resolve an option.
   * @param builderOptions the options which came in from the openFile builder.
   * @param fsConf configuration of the owning FS.
   * @param base base option (no s3a: prefix)
   * @param defVal default value. Must not be null.
   * @param trim should the result be trimmed.
   * @return the possibly trimmed value.
   */
  static String opt(Configuration builderOptions,
      Configuration fsConf,
      String base,
      String defVal,
      boolean trim) {
    String r = builderOptions.get(base, fsConf.get(base, defVal));
    return trim ? r.trim() : r;
  }

  /**
   * Get an option with backslash arguments transformed.
   * These are not trimmed, so whitespace is significant.
   * @param selectOpts options in the select call
   * @param fsConf filesystem conf
   * @param base base option name
   * @param defVal default value
   * @return the transformed value
   */
  static String xopt(Configuration selectOpts,
      Configuration fsConf,
      String base,
      String defVal) {
    return expandBackslashChars(
        opt(selectOpts, fsConf, base, defVal, false));
  }

  /**
   * Perform escaping.
   * @param src source string.
   * @return the replaced value
   */
  static String expandBackslashChars(String src) {
    return src.replace("\\n", "\n")
        .replace("\\\"", "\"")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
        .replace("\\\"", "\"")
        // backslash substitution must come last
        .replace("\\\\", "\\");
  }

}

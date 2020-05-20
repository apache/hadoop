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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.FutureIOSupport;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.OperationDuration;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.*;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.*;

/**
 * This is a CLI tool for the select operation, which is available
 * through the S3Guard command.
 *
 * Usage:
 * <pre>
 *   hadoop s3guard select [options] Path Statement
 * </pre>
 */
public class SelectTool extends S3GuardTool {

  private static final Logger LOG =
      LoggerFactory.getLogger(SelectTool.class);

  public static final String NAME = "select";

  public static final String PURPOSE = "make an S3 Select call";

  private static final String USAGE = NAME
      + " [OPTIONS]"
      + " [-limit rows]"
      + " [-header (use|none|ignore)]"
      + " [-out path]"
      + " [-expected rows]"
      + " [-compression (gzip|bzip2|none)]"
      + " [-inputformat csv]"
      + " [-outputformat csv]"
      + " <PATH> <SELECT QUERY>\n"
      + "\t" + PURPOSE + "\n\n";

  public static final String OPT_COMPRESSION = "compression";

  public static final String OPT_EXPECTED = "expected";

  public static final String OPT_HEADER = "header";

  public static final String OPT_INPUTFORMAT = "inputformat";

  public static final String OPT_LIMIT = "limit";

  public static final String OPT_OUTPUT = "out";

  public static final String OPT_OUTPUTFORMAT = "inputformat";

  static final String TOO_FEW_ARGUMENTS = "Too few arguments";

  static final String SELECT_IS_DISABLED = "S3 Select is disabled";

  private OperationDuration selectDuration;

  private long bytesRead;

  private long linesRead;

  public SelectTool(Configuration conf) {
    super(conf);
    // read capacity.
    getCommandFormat().addOptionWithValue(OPT_COMPRESSION);
    getCommandFormat().addOptionWithValue(OPT_EXPECTED);
    getCommandFormat().addOptionWithValue(OPT_HEADER);
    getCommandFormat().addOptionWithValue(OPT_INPUTFORMAT);
    getCommandFormat().addOptionWithValue(OPT_LIMIT);
    getCommandFormat().addOptionWithValue(OPT_OUTPUT);
    getCommandFormat().addOptionWithValue(OPT_OUTPUTFORMAT);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getUsage() {
    return USAGE;
  }

  public OperationDuration getSelectDuration() {
    return selectDuration;
  }

  public long getBytesRead() {
    return bytesRead;
  }

  /**
   * Number of lines read, when printing to the console.
   * @return line count. 0 if writing direct to a file.
   */
  public long getLinesRead() {
    return linesRead;
  }

  private int parseNaturalInt(String option, String value) {
    try {
      int r = Integer.parseInt(value);
      if (r < 0) {
        throw invalidArgs("Negative value for option %s : %s", option, value);
      }
      return r;
    } catch (NumberFormatException e) {
      throw invalidArgs("Invalid number for option %s : %s", option, value);
    }
  }

  private Optional<String> getOptValue(String key) {
    String value = getCommandFormat().getOptValue(key);
    return isNotEmpty(value) ? Optional.of(value): Optional.empty();
  }

  private Optional<Integer> getIntValue(String key) {
    Optional<String> v = getOptValue(key);
    return v.map(i -> parseNaturalInt(key, i));
  }

  /**
   * Execute the select operation.
   * @param args argument list
   * @param out output stream
   * @return an exit code
   * @throws IOException IO failure
   * @throws ExitUtil.ExitException managed failure
   */
  public int run(String[] args, PrintStream out)
      throws IOException, ExitUtil.ExitException {
    final List<String> parsedArgs;
    try {
      parsedArgs = parseArgs(args);
    } catch (CommandFormat.UnknownOptionException e) {
      errorln(getUsage());
      throw new ExitUtil.ExitException(EXIT_USAGE, e.getMessage(), e);
    }
    if (parsedArgs.size() < 2) {
      errorln(getUsage());
      throw new ExitUtil.ExitException(EXIT_USAGE, TOO_FEW_ARGUMENTS);
    }

    // read mandatory arguments
    final String file = parsedArgs.get(0);
    final Path path = new Path(file);

    String expression = parsedArgs.get(1);

    println(out, "selecting file %s with query %s",
        path, expression);

    // and the optional arguments to adjust the configuration.
    final Optional<String> header = getOptValue(OPT_HEADER);
    header.ifPresent(h -> println(out, "Using header option %s", h));

    Path destPath = getOptValue(OPT_OUTPUT).map(
        output -> {
          println(out, "Saving output to %s", output);
          return new Path(output);
        }).orElse(null);
    final boolean toConsole = destPath == null;

    // expected lines are only checked if empty
    final Optional<Integer> expectedLines = toConsole
        ? getIntValue(OPT_EXPECTED)
        : Optional.empty();

    final Optional<Integer> limit = getIntValue(OPT_LIMIT);
    if (limit.isPresent()) {
      final int l = limit.get();
      println(out, "Using line limit %s", l);
      if (expression.toLowerCase(Locale.ENGLISH).contains(" limit ")) {
        println(out, "line limit already specified in SELECT expression");
      } else {
        expression = expression + " LIMIT " + l;
      }
    }

    // now bind to the filesystem.
    FileSystem fs = bindFilesystem(path.getFileSystem(getConf()));

    if (!fs.hasPathCapability(path, S3_SELECT_CAPABILITY)) {
      // capability disabled
      throw new ExitUtil.ExitException(EXIT_SERVICE_UNAVAILABLE,
          SELECT_IS_DISABLED + " for " + file);
    }
    linesRead = 0;

    selectDuration = new OperationDuration();

    // open and scan the stream.
    final FutureDataInputStreamBuilder builder = fs.openFile(path)
        .must(SELECT_SQL, expression);

    header.ifPresent(h -> builder.must(CSV_INPUT_HEADER, h));

    getOptValue(OPT_COMPRESSION).ifPresent(compression ->
        builder.must(SELECT_INPUT_COMPRESSION,
          compression.toUpperCase(Locale.ENGLISH)));

    getOptValue(OPT_INPUTFORMAT).ifPresent(opt -> {
      if (!"csv".equalsIgnoreCase(opt)) {
        throw invalidArgs("Unsupported input format %s", opt);
      }
    });
    getOptValue(OPT_OUTPUTFORMAT).ifPresent(opt -> {
      if (!"csv".equalsIgnoreCase(opt)) {
        throw invalidArgs("Unsupported output format %s", opt);
      }
    });
    // turn on SQL error reporting.
    builder.opt(SELECT_ERRORS_INCLUDE_SQL, true);

    FSDataInputStream stream;
    try(DurationInfo ignored =
            new DurationInfo(LOG, "Selecting stream")) {
      stream = FutureIOSupport.awaitFuture(builder.build());
    } catch (FileNotFoundException e) {
      // the source file is missing.
      throw storeNotFound(e);
    }
    try {
      if (toConsole) {
        // logging to console
        bytesRead = 0;
        @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
        Scanner scanner =
            new Scanner(
                new BufferedReader(
                    new InputStreamReader(stream, StandardCharsets.UTF_8)));
        scanner.useDelimiter("\n");
        while (scanner.hasNextLine()) {
          linesRead++;
          String l = scanner.nextLine();
          bytesRead += l.length() + 1;
          println(out, "%s", l);
        }
      } else {
        // straight dump of whole file; no line counting
        FileSystem destFS = destPath.getFileSystem(getConf());
        try(DurationInfo ignored =
                new DurationInfo(LOG, "Copying File");
            OutputStream destStream = destFS.createFile(destPath)
                .overwrite(true)
                .build()) {
          bytesRead = IOUtils.copy(stream, destStream);
        }
      }

      // close the stream.
      // this will take time if there's a lot of data remaining
      try (DurationInfo ignored =
               new DurationInfo(LOG, "Closing stream")) {
        stream.close();
      }

      // generate a meaningful result depending on the operation
      String result = toConsole
          ? String.format("%s lines", linesRead)
          : String.format("%s bytes", bytesRead);

      // print some statistics
      selectDuration.finished();
      println(out, "Read %s in time %s",
          result, selectDuration.getDurationString());

      println(out, "Bytes Read: %,d bytes", bytesRead);

      println(out, "Bandwidth: %,.1f MiB/s",
          bandwidthMBs(bytesRead, selectDuration.value()));

    } finally {
      cleanupWithLogger(LOG, stream);
    }

    LOG.debug("Statistics {}", stream);

    expectedLines.ifPresent(l -> {
      if (l != linesRead) {
        throw exitException(EXIT_FAIL,
            "Expected %d rows but the operation returned %d",
            l, linesRead);
      }
    });
    out.flush();
    return EXIT_SUCCESS;
  }

  /**
   * Work out the bandwidth in MB/s.
   * @param bytes bytes
   * @param durationMillisNS duration in nanos
   * @return the number of megabytes/second of the recorded operation
   */
  public static double bandwidthMBs(long bytes, long durationMillisNS) {
    return durationMillisNS > 0
        ? (bytes / 1048576.0 * 1000 / durationMillisNS)
        : 0;
  }
}

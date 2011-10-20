
/*
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.ByteBloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Implements pretty-printing functionality for {@link HFile}s.
 */
public class HFilePrettyPrinter {

  private static final Log LOG = LogFactory.getLog(HFilePrettyPrinter.class);

  private Options options = new Options();

  private boolean verbose;
  private boolean printValue;
  private boolean printKey;
  private boolean shouldPrintMeta;
  private boolean printBlocks;
  private boolean checkRow;
  private boolean checkFamily;

  private Configuration conf;

  private List<Path> files = new ArrayList<Path>();
  private int count;

  private static final String FOUR_SPACES = "    ";

  public HFilePrettyPrinter() {
    options.addOption("v", "verbose", false,
        "Verbose output; emits file and meta data delimiters");
    options.addOption("p", "printkv", false, "Print key/value pairs");
    options.addOption("e", "printkey", false, "Print keys");
    options.addOption("m", "printmeta", false, "Print meta data of file");
    options.addOption("b", "printblocks", false, "Print block index meta data");
    options.addOption("k", "checkrow", false,
        "Enable row order check; looks for out-of-order keys");
    options.addOption("a", "checkfamily", false, "Enable family check");
    options.addOption("f", "file", true,
        "File to scan. Pass full-path; e.g. hdfs://a:9000/hbase/.META./12/34");
    options.addOption("r", "region", true,
        "Region to scan. Pass region name; e.g. '.META.,,1'");
  }

  public boolean parseOptions(String args[]) throws ParseException,
      IOException {
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("HFile", options, true);
      return false;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    verbose = cmd.hasOption("v");
    printValue = cmd.hasOption("p");
    printKey = cmd.hasOption("e") || printValue;
    shouldPrintMeta = cmd.hasOption("m");
    printBlocks = cmd.hasOption("b");
    checkRow = cmd.hasOption("k");
    checkFamily = cmd.hasOption("a");

    if (cmd.hasOption("f")) {
      files.add(new Path(cmd.getOptionValue("f")));
    }

    if (cmd.hasOption("r")) {
      String regionName = cmd.getOptionValue("r");
      byte[] rn = Bytes.toBytes(regionName);
      byte[][] hri = HRegionInfo.parseRegionName(rn);
      Path rootDir = FSUtils.getRootDir(conf);
      Path tableDir = new Path(rootDir, Bytes.toString(hri[0]));
      String enc = HRegionInfo.encodeRegionName(rn);
      Path regionDir = new Path(tableDir, enc);
      if (verbose)
        System.out.println("region dir -> " + regionDir);
      List<Path> regionFiles = HFile.getStoreFiles(FileSystem.get(conf),
          regionDir);
      if (verbose)
        System.out.println("Number of region files found -> "
            + regionFiles.size());
      if (verbose) {
        int i = 1;
        for (Path p : regionFiles) {
          if (verbose)
            System.out.println("Found file[" + i++ + "] -> " + p);
        }
      }
      files.addAll(regionFiles);
    }

    return true;
  }

  /**
   * Runs the command-line pretty-printer, and returns the desired command
   * exit code (zero for success, non-zero for failure).
   */
  public int run(String[] args) {
    conf = HBaseConfiguration.create();
    conf.set("fs.defaultFS",
        conf.get(org.apache.hadoop.hbase.HConstants.HBASE_DIR));
    conf.set("fs.default.name",
        conf.get(org.apache.hadoop.hbase.HConstants.HBASE_DIR));
    try {
      if (!parseOptions(args))
        return 1;
    } catch (IOException ex) {
      LOG.error("Error parsing command-line options", ex);
      return 1;
    } catch (ParseException ex) {
      LOG.error("Error parsing command-line options", ex);
      return 1;
    }

    // iterate over all files found
    for (Path fileName : files) {
      try {
        processFile(fileName);
      } catch (IOException ex) {
        LOG.error("Error reading " + fileName, ex);
      }
    }

    if (verbose || printKey) {
      System.out.println("Scanned kv count -> " + count);
    }

    return 0;
  }

  private void processFile(Path file) throws IOException {
    if (verbose)
      System.out.println("Scanning -> " + file);
    FileSystem fs = file.getFileSystem(conf);
    if (!fs.exists(file)) {
      System.err.println("ERROR, file doesnt exist: " + file);
    }

    HFile.Reader reader = HFile.createReader(fs, file, new CacheConfig(conf));

    Map<byte[], byte[]> fileInfo = reader.loadFileInfo();

    if (verbose || printKey || checkRow || checkFamily) {

      // scan over file and read key/value's and check if requested
      HFileScanner scanner = reader.getScanner(false, false, false);
      scanner.seekTo();
      scanKeysValues(file, scanner);
    }

    // print meta data
    if (shouldPrintMeta) {
      printMeta(reader, fileInfo);
    }

    if (printBlocks) {
      System.out.println("Block Index:");
      System.out.println(reader.getDataBlockIndexReader());
    }

    reader.close();
  }

  private void scanKeysValues(Path file, HFileScanner scanner)
      throws IOException {
    KeyValue pkv = null;
    do {
      KeyValue kv = scanner.getKeyValue();
      // dump key value
      if (printKey) {
        System.out.print("K: " + kv);
        if (printValue) {
          System.out.print(" V: " + Bytes.toStringBinary(kv.getValue()));
        }
        System.out.println();
      }
      // check if rows are in order
      if (checkRow && pkv != null) {
        if (Bytes.compareTo(pkv.getRow(), kv.getRow()) > 0) {
          System.err.println("WARNING, previous row is greater then"
              + " current row\n\tfilename -> " + file + "\n\tprevious -> "
              + Bytes.toStringBinary(pkv.getKey()) + "\n\tcurrent  -> "
              + Bytes.toStringBinary(kv.getKey()));
        }
      }
      // check if families are consistent
      if (checkFamily) {
        String fam = Bytes.toString(kv.getFamily());
        if (!file.toString().contains(fam)) {
          System.err.println("WARNING, filename does not match kv family,"
              + "\n\tfilename -> " + file + "\n\tkeyvalue -> "
              + Bytes.toStringBinary(kv.getKey()));
        }
        if (pkv != null
            && !Bytes.equals(pkv.getFamily(), kv.getFamily())) {
          System.err.println("WARNING, previous kv has different family"
              + " compared to current key\n\tfilename -> " + file
              + "\n\tprevious -> " + Bytes.toStringBinary(pkv.getKey())
              + "\n\tcurrent  -> " + Bytes.toStringBinary(kv.getKey()));
        }
      }
      pkv = kv;
      ++count;
    } while (scanner.next());
  }

  /**
   * Format a string of the form "k1=v1, k2=v2, ..." into separate lines
   * with a four-space indentation.
   */
  private static String asSeparateLines(String keyValueStr) {
    return keyValueStr.replaceAll(", ([a-zA-Z]+=)",
                                  ",\n" + FOUR_SPACES + "$1");
  }

  private void printMeta(HFile.Reader reader, Map<byte[], byte[]> fileInfo)
      throws IOException {
    System.out.println("Block index size as per heapsize: "
        + reader.indexSize());
    System.out.println(asSeparateLines(reader.toString()));
    System.out.println("Trailer:\n    "
        + asSeparateLines(reader.getTrailer().toString()));
    System.out.println("Fileinfo:");
    for (Map.Entry<byte[], byte[]> e : fileInfo.entrySet()) {
      System.out.print(FOUR_SPACES + Bytes.toString(e.getKey()) + " = ");
      if (Bytes.compareTo(e.getKey(), Bytes.toBytes("MAX_SEQ_ID_KEY")) == 0) {
        long seqid = Bytes.toLong(e.getValue());
        System.out.println(seqid);
      } else if (Bytes.compareTo(e.getKey(), Bytes.toBytes("TIMERANGE")) == 0) {
        TimeRangeTracker timeRangeTracker = new TimeRangeTracker();
        Writables.copyWritable(e.getValue(), timeRangeTracker);
        System.out.println(timeRangeTracker.getMinimumTimestamp() + "...."
            + timeRangeTracker.getMaximumTimestamp());
      } else if (Bytes.compareTo(e.getKey(), FileInfo.AVG_KEY_LEN) == 0
          || Bytes.compareTo(e.getKey(), FileInfo.AVG_VALUE_LEN) == 0) {
        System.out.println(Bytes.toInt(e.getValue()));
      } else {
        System.out.println(Bytes.toStringBinary(e.getValue()));
      }
    }

    System.out.println("Mid-key: " + Bytes.toStringBinary(reader.midkey()));

    // Printing bloom information
    DataInput bloomMeta = reader.getBloomFilterMetadata();
    BloomFilter bloomFilter = null;
    if (bloomMeta != null)
      bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);

    System.out.println("Bloom filter:");
    if (bloomFilter != null) {
      System.out.println(FOUR_SPACES + bloomFilter.toString().replaceAll(
          ByteBloomFilter.STATS_RECORD_SEP, "\n" + FOUR_SPACES));
    } else {
      System.out.println(FOUR_SPACES + "Not present");
    }
  }

}

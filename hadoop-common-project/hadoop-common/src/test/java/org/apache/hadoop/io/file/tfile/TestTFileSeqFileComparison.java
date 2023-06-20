/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.StringTokenizer;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;

public class TestTFileSeqFileComparison {
  MyOptions options;

  private FileSystem fs;
  private Configuration conf;
  private long startTimeEpoch;
  private long finishTimeEpoch;
  private DateFormat formatter;
  byte[][] dictionary;

  @Before
  public void setUp() throws IOException {
    if (options == null) {
      options = new MyOptions(new String[0]);
    }

    conf = new Configuration();
    conf.setInt("tfile.fs.input.buffer.size", options.fsInputBufferSize);
    conf.setInt("tfile.fs.output.buffer.size", options.fsOutputBufferSize);
    Path path = new Path(options.rootDir);
    fs = path.getFileSystem(conf);
    formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    setUpDictionary();
  }

  private void setUpDictionary() {
    Random rng = new Random();
    dictionary = new byte[options.dictSize][];
    for (int i = 0; i < options.dictSize; ++i) {
      int len =
          rng.nextInt(options.maxWordLen - options.minWordLen)
              + options.minWordLen;
      dictionary[i] = new byte[len];
      rng.nextBytes(dictionary[i]);
    }
  }

  @After
  public void tearDown() throws IOException {
    // do nothing
  }

  public void startTime() throws IOException {
    startTimeEpoch = Time.now();
    System.out.println(formatTime() + " Started timing.");
  }

  public void stopTime() throws IOException {
    finishTimeEpoch = Time.now();
    System.out.println(formatTime() + " Stopped timing.");
  }

  public long getIntervalMillis() throws IOException {
    return finishTimeEpoch - startTimeEpoch;
  }

  public void printlnWithTimestamp(String message) throws IOException {
    System.out.println(formatTime() + "  " + message);
  }

  /*
   * Format millis into minutes and seconds.
   */
  public String formatTime(long milis) {
    return formatter.format(milis);
  }

  public String formatTime() {
    return formatTime(Time.now());
  }

  private interface KVAppendable {
    public void append(BytesWritable key, BytesWritable value)
        throws IOException;

    public void close() throws IOException;
  }

  private interface KVReadable {
    public byte[] getKey();

    public byte[] getValue();

    public int getKeyLength();

    public int getValueLength();

    public boolean next() throws IOException;

    public void close() throws IOException;
  }

  static class TFileAppendable implements KVAppendable {
    private FSDataOutputStream fsdos;
    private TFile.Writer writer;

    public TFileAppendable(FileSystem fs, Path path, String compress,
        int minBlkSize, int osBufferSize, Configuration conf)
        throws IOException {
      this.fsdos = fs.create(path, true, osBufferSize);
      this.writer = new TFile.Writer(fsdos, minBlkSize, compress, null, conf);
    }

    @Override
    public void append(BytesWritable key, BytesWritable value)
        throws IOException {
      writer.append(key.getBytes(), 0, key.getLength(), value.getBytes(), 0,
          value.getLength());
    }

    @Override
    public void close() throws IOException {
      writer.close();
      fsdos.close();
    }
  }

  static class TFileReadable implements KVReadable {
    private FSDataInputStream fsdis;
    private TFile.Reader reader;
    private TFile.Reader.Scanner scanner;
    private byte[] keyBuffer;
    private int keyLength;
    private byte[] valueBuffer;
    private int valueLength;

    public TFileReadable(FileSystem fs, Path path, int osBufferSize,
        Configuration conf) throws IOException {
      this.fsdis = fs.open(path, osBufferSize);
      this.reader =
          new TFile.Reader(fsdis, fs.getFileStatus(path).getLen(), conf);
      this.scanner = reader.createScanner();
      keyBuffer = new byte[32];
      valueBuffer = new byte[32];
    }

    private void checkKeyBuffer(int size) {
      if (size <= keyBuffer.length) {
        return;
      }
      keyBuffer =
          new byte[Math.max(2 * keyBuffer.length, 2 * size - keyBuffer.length)];
    }

    private void checkValueBuffer(int size) {
      if (size <= valueBuffer.length) {
        return;
      }
      valueBuffer =
          new byte[Math.max(2 * valueBuffer.length, 2 * size
              - valueBuffer.length)];
    }

    @Override
    public byte[] getKey() {
      return keyBuffer;
    }

    @Override
    public int getKeyLength() {
      return keyLength;
    }

    @Override
    public byte[] getValue() {
      return valueBuffer;
    }

    @Override
    public int getValueLength() {
      return valueLength;
    }

    @Override
    public boolean next() throws IOException {
      if (scanner.atEnd()) return false;
      Entry entry = scanner.entry();
      keyLength = entry.getKeyLength();
      checkKeyBuffer(keyLength);
      entry.getKey(keyBuffer);
      valueLength = entry.getValueLength();
      checkValueBuffer(valueLength);
      entry.getValue(valueBuffer);
      scanner.advance();
      return true;
    }

    @Override
    public void close() throws IOException {
      scanner.close();
      reader.close();
      fsdis.close();
    }
  }

  static class SeqFileAppendable implements KVAppendable {
    private FSDataOutputStream fsdos;
    private SequenceFile.Writer writer;

    public SeqFileAppendable(FileSystem fs, Path path, int osBufferSize,
        String compress, int minBlkSize) throws IOException {
      Configuration conf = new Configuration();

      CompressionCodec codec = null;
      if ("lzo".equals(compress)) {
        codec = Compression.Algorithm.LZO.getCodec();
      }
      else if ("gz".equals(compress)) {
        codec = Compression.Algorithm.GZ.getCodec();
      }
      else if (!"none".equals(compress))
        throw new IOException("Codec not supported.");

      this.fsdos = fs.create(path, true, osBufferSize);

      if (!"none".equals(compress)) {
        writer =
            SequenceFile.createWriter(conf, fsdos, BytesWritable.class,
                BytesWritable.class, SequenceFile.CompressionType.BLOCK, codec);
      }
      else {
        writer =
            SequenceFile.createWriter(conf, fsdos, BytesWritable.class,
                BytesWritable.class, SequenceFile.CompressionType.NONE, null);
      }
    }

    @Override
    public void append(BytesWritable key, BytesWritable value)
        throws IOException {
      writer.append(key, value);
    }

    @Override
    public void close() throws IOException {
      writer.close();
      fsdos.close();
    }
  }

  static class SeqFileReadable implements KVReadable {
    private SequenceFile.Reader reader;
    private BytesWritable key;
    private BytesWritable value;

    public SeqFileReadable(FileSystem fs, Path path, int osBufferSize)
        throws IOException {
      Configuration conf = new Configuration();
      conf.setInt("io.file.buffer.size", osBufferSize);
      reader = new SequenceFile.Reader(fs, path, conf);
      key = new BytesWritable();
      value = new BytesWritable();
    }

    @Override
    public byte[] getKey() {
      return key.getBytes();
    }

    @Override
    public int getKeyLength() {
      return key.getLength();
    }

    @Override
    public byte[] getValue() {
      return value.getBytes();
    }

    @Override
    public int getValueLength() {
      return value.getLength();
    }

    @Override
    public boolean next() throws IOException {
      return reader.next(key, value);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

  private void reportStats(Path path, long totalBytes) throws IOException {
    long duration = getIntervalMillis();
    long fsize = fs.getFileStatus(path).getLen();
    printlnWithTimestamp(String.format(
        "Duration: %dms...total size: %.2fMB...raw thrpt: %.2fMB/s", duration,
        (double) totalBytes / 1024 / 1024, (double) totalBytes / duration
            * 1000 / 1024 / 1024));
    printlnWithTimestamp(String.format(
        "Compressed size: %.2fMB...compressed thrpt: %.2fMB/s.",
        (double) fsize / 1024 / 1024, (double) fsize / duration * 1000 / 1024
            / 1024));
  }

  private void fillBuffer(Random rng, BytesWritable bw, byte[] tmp, int len) {
    int n = 0;
    while (n < len) {
      byte[] word = dictionary[rng.nextInt(dictionary.length)];
      int l = Math.min(word.length, len - n);
      System.arraycopy(word, 0, tmp, n, l);
      n += l;
    }
    bw.set(tmp, 0, len);
  }

  private void timeWrite(Path path, KVAppendable appendable, int baseKlen,
      int baseVlen, long fileSize) throws IOException {
    int maxKlen = baseKlen * 2;
    int maxVlen = baseVlen * 2;
    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    byte[] keyBuffer = new byte[maxKlen];
    byte[] valueBuffer = new byte[maxVlen];
    Random rng = new Random(options.seed);
    long totalBytes = 0;
    printlnWithTimestamp("Start writing: " + path.getName() + "...");
    startTime();

    for (long i = 0; true; ++i) {
      if (i % 1000 == 0) { // test the size for every 1000 rows.
        if (fs.getFileStatus(path).getLen() >= fileSize) {
          break;
        }
      }
      int klen = rng.nextInt(baseKlen) + baseKlen;
      int vlen = rng.nextInt(baseVlen) + baseVlen;
      fillBuffer(rng, key, keyBuffer, klen);
      fillBuffer(rng, value, valueBuffer, vlen);
      key.set(keyBuffer, 0, klen);
      value.set(valueBuffer, 0, vlen);
      appendable.append(key, value);
      totalBytes += klen;
      totalBytes += vlen;
    }
    stopTime();
    appendable.close();
    reportStats(path, totalBytes);
  }

  private void timeRead(Path path, KVReadable readable) throws IOException {
    printlnWithTimestamp("Start reading: " + path.getName() + "...");
    long totalBytes = 0;
    startTime();
    for (; readable.next();) {
      totalBytes += readable.getKeyLength();
      totalBytes += readable.getValueLength();
    }
    stopTime();
    readable.close();
    reportStats(path, totalBytes);
  }

  private void createTFile(String parameters, String compress)
      throws IOException {
    System.out.println("=== TFile: Creation (" + parameters + ") === ");
    Path path = new Path(options.rootDir, "TFile.Performance");
    KVAppendable appendable =
        new TFileAppendable(fs, path, compress, options.minBlockSize,
            options.osOutputBufferSize, conf);
    timeWrite(path, appendable, options.keyLength, options.valueLength,
        options.fileSize);
  }

  private void readTFile(String parameters, boolean delFile) throws IOException {
    System.out.println("=== TFile: Reading (" + parameters + ") === ");
    {
      Path path = new Path(options.rootDir, "TFile.Performance");
      KVReadable readable =
          new TFileReadable(fs, path, options.osInputBufferSize, conf);
      timeRead(path, readable);
      if (delFile) {
        if (fs.exists(path)) {
          fs.delete(path, true);
        }
      }
    }
  }

  private void createSeqFile(String parameters, String compress)
      throws IOException {
    System.out.println("=== SeqFile: Creation (" + parameters + ") === ");
    Path path = new Path(options.rootDir, "SeqFile.Performance");
    KVAppendable appendable =
        new SeqFileAppendable(fs, path, options.osOutputBufferSize, compress,
            options.minBlockSize);
    timeWrite(path, appendable, options.keyLength, options.valueLength,
        options.fileSize);
  }

  private void readSeqFile(String parameters, boolean delFile)
      throws IOException {
    System.out.println("=== SeqFile: Reading (" + parameters + ") === ");
    Path path = new Path(options.rootDir, "SeqFile.Performance");
    KVReadable readable =
        new SeqFileReadable(fs, path, options.osInputBufferSize);
    timeRead(path, readable);
    if (delFile) {
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
    }
  }

  private void compareRun(String compress) throws IOException {
    String[] supported = TFile.getSupportedCompressionAlgorithms();
    boolean proceed = false;
    for (String c : supported) {
      if (c.equals(compress)) {
        proceed = true;
        break;
      }
    }

    if (!proceed) {
      System.out.println("Skipped for " + compress);
      return;
    }
    
    options.compress = compress;
    String parameters = parameters2String(options);
    createSeqFile(parameters, compress);
    readSeqFile(parameters, true);
    createTFile(parameters, compress);
    readTFile(parameters, true);
    createTFile(parameters, compress);
    readTFile(parameters, true);
    createSeqFile(parameters, compress);
    readSeqFile(parameters, true);
  }

  @Test
  public void testRunComparisons() throws IOException {
    String[] compresses = new String[] { "none", "lzo", "gz" };
    for (String compress : compresses) {
      if (compress.equals("none")) {
        conf
            .setInt("tfile.fs.input.buffer.size", options.fsInputBufferSizeNone);
        conf.setInt("tfile.fs.output.buffer.size",
            options.fsOutputBufferSizeNone);
      }
      else if (compress.equals("lzo")) {
        conf.setInt("tfile.fs.input.buffer.size", options.fsInputBufferSizeLzo);
        conf.setInt("tfile.fs.output.buffer.size",
            options.fsOutputBufferSizeLzo);
      }
      else {
        conf.setInt("tfile.fs.input.buffer.size", options.fsInputBufferSizeGz);
        conf
            .setInt("tfile.fs.output.buffer.size", options.fsOutputBufferSizeGz);
      }
      compareRun(compress);
    }
  }

  private static String parameters2String(MyOptions options) {
    return String
        .format(
            "KLEN: %d-%d... VLEN: %d-%d...MinBlkSize: %.2fKB...Target Size: %.2fMB...Compression: ...%s",
            options.keyLength, options.keyLength * 2, options.valueLength,
            options.valueLength * 2, (double) options.minBlockSize / 1024,
            (double) options.fileSize / 1024 / 1024, options.compress);
  }

  private static class MyOptions {
    String rootDir = GenericTestUtils.getTestDir().getAbsolutePath();
    String compress = "gz";
    String format = "tfile";
    int dictSize = 1000;
    int minWordLen = 5;
    int maxWordLen = 20;
    int keyLength = 50;
    int valueLength = 100;
    int minBlockSize = 256 * 1024;
    int fsOutputBufferSize = 1;
    int fsInputBufferSize = 0;
    // special variable only for unit testing.
    int fsInputBufferSizeNone = 0;
    int fsInputBufferSizeGz = 0;
    int fsInputBufferSizeLzo = 0;
    int fsOutputBufferSizeNone = 1;
    int fsOutputBufferSizeGz = 1;
    int fsOutputBufferSizeLzo = 1;

    // un-exposed parameters.
    int osInputBufferSize = 64 * 1024;
    int osOutputBufferSize = 64 * 1024;

    long fileSize = 3 * 1024 * 1024;
    long seed;

    static final int OP_CREATE = 1;
    static final int OP_READ = 2;
    int op = OP_READ;

    boolean proceed = false;

    public MyOptions(String[] args) {
      seed = System.nanoTime();

      try {
        Options opts = buildOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(opts, args, true);
        processOptions(line, opts);
        validateOptions();
      }
      catch (ParseException e) {
        System.out.println(e.getMessage());
        System.out.println("Try \"--help\" option for details.");
        setStopProceed();
      }
    }

    public boolean proceed() {
      return proceed;
    }

    private Options buildOptions() {
      Option compress =
          Option.builder("c").longOpt("compress").argName("[none|lzo|gz]")
          .hasArg().desc("compression scheme").build();

      Option ditSize =
          Option.builder("d").longOpt("dict").argName("size")
          .hasArg().desc("number of dictionary entries").build();

      Option fileSize =
          Option.builder("s").longOpt("file-size").argName("size-in-MB")
          .hasArg().desc("target size of the file (in MB).").build();

      Option format =
          Option.builder("f").longOpt("format").argName("[tfile|seqfile]")
          .hasArg().desc("choose TFile or SeqFile").build();

      Option fsInputBufferSz =
          Option.builder("i").longOpt("fs-input-buffer").argName("size")
          .hasArg().desc("size of the file system input buffer (in bytes).").build();

      Option fsOutputBufferSize =
          Option.builder("o").longOpt("fs-output-buffer").argName("size")
          .hasArg().desc("size of the file system output buffer (in bytes).").build();

      Option keyLen =
          Option.builder("o").longOpt("key-length").argName("length")
          .hasArg()
          .desc("base length of the key (in bytes), actual length varies in [base, 2*base)")
          .build();

      Option valueLen =
          Option.builder("v").longOpt("key-length").argName("length")
          .longOpt("value-length").argName("length").hasArg()
          .desc("base length of the value (in bytes), actual length varies in [base, 2*base)")
          .build();

      Option wordLen =
          Option.builder("w").longOpt("word-length").argName("min,max")
          .hasArg().desc("range of dictionary word length (in bytes)").build();

      Option blockSz =
          Option.builder("b").longOpt("block").argName("size-in-KB").hasArg()
          .desc("minimum block size (in KB)").build();

      Option seed =
          Option.builder("S").longOpt("seed").argName("long-int").hasArg()
          .desc("specify the seed").build();

      Option operation =
          Option.builder("x").longOpt("operation").argName("r|w|rw").hasArg()
          .desc("action: read-only, create-only, read-after-create").build();

      Option rootDir =
          Option.builder("r").longOpt("root-dir").argName("path").hasArg()
          .desc("specify root directory where files will be created.").build();

      Option help =
          Option.builder("h").longOpt("help").hasArg(false)
          .desc("show this screen").build();

      return new Options().addOption(compress).addOption(ditSize).addOption(
          fileSize).addOption(format).addOption(fsInputBufferSz).addOption(
          fsOutputBufferSize).addOption(keyLen).addOption(wordLen).addOption(
          blockSz).addOption(rootDir).addOption(valueLen).addOption(operation)
          .addOption(help);
    }

    private void processOptions(CommandLine line, Options opts)
        throws ParseException {
      // --help -h and --version -V must be processed first.
      if (line.hasOption('h')) {
        HelpFormatter formatter = new HelpFormatter();
        System.out.println("TFile and SeqFile benchmark.");
        System.out.println();
        formatter.printHelp(100,
            "java ... TestTFileSeqFileComparison [options]",
            "\nSupported options:", opts, "");
        return;
      }

      if (line.hasOption('c')) {
        compress = line.getOptionValue('c');
      }

      if (line.hasOption('d')) {
        dictSize = Integer.parseInt(line.getOptionValue('d'));
      }

      if (line.hasOption('s')) {
        fileSize = Long.parseLong(line.getOptionValue('s')) * 1024 * 1024;
      }

      if (line.hasOption('f')) {
        format = line.getOptionValue('f');
      }

      if (line.hasOption('i')) {
        fsInputBufferSize = Integer.parseInt(line.getOptionValue('i'));
      }

      if (line.hasOption('o')) {
        fsOutputBufferSize = Integer.parseInt(line.getOptionValue('o'));
      }

      if (line.hasOption('k')) {
        keyLength = Integer.parseInt(line.getOptionValue('k'));
      }

      if (line.hasOption('v')) {
        valueLength = Integer.parseInt(line.getOptionValue('v'));
      }

      if (line.hasOption('b')) {
        minBlockSize = Integer.parseInt(line.getOptionValue('b')) * 1024;
      }

      if (line.hasOption('r')) {
        rootDir = line.getOptionValue('r');
      }

      if (line.hasOption('S')) {
        seed = Long.parseLong(line.getOptionValue('S'));
      }

      if (line.hasOption('w')) {
        String min_max = line.getOptionValue('w');
        StringTokenizer st = new StringTokenizer(min_max, " \t,");
        if (st.countTokens() != 2) {
          throw new ParseException("Bad word length specification: " + min_max);
        }
        minWordLen = Integer.parseInt(st.nextToken());
        maxWordLen = Integer.parseInt(st.nextToken());
      }

      if (line.hasOption('x')) {
        String strOp = line.getOptionValue('x');
        if (strOp.equals("r")) {
          op = OP_READ;
        }
        else if (strOp.equals("w")) {
          op = OP_CREATE;
        }
        else if (strOp.equals("rw")) {
          op = OP_CREATE | OP_READ;
        }
        else {
          throw new ParseException("Unknown action specifier: " + strOp);
        }
      }

      proceed = true;
    }

    private void validateOptions() throws ParseException {
      if (!compress.equals("none") && !compress.equals("lzo")
          && !compress.equals("gz")) {
        throw new ParseException("Unknown compression scheme: " + compress);
      }

      if (!format.equals("tfile") && !format.equals("seqfile")) {
        throw new ParseException("Unknown file format: " + format);
      }

      if (minWordLen >= maxWordLen) {
        throw new ParseException(
            "Max word length must be greater than min word length.");
      }
      return;
    }

    private void setStopProceed() {
      proceed = false;
    }

    public boolean doCreate() {
      return (op & OP_CREATE) != 0;
    }

    public boolean doRead() {
      return (op & OP_READ) != 0;
    }
  }

  public static void main(String[] args) throws IOException {
    TestTFileSeqFileComparison testCase = new TestTFileSeqFileComparison();
    MyOptions options = new MyOptions(args);
    if (options.proceed == false) {
      return;
    }
    testCase.options = options;
    String parameters = parameters2String(options);

    testCase.setUp();
    if (testCase.options.format.equals("tfile")) {
      if (options.doCreate()) {
        testCase.createTFile(parameters, options.compress);
      }
      if (options.doRead()) {
        testCase.readTFile(parameters, options.doCreate());
      }
    }
    else {
      if (options.doCreate()) {
        testCase.createSeqFile(parameters, options.compress);
      }
      if (options.doRead()) {
        testCase.readSeqFile(parameters, options.doCreate());
      }
    }
    testCase.tearDown();
  }
}

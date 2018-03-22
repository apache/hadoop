/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode.rawcoder;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.util.StopWatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A benchmark tool to test the performance of different erasure coders.
 * The tool launches multiple threads to encode/decode certain amount of data,
 * and measures the total throughput. It only focuses on performance and doesn't
 * validate correctness of the encoded/decoded results.
 * User can specify the data size each thread processes, as well as the chunk
 * size to use for the coder.
 * Different coders are supported. User can specify the coder by a coder index.
 * The coder is shared among all the threads.
 */
public final class RawErasureCoderBenchmark {

  private RawErasureCoderBenchmark() {
    // prevent instantiation
  }

  // target size of input data buffer
  private static final int TARGET_BUFFER_SIZE_MB = 126;

  private static final int MAX_CHUNK_SIZE =
      TARGET_BUFFER_SIZE_MB / BenchData.NUM_DATA_UNITS * 1024;

  private static final List<RawErasureCoderFactory> CODER_MAKERS =
      Collections.unmodifiableList(
          Arrays.asList(new DummyRawErasureCoderFactory(),
              new RSLegacyRawErasureCoderFactory(),
              new RSRawErasureCoderFactory(),
              new NativeRSRawErasureCoderFactory()));

  enum CODER {
    DUMMY_CODER("Dummy coder"),
    LEGACY_RS_CODER("Legacy Reed-Solomon Java coder"),
    RS_CODER("Reed-Solomon Java coder"),
    ISAL_CODER("ISA-L coder");

    private final String name;

    CODER(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  static {
    Preconditions.checkArgument(CODER_MAKERS.size() == CODER.values().length);
  }

  private static void printAvailableCoders() {
    StringBuilder sb = new StringBuilder(
        "Available coders with coderIndex:\n");
    for (CODER coder : CODER.values()) {
      sb.append(coder.ordinal()).append(":").append(coder).append("\n");
    }
    System.out.println(sb.toString());
  }

  private static void usage(String message) {
    if (message != null) {
      System.out.println(message);
    }
    System.out.println(
        "Usage: RawErasureCoderBenchmark <encode/decode> <coderIndex> " +
            "[numThreads] [dataSize-in-MB] [chunkSize-in-KB]");
    printAvailableCoders();
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    String opType = null;
    int coderIndex = 0;
    // default values
    int dataSizeMB = 10240;
    int chunkSizeKB = 1024;
    int numThreads = 1;

    if (args.length > 1) {
      opType = args[0];
      if (!"encode".equals(opType) && !"decode".equals(opType)) {
        usage("Invalid type: should be either 'encode' or 'decode'");
      }

      try {
        coderIndex = Integer.parseInt(args[1]);
        if (coderIndex < 0 || coderIndex >= CODER.values().length) {
          usage("Invalid coder index, should be [0-" +
              (CODER.values().length - 1) + "]");
        }
      } catch (NumberFormatException e) {
        usage("Malformed coder index, " + e.getMessage());
      }
    } else {
      usage(null);
    }

    if (args.length > 2) {
      try {
        numThreads = Integer.parseInt(args[2]);
        if (numThreads <= 0) {
          usage("Invalid number of threads.");
        }
      } catch (NumberFormatException e) {
        usage("Malformed number of threads, " + e.getMessage());
      }
    }

    if (args.length > 3) {
      try {
        dataSizeMB = Integer.parseInt(args[3]);
        if (dataSizeMB <= 0) {
          usage("Invalid data size.");
        }
      } catch (NumberFormatException e) {
        usage("Malformed data size, " + e.getMessage());
      }
    }

    if (args.length > 4) {
      try {
        chunkSizeKB = Integer.parseInt(args[4]);
        if (chunkSizeKB <= 0) {
          usage("Chunk size should be positive.");
        }
        if (chunkSizeKB > MAX_CHUNK_SIZE) {
          usage("Chunk size should be no larger than " + MAX_CHUNK_SIZE);
        }
      } catch (NumberFormatException e) {
        usage("Malformed chunk size, " + e.getMessage());
      }
    }

    performBench(opType, CODER.values()[coderIndex],
        numThreads, dataSizeMB, chunkSizeKB);
  }

  /**
   * Performs benchmark.
   *
   * @param opType      The operation to perform. Can be encode or decode
   * @param coder       The coder to use
   * @param numThreads  Number of threads to launch concurrently
   * @param dataSizeMB  Total test data size in MB
   * @param chunkSizeKB Chunk size in KB
   */
  public static void performBench(String opType, CODER coder,
      int numThreads, int dataSizeMB, int chunkSizeKB) throws Exception {
    BenchData.configure(dataSizeMB, chunkSizeKB);

    RawErasureEncoder encoder = null;
    RawErasureDecoder decoder = null;
    ByteBuffer testData;
    boolean isEncode = opType.equals("encode");

    if (isEncode) {
      encoder = getRawEncoder(coder.ordinal());
      testData = genTestData(encoder.preferDirectBuffer(),
          BenchData.bufferSizeKB);
    } else {
      decoder = getRawDecoder(coder.ordinal());
      testData = genTestData(decoder.preferDirectBuffer(),
          BenchData.bufferSizeKB);
    }

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<Long>> futures = new ArrayList<>(numThreads);
    StopWatch sw = new StopWatch().start();
    for (int i = 0; i < numThreads; i++) {
      futures.add(executor.submit(new BenchmarkCallable(isEncode,
          encoder, decoder, testData.duplicate())));
    }
    List<Long> durations = new ArrayList<>(numThreads);
    try {
      for (Future<Long> future : futures) {
        durations.add(future.get());
      }
      long duration = sw.now(TimeUnit.MILLISECONDS);
      double totalDataSize = BenchData.totalDataSizeKB * numThreads / 1024.0;
      DecimalFormat df = new DecimalFormat("#.##");
      System.out.println(coder + " " + opType + " " +
          df.format(totalDataSize) + "MB data, with chunk size " +
          BenchData.chunkSize / 1024 + "KB");
      System.out.println("Total time: " + df.format(duration / 1000.0) + " s.");
      System.out.println("Total throughput: " + df.format(
          totalDataSize / duration * 1000.0) + " MB/s");
      printThreadStatistics(durations, df);
    } catch (Exception e) {
      System.out.println("Error waiting for thread to finish.");
      e.printStackTrace();
      throw e;
    } finally {
      executor.shutdown();
    }
  }

  private static RawErasureEncoder getRawEncoder(int index) throws IOException {
    RawErasureEncoder encoder =
        CODER_MAKERS.get(index).createEncoder(BenchData.OPTIONS);
    final boolean isDirect = encoder.preferDirectBuffer();
    encoder.encode(
        getBufferForInit(BenchData.NUM_DATA_UNITS, 1, isDirect),
        getBufferForInit(BenchData.NUM_PARITY_UNITS, 1, isDirect));
    return encoder;
  }

  private static RawErasureDecoder getRawDecoder(int index) throws IOException {
    RawErasureDecoder decoder =
        CODER_MAKERS.get(index).createDecoder(BenchData.OPTIONS);
    final boolean isDirect = decoder.preferDirectBuffer();
    ByteBuffer[] inputs = getBufferForInit(
        BenchData.NUM_ALL_UNITS, 1, isDirect);
    for (int erasedIndex : BenchData.ERASED_INDEXES) {
      inputs[erasedIndex] = null;
    }
    decoder.decode(inputs, BenchData.ERASED_INDEXES,
        getBufferForInit(BenchData.ERASED_INDEXES.length, 1, isDirect));
    return decoder;
  }

  private static ByteBuffer[] getBufferForInit(int numBuf,
      int bufCap, boolean isDirect) {
    ByteBuffer[] buffers = new ByteBuffer[numBuf];
    for (int i = 0; i < buffers.length; i++) {
      buffers[i] = isDirect ? ByteBuffer.allocateDirect(bufCap) :
          ByteBuffer.allocate(bufCap);
    }
    return buffers;
  }

  private static void printThreadStatistics(
      List<Long> durations, DecimalFormat df) {
    Collections.sort(durations);
    System.out.println("Threads statistics: ");
    Double min = durations.get(0) / 1000.0;
    Double max = durations.get(durations.size() - 1) / 1000.0;
    Long sum = 0L;
    for (Long duration : durations) {
      sum += duration;
    }
    Double avg = sum.doubleValue() / durations.size() / 1000.0;
    Double percentile = durations.get(
        (int) Math.ceil(durations.size() * 0.9) - 1) / 1000.0;
    System.out.println(durations.size() + " threads in total.");
    System.out.println("Min: " + df.format(min) + " s, Max: " +
        df.format(max) + " s, Avg: " + df.format(avg) +
        " s, 90th Percentile: " + df.format(percentile) + " s.");
  }

  private static ByteBuffer genTestData(boolean useDirectBuffer, int sizeKB) {
    Random random = new Random();
    int bufferSize = sizeKB * 1024;
    byte[] tmp = new byte[bufferSize];
    random.nextBytes(tmp);
    ByteBuffer data = useDirectBuffer ?
        ByteBuffer.allocateDirect(bufferSize) :
        ByteBuffer.allocate(bufferSize);
    data.put(tmp);
    data.flip();
    return data;
  }

  private static class BenchData {
    public static final ErasureCoderOptions OPTIONS =
        new ErasureCoderOptions(6, 3);
    public static final int NUM_DATA_UNITS = OPTIONS.getNumDataUnits();
    public static final int NUM_PARITY_UNITS = OPTIONS.getNumParityUnits();
    public static final int NUM_ALL_UNITS = OPTIONS.getNumAllUnits();
    private static int chunkSize;
    private static long totalDataSizeKB;
    private static int bufferSizeKB;

    private static final int[] ERASED_INDEXES = new int[]{6, 7, 8};
    private final ByteBuffer[] inputs = new ByteBuffer[NUM_DATA_UNITS];
    private ByteBuffer[] outputs = new ByteBuffer[NUM_PARITY_UNITS];
    private ByteBuffer[] decodeInputs = new ByteBuffer[NUM_ALL_UNITS];

    public static void configure(int dataSizeMB, int chunkSizeKB) {
      chunkSize = chunkSizeKB * 1024;
      // buffer size needs to be a multiple of (numDataUnits * chunkSize)
      int round = (int) Math.round(
          TARGET_BUFFER_SIZE_MB * 1024.0 / NUM_DATA_UNITS / chunkSizeKB);
      Preconditions.checkArgument(round > 0);
      bufferSizeKB = NUM_DATA_UNITS * chunkSizeKB * round;
      System.out.println("Using " + bufferSizeKB / 1024 + "MB buffer.");

      round = (int) Math.round(
          (dataSizeMB * 1024.0) / bufferSizeKB);
      if (round == 0) {
        round = 1;
      }
      totalDataSizeKB = round * bufferSizeKB;
    }

    public BenchData(boolean useDirectBuffer) {
      for (int i = 0; i < outputs.length; i++) {
        outputs[i] = useDirectBuffer ? ByteBuffer.allocateDirect(chunkSize) :
            ByteBuffer.allocate(chunkSize);
      }
    }

    public void prepareDecInput() {
      System.arraycopy(inputs, 0, decodeInputs, 0, NUM_DATA_UNITS);
    }

    public void encode(RawErasureEncoder encoder) throws IOException {
      encoder.encode(inputs, outputs);
    }

    public void decode(RawErasureDecoder decoder) throws IOException {
      decoder.decode(decodeInputs, ERASED_INDEXES, outputs);
    }
  }

  private static class BenchmarkCallable implements Callable<Long> {
    private final boolean isEncode;
    private final RawErasureEncoder encoder;
    private final RawErasureDecoder decoder;
    private final BenchData benchData;
    private final ByteBuffer testData;

    public BenchmarkCallable(boolean isEncode, RawErasureEncoder encoder,
        RawErasureDecoder decoder, ByteBuffer testData) {
      if (isEncode) {
        Preconditions.checkArgument(encoder != null);
        this.encoder = encoder;
        this.decoder = null;
        benchData = new BenchData(encoder.preferDirectBuffer());
      } else {
        Preconditions.checkArgument(decoder != null);
        this.decoder = decoder;
        this.encoder = null;
        benchData = new BenchData(decoder.preferDirectBuffer());
      }
      this.isEncode = isEncode;
      this.testData = testData;
    }

    @Override
    public Long call() throws Exception {
      long rounds = BenchData.totalDataSizeKB / BenchData.bufferSizeKB;

      StopWatch sw = new StopWatch().start();
      for (long i = 0; i < rounds; i++) {
        while (testData.remaining() > 0) {
          for (ByteBuffer output : benchData.outputs) {
            output.clear();
          }

          for (int j = 0; j < benchData.inputs.length; j++) {
            benchData.inputs[j] = testData.duplicate();
            benchData.inputs[j].limit(
                testData.position() + BenchData.chunkSize);
            benchData.inputs[j] = benchData.inputs[j].slice();
            testData.position(testData.position() + BenchData.chunkSize);
          }

          if (isEncode) {
            benchData.encode(encoder);
          } else {
            benchData.prepareDecInput();
            benchData.decode(decoder);
          }
        }
        testData.clear();
      }
      return sw.now(TimeUnit.MILLISECONDS);
    }
  }
}

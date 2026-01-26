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

package org.apache.hadoop.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.impl.FileRangeImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class VectoredReadBenchmark {

  static final Path DATA_PATH = getTestDataPath();
  static final String DATA_PATH_PROPERTY = "bench.data";
  static final int READ_SIZE = 64 * 1024;
  static final long SEEK_SIZE = 1024L * 1024;


  static Path getTestDataPath() {
    String value = System.getProperty(DATA_PATH_PROPERTY);
    return new Path(value == null ? "/tmp/taxi.orc" : value);
  }

  @State(Scope.Thread)
  public static class FileSystemChoice {

    @Param({"local", "raw"})
    private String fileSystemKind;

    private Configuration conf;
    private FileSystem fs;

    @Setup(Level.Trial)
    public void setup() {
      conf = new Configuration();
      try {
        LocalFileSystem local = FileSystem.getLocal(conf);
        fs = "raw".equals(fileSystemKind) ? local.getRaw() : local;
      } catch (IOException e) {
        throw new IllegalArgumentException("Can't get filesystem", e);
      }
    }
  }

  @State(Scope.Thread)
  public static class BufferChoice {
    @Param({"direct", "array"})
    private String bufferKind;

    private IntFunction<ByteBuffer> allocate;
    @Setup(Level.Trial)
    public void setup() {
      allocate = "array".equals(bufferKind)
                     ? ByteBuffer::allocate : ByteBuffer::allocateDirect;
    }
  }

  @Benchmark
  public void asyncRead(FileSystemChoice fsChoice,
                        BufferChoice bufferChoice,
                        Blackhole blackhole) throws Exception {
    FSDataInputStream stream = fsChoice.fs.open(DATA_PATH);
    List<FileRange> ranges = new ArrayList<>();
    for(int m=0; m < 100; ++m) {
      FileRange range = FileRange.createFileRange(m * SEEK_SIZE, READ_SIZE);
      ranges.add(range);
    }
    stream.readVectored(ranges, bufferChoice.allocate);
    for(FileRange range: ranges) {
      blackhole.consume(range.getData().get());
    }
    stream.close();
  }

  static class Joiner implements CompletionHandler<ByteBuffer, FileRange> {
    private int remaining;
    private final ByteBuffer[] result;
    private Throwable exception = null;

    Joiner(int total) {
      remaining = total;
      result = new ByteBuffer[total];
    }

    synchronized void finish() {
      remaining -= 1;
      if (remaining == 0) {
        notify();
      }
    }

    synchronized ByteBuffer[] join() throws InterruptedException, IOException {
      while (remaining > 0 && exception == null) {
        wait();
      }
      if (exception != null) {
        throw new IOException("problem reading", exception);
      }
      return result;
    }


    @Override
    public synchronized void completed(ByteBuffer buffer, FileRange attachment) {
      result[--remaining] = buffer;
      if (remaining == 0) {
        notify();
      }
    }

    @Override
    public synchronized void failed(Throwable exc, FileRange attachment) {
      this.exception = exc;
      notify();
    }
  }

  static class FileRangeCallback extends FileRangeImpl implements
          CompletionHandler<Integer, FileRangeCallback> {
    private final AsynchronousFileChannel channel;
    private final ByteBuffer buffer;
    private int completed = 0;
    private final Joiner joiner;

    FileRangeCallback(AsynchronousFileChannel channel, long offset,
                      int length, Joiner joiner, ByteBuffer buffer) {
      super(offset, length, null);
      this.channel = channel;
      this.joiner = joiner;
      this.buffer = buffer;
    }

    @Override
    public void completed(Integer result, FileRangeCallback attachment) {
      final int bytes = result;
      if (bytes == -1) {
        failed(new EOFException("Read past end of file"), this);
      }
      completed += bytes;
      if (completed < this.getLength()) {
        channel.read(buffer, this.getOffset() + completed, this, this);
      } else {
        buffer.flip();
        joiner.finish();
      }
    }

    @Override
    public void failed(Throwable exc, FileRangeCallback attachment) {
      joiner.failed(exc, this);
    }
  }

  @Benchmark
  public void asyncFileChanArray(BufferChoice bufferChoice,
                                 Blackhole blackhole) throws Exception {
    java.nio.file.Path path = FileSystems.getDefault().getPath(DATA_PATH.toString());
    AsynchronousFileChannel channel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
    List<FileRangeImpl> ranges = new ArrayList<>();
    Joiner joiner = new Joiner(100);
    for(int m=0; m < 100; ++m) {
      ByteBuffer buffer = bufferChoice.allocate.apply(READ_SIZE);
      FileRangeCallback range = new FileRangeCallback(channel, m * SEEK_SIZE,
          READ_SIZE, joiner, buffer);
      ranges.add(range);
      channel.read(buffer, range.getOffset(), range, range);
    }
    joiner.join();
    channel.close();
    blackhole.consume(ranges);
  }

  @Benchmark
  public void syncRead(FileSystemChoice fsChoice,
                       Blackhole blackhole) throws Exception {
    FSDataInputStream stream = fsChoice.fs.open(DATA_PATH);
    List<byte[]> result = new ArrayList<>();
    for(int m=0; m < 100; ++m) {
      byte[] buffer = new byte[READ_SIZE];
      stream.readFully(m * SEEK_SIZE, buffer);
      result.add(buffer);
    }
    blackhole.consume(result);
    stream.close();
  }

  /**
   * Run the benchmarks.
   * @param args the pathname of a 100MB data file
   * @throws Exception any ex.
   */
  public static void main(String[] args) throws Exception {
    OptionsBuilder opts = new OptionsBuilder();
    opts.include("VectoredReadBenchmark");
    opts.jvmArgs("-server", "-Xms256m", "-Xmx2g",
        "-D" + DATA_PATH_PROPERTY + "=" + args[0]);
    opts.forks(1);
    new Runner(opts.build()).run();
  }
}

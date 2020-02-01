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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileRangeImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class AsyncBenchmark {

  static final Path DATA_PATH = getTestDataPath();
  static final String DATA_PATH_PROPERTY = "bench.data";

  static Path getTestDataPath() {
    String value = System.getProperty(DATA_PATH_PROPERTY);
    return new Path(value == null ? "/tmp/taxi.orc" : value);
  }

  @State(Scope.Thread)
  public static class FileSystemChoice {

    @Param({"local", "raw"})
    String fileSystemKind;

    Configuration conf;
    FileSystem fs;

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
    String bufferKind;

    IntFunction<ByteBuffer> allocate;
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
      FileRangeImpl range = new FileRangeImpl(m * 1024L * 1024, 64 * 1024);
      ranges.add(range);
    }
    stream.readAsync(ranges, bufferChoice.allocate);
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

  static class FileRangeCallback extends FileRangeImpl implements CompletionHandler<Integer, FileRangeCallback> {
    private final AsynchronousFileChannel channel;
    private final ByteBuffer buffer;
    private int completed = 0;
    private final Joiner joiner;

    FileRangeCallback(AsynchronousFileChannel channel, long offset,
                      int length, Joiner joiner, ByteBuffer buffer) {
      super(offset, length);
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
      if (completed < length) {
        channel.read(buffer, offset + completed, this, this);
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
    final int SIZE = 64 * 1024;
    for(int m=0; m < 100; ++m) {
      ByteBuffer buffer = bufferChoice.allocate.apply(SIZE);
      FileRangeCallback range = new FileRangeCallback(channel, m * 1024L * 1024,
          SIZE, joiner, buffer);
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
      byte[] buffer = new byte[64 * 1024];
      stream.readFully(m * 1024L * 1024, buffer);
      result.add(buffer);
    }
    blackhole.consume(result);
    stream.close();
  }

  /**
   * Run the benchmarks.
   * @param args the pathname of a 100MB data file
   */
  public static void main(String[] args) throws Exception {
    OptionsBuilder opts = new OptionsBuilder();
    opts.include("AsyncBenchmark");
    opts.jvmArgs("-server", "-Xms256m", "-Xmx2g",
        "-D" + DATA_PATH_PROPERTY + "=" + args[0]);
    opts.forks(1);
    new Runner(opts.build()).run();
  }
}

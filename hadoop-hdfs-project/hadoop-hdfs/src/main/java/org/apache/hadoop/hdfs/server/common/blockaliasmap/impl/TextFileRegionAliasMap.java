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

package org.apache.hadoop.hdfs.server.common.blockaliasmap.impl;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * This class is used for block maps stored as text files,
 * with a specified delimiter.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TextFileRegionAliasMap
    extends BlockAliasMap<FileRegion> implements Configurable {

  private Configuration conf;
  private ReaderOptions readerOpts = TextReader.defaults();
  private WriterOptions writerOpts = TextWriter.defaults();

  public static final Logger LOG =
      LoggerFactory.getLogger(TextFileRegionAliasMap.class);
  @Override
  public void setConf(Configuration conf) {
    readerOpts.setConf(conf);
    writerOpts.setConf(conf);
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Reader<FileRegion> getReader(Reader.Options opts, String blockPoolID)
      throws IOException {
    if (null == opts) {
      opts = readerOpts;
    }
    if (!(opts instanceof ReaderOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    ReaderOptions o = (ReaderOptions) opts;
    Configuration readerConf = (null == o.getConf())
        ? new Configuration()
            : o.getConf();
    return createReader(o.file, o.delim, readerConf, blockPoolID);
  }

  @VisibleForTesting
  TextReader createReader(Path file, String delim, Configuration cfg,
      String blockPoolID) throws IOException {
    FileSystem fs = file.getFileSystem(cfg);
    if (fs instanceof LocalFileSystem) {
      fs = ((LocalFileSystem)fs).getRaw();
    }
    CompressionCodecFactory factory = new CompressionCodecFactory(cfg);
    CompressionCodec codec = factory.getCodec(file);
    String filename = fileNameFromBlockPoolID(blockPoolID);
    if (codec != null) {
      filename = filename + codec.getDefaultExtension();
    }
    Path bpidFilePath = new Path(file.getParent(), filename);
    return new TextReader(fs, bpidFilePath, codec, delim);
  }

  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts, String blockPoolID)
      throws IOException {
    if (null == opts) {
      opts = writerOpts;
    }
    if (!(opts instanceof WriterOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    WriterOptions o = (WriterOptions) opts;
    Configuration cfg = (null == o.getConf())
        ? new Configuration()
            : o.getConf();
    String baseName = fileNameFromBlockPoolID(blockPoolID);
    Path blocksFile = new Path(o.dir, baseName);
    if (o.codec != null) {
      CompressionCodecFactory factory = new CompressionCodecFactory(cfg);
      CompressionCodec codec = factory.getCodecByName(o.codec);
      blocksFile = new Path(o.dir, baseName + codec.getDefaultExtension());
      return createWriter(blocksFile, codec, o.delim, cfg);
    }
    return createWriter(blocksFile, null, o.delim, conf);
  }

  @VisibleForTesting
  TextWriter createWriter(Path file, CompressionCodec codec, String delim,
      Configuration cfg) throws IOException {
    FileSystem fs = file.getFileSystem(cfg);
    if (fs instanceof LocalFileSystem) {
      fs = ((LocalFileSystem)fs).getRaw();
    }
    OutputStream tmp = fs.create(file);
    java.io.Writer out = new BufferedWriter(new OutputStreamWriter(
          (null == codec) ? tmp : codec.createOutputStream(tmp), "UTF-8"));
    return new TextWriter(out, delim);
  }

  /**
   * Class specifying reader options for the {@link TextFileRegionAliasMap}.
   */
  public static class ReaderOptions
      implements TextReader.Options, Configurable {

    private Configuration conf;
    private String delim =
        DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER_DEFAULT;
    private Path file = new Path(
        new File(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE_DEFAULT)
            .toURI().toString());

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      String tmpfile =
          conf.get(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE,
              DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE_DEFAULT);
      file = new Path(tmpfile);
      delim = conf.get(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER,
          DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER_DEFAULT);
      LOG.info("TextFileRegionAliasMap: read path {}", tmpfile);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public ReaderOptions filename(Path file) {
      this.file = file;
      return this;
    }

    @Override
    public ReaderOptions delimiter(String delim) {
      this.delim = delim;
      return this;
    }
  }

  /**
   * Class specifying writer options for the {@link TextFileRegionAliasMap}.
   */
  public static class WriterOptions
      implements TextWriter.Options, Configurable {

    private Configuration conf;
    private String codec = null;
    private Path dir =
        new Path(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR_DEFAULT);
    private String delim =
        DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER_DEFAULT;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      String tmpDir = conf.get(
          DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR, dir.toString());
      dir = new Path(tmpDir);
      codec = conf.get(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_CODEC);
      delim = conf.get(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER,
          DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER_DEFAULT);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public WriterOptions dirName(Path dir) {
      this.dir = dir;
      return this;
    }

    public String getCodec() {
      return codec;
    }

    public Path getDir() {
      return dir;
    }

    @Override
    public WriterOptions codec(String codec) {
      this.codec = codec;
      return this;
    }

    @Override
    public WriterOptions delimiter(String delim) {
      this.delim = delim;
      return this;
    }

  }

  /**
   * This class is used as a reader for block maps which
   * are stored as delimited text files.
   */
  public static class TextReader extends Reader<FileRegion> {

    /**
     * Options for {@link TextReader}.
     */
    public interface Options extends Reader.Options {
      Options filename(Path file);
      Options delimiter(String delim);
    }

    public static ReaderOptions defaults() {
      return new ReaderOptions();
    }

    private final Path file;
    private final String delim;
    private final FileSystem fs;
    private final CompressionCodec codec;
    private final Map<FRIterator, BufferedReader> iterators;
    private final String blockPoolID;

    protected TextReader(FileSystem fs, Path file, CompressionCodec codec,
        String delim) {
      this(fs, file, codec, delim,
          new IdentityHashMap<FRIterator, BufferedReader>());
    }

    TextReader(FileSystem fs, Path file, CompressionCodec codec, String delim,
        Map<FRIterator, BufferedReader> iterators) {
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.delim = delim;
      this.iterators = Collections.synchronizedMap(iterators);
      this.blockPoolID = blockPoolIDFromFileName(file);
    }

    @Override
    public Optional<FileRegion> resolve(Block ident) throws IOException {
      // consider layering index w/ composable format
      Iterator<FileRegion> i = iterator();
      try {
        while (i.hasNext()) {
          FileRegion f = i.next();
          if (f.getBlock().equals(ident)) {
            return Optional.of(f);
          }
        }
      } finally {
        BufferedReader r = iterators.remove(i);
        if (r != null) {
          // null on last element
          r.close();
        }
      }
      return Optional.empty();
    }

    class FRIterator implements Iterator<FileRegion> {

      private FileRegion pending;

      @Override
      public boolean hasNext() {
        return pending != null;
      }

      @Override
      public FileRegion next() {
        if (null == pending) {
          throw new NoSuchElementException();
        }
        FileRegion ret = pending;
        try {
          pending = nextInternal(this);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return ret;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    private FileRegion nextInternal(Iterator<FileRegion> i) throws IOException {
      BufferedReader r = iterators.get(i);
      if (null == r) {
        throw new IllegalStateException();
      }
      String line = r.readLine();
      if (null == line) {
        iterators.remove(i);
        return null;
      }
      String[] f = line.split(delim);
      if (f.length != 5 && f.length != 6) {
        throw new IOException("Invalid line: " + line);
      }
      byte[] nonce = new byte[0];
      if (f.length == 6) {
        nonce = Base64.getDecoder().decode(f[5]);
      }
      return new FileRegion(Long.parseLong(f[0]), new Path(f[1]),
          Long.parseLong(f[2]), Long.parseLong(f[3]), Long.parseLong(f[4]),
          nonce);
    }

    public InputStream createStream() throws IOException {
      InputStream i = fs.open(file);
      if (codec != null) {
        i = codec.createInputStream(i);
      }
      return i;
    }

    @Override
    public Iterator<FileRegion> iterator() {
      FRIterator i = new FRIterator();
      try {
        BufferedReader r =
            new BufferedReader(new InputStreamReader(createStream(), "UTF-8"));
        iterators.put(i, r);
        i.pending = nextInternal(i);
      } catch (IOException e) {
        iterators.remove(i);
        throw new RuntimeException(e);
      }
      return i;
    }

    @Override
    public void close() throws IOException {
      ArrayList<IOException> ex = new ArrayList<>();
      synchronized (iterators) {
        for (Iterator<BufferedReader> i = iterators.values().iterator();
             i.hasNext();) {
          try {
            BufferedReader r = i.next();
            r.close();
          } catch (IOException e) {
            ex.add(e);
          } finally {
            i.remove();
          }
        }
        iterators.clear();
      }
      if (!ex.isEmpty()) {
        throw MultipleIOException.createIOException(ex);
      }
    }
  }

  /**
   * This class is used as a writer for block maps which
   * are stored as delimited text files.
   */
  public static class TextWriter extends Writer<FileRegion> {

    /**
     * Interface for Writer options.
     */
    public interface Options extends Writer.Options {
      Options codec(String codec);
      Options dirName(Path dir);
      Options delimiter(String delim);
    }

    public static WriterOptions defaults() {
      return new WriterOptions();
    }

    private final String delim;
    private final java.io.Writer out;

    public TextWriter(java.io.Writer out, String delim) {
      this.out = out;
      this.delim = delim;
    }

    @Override
    public void store(FileRegion token) throws IOException {
      final Block block = token.getBlock();
      final ProvidedStorageLocation psl = token.getProvidedStorageLocation();

      out.append(String.valueOf(block.getBlockId())).append(delim);
      out.append(psl.getPath().toString()).append(delim);
      out.append(Long.toString(psl.getOffset())).append(delim);
      out.append(Long.toString(psl.getLength())).append(delim);
      out.append(Long.toString(block.getGenerationStamp()));
      if (psl.getNonce().length > 0) {
        out.append(delim)
            .append(Base64.getEncoder().encodeToString(psl.getNonce()));
      }
      out.append("\n");
    }

    @Override
    public void close() throws IOException {
      out.close();
    }

  }

  @Override
  public void refresh() throws IOException {
    throw new UnsupportedOperationException(
        "Refresh not supported by " + getClass());
  }

  @Override
  public void close() throws IOException {
    // nothing to do;
  }

  @VisibleForTesting
  public static String blockPoolIDFromFileName(Path file) {
    if (file == null) {
      return "";
    }
    String fileName = file.getName();
    return fileName.substring("blocks_".length()).split("\\.")[0];
  }

  @VisibleForTesting
  public static String fileNameFromBlockPoolID(String blockPoolID) {
    return "blocks_" + blockPoolID + ".csv";
  }
}

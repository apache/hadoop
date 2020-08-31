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

package org.apache.hadoop.fs.impl;

import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureRenameBuilder;
import org.apache.hadoop.fs.Options;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Builder for input streams and subclasses whose return value is
 * actually a completable future: this allows for better asynchronous
 * operation.
 *
 * To be more generic, {@link #opt(String, int)} and {@link #must(String, int)}
 * variants provide implementation-agnostic way to customize the builder.
 * Each FS-specific builder implementation can interpret the FS-specific
 * options accordingly, for example:
 *
 * If the option is not related to the file system, the option will be ignored.
 * If the option is must, but not supported by the file system, a
 * {@link IllegalArgumentException} will be thrown.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class FutureRenameBuilderImpl
    implements FutureRenameBuilder {

  private final FileSystem fileSystem;
  private List<String> srcs = new ArrayList<>();
  private List<String> dsts = new ArrayList<>();
  private List<Options.Rename> options = new ArrayList<>();
  /**
   * Constructor.
   * @param srcs owner FS.
   * @param dsts path
   * @param dsts options
   */
  protected FutureRenameBuilderImpl(@Nonnull FileSystem fileSystem,
                                    @Nonnull List<String> srcs,
                                    @Nonnull List<String> dsts,
                                    Options.Rename... options) {
    this.fileSystem = requireNonNull(fileSystem, "fileSystem");
    this.srcs.addAll(srcs);
    this.dsts.addAll(dsts);
    for (Options.Rename opt : options) {
      this.options.add(opt);
    }
  }

  public FileSystem getFS() {
    return fileSystem;
  }

  public List<String> getSrcs() {
    return srcs;
  }

  public List<String> getDsts() {
    return dsts;
  }

  public Options.Rename[] getOptions() {
    return options.toArray(new Options.Rename[options.size()]);
  }

  public FutureRenameBuilder rename(Pair<String, String> src2dst) {
    srcs.add(src2dst.fst);
    dsts.add(src2dst.snd);
    return this;
  }

  public FutureRenameBuilder option(Options.Rename... options) {
    for (Options.Rename opt : options) {
      this.options.add(opt);
    }
    return this;
  }
}

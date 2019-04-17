/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Set;

/**
 * DispatcherContext class holds transport protocol specific context info
 * required for execution of container commands over the container dispatcher.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class DispatcherContext {
  /**
   * Determines which stage of writeChunk a write chunk request is for.
   */
  public enum WriteChunkStage {
    WRITE_DATA, COMMIT_DATA, COMBINED
  }

  // whether the chunk data needs to be written or committed or both
  private final WriteChunkStage stage;
  // indicates whether the read from tmp chunk files is allowed
  private final boolean readFromTmpFile;
  // which term the request is being served in Ratis
  private final long term;
  // the log index in Ratis log to which the request belongs to
  private final long logIndex;

  private final Set<Long> createContainerSet;

  private DispatcherContext(long term, long index, WriteChunkStage stage,
      boolean readFromTmpFile, Set<Long> containerSet) {
    this.term = term;
    this.logIndex = index;
    this.stage = stage;
    this.readFromTmpFile = readFromTmpFile;
    this.createContainerSet = containerSet;
  }

  public long getLogIndex() {
    return logIndex;
  }

  public boolean isReadFromTmpFile() {
    return readFromTmpFile;
  }

  public long getTerm() {
    return term;
  }

  public WriteChunkStage getStage() {
    return stage;
  }

  public Set<Long> getCreateContainerSet() {
    return createContainerSet;
  }

  /**
   * Builder class for building DispatcherContext.
   */
  public static final class Builder {
    private WriteChunkStage stage = WriteChunkStage.COMBINED;
    private boolean readFromTmpFile = false;
    private long term;
    private long logIndex;
    private Set<Long> createContainerSet;

    /**
     * Sets the WriteChunkStage.
     *
     * @param writeChunkStage WriteChunk Stage
     * @return DispatcherContext.Builder
     */
    public Builder setStage(WriteChunkStage writeChunkStage) {
      this.stage = writeChunkStage;
      return this;
    }

    /**
     * Sets the flag for reading from tmp chunk files.
     *
     * @param setReadFromTmpFile whether to read from tmp chunk file or not
     * @return DispatcherContext.Builder
     */
    public Builder setReadFromTmpFile(boolean setReadFromTmpFile) {
      this.readFromTmpFile = setReadFromTmpFile;
      return this;
    }

    /**
     * Sets the current term for the container request from Ratis.
     *
     * @param currentTerm current term
     * @return DispatcherContext.Builder
     */
    public Builder setTerm(long currentTerm) {
      this.term = currentTerm;
      return this;
    }

    /**
     * Sets the logIndex for the container request from Ratis.
     *
     * @param index log index
     * @return DispatcherContext.Builder
     */
    public Builder setLogIndex(long index) {
      this.logIndex = index;
      return this;
    }

    /**
     * Sets the createContainerSet to contain all the containerIds per
     * RaftGroup.
     * @param set createContainerSet
     * @return Builder
     */
    public Builder setCreateContainerSet(Set<Long> set) {
      this.createContainerSet = set;
      return this;
    }
    /**
     * Builds and returns DispatcherContext instance.
     *
     * @return DispatcherContext
     */
    public DispatcherContext build() {
      return new DispatcherContext(term, logIndex, stage, readFromTmpFile,
          createContainerSet);
    }

  }
}

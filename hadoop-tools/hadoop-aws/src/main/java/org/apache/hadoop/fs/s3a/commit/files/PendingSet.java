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

package org.apache.hadoop.fs.s3a.commit.files;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.ValidationFailure;
import org.apache.hadoop.util.JsonSerialization;

import static org.apache.hadoop.fs.s3a.commit.CommitUtils.validateCollectionClass;
import static org.apache.hadoop.fs.s3a.commit.ValidationFailure.verify;

/**
 * Persistent format for multiple pending commits.
 * Contains 0 or more {@link SinglePendingCommit} entries; validation logic
 * checks those values on load.
 */
@SuppressWarnings("unused")
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PendingSet extends PersistentCommitData {
  private static final Logger LOG = LoggerFactory.getLogger(PendingSet.class);

  /**
   * Supported version value: {@value}.
   * If this is changed the value of {@link #serialVersionUID} will change,
   * to avoid deserialization problems.
   */
  public static final int VERSION = 1;

  /**
   * Serialization ID: {@value}.
   */
  private static final long serialVersionUID = 0x11000 + VERSION;


  /** Version marker. */
  private int version = VERSION;

  /**
   * Commit list.
   */
  private List<SinglePendingCommit> commits;

  /**
   * Any custom extra data committer subclasses may choose to add.
   */
  private final Map<String, String> extraData = new HashMap<>(0);

  public PendingSet() {
    this(0);
  }


  public PendingSet(int size) {
    commits = new ArrayList<>(size);
  }

  /**
   * Get a JSON serializer for this class.
   * @return a serializer.
   */
  public static JsonSerialization<PendingSet> serializer() {
    return new JsonSerialization<>(PendingSet.class, false, true);
  }

  /**
   * Load an instance from a file, then validate it.
   * @param fs filesystem
   * @param path path
   * @return the loaded instance
   * @throws IOException IO failure
   * @throws ValidationFailure if the data is invalid
   */
  public static PendingSet load(FileSystem fs, Path path)
      throws IOException {
    LOG.debug("Reading pending commits in file {}", path);
    PendingSet instance = serializer().load(fs, path);
    instance.validate();
    return instance;
  }

  /**
   * Add a commit.
   * @param commit the single commit
   */
  public void add(SinglePendingCommit commit) {
    commits.add(commit);
  }

  /**
   * Deserialize via java Serialization API: deserialize the instance
   * and then call {@link #validate()} to verify that the deserialized
   * data is valid.
   * @param inStream input stream
   * @throws IOException IO problem or validation failure
   * @throws ClassNotFoundException reflection problems
   */
  private void readObject(ObjectInputStream inStream) throws IOException,
      ClassNotFoundException {
    inStream.defaultReadObject();
    validate();
  }

  /**
   * Validate the data: those fields which must be non empty, must be set.
   * @throws ValidationFailure if the data is invalid
   */
  public void validate() throws ValidationFailure {
    verify(version == VERSION, "Wrong version: %s", version);
    validateCollectionClass(extraData.keySet(), String.class);
    validateCollectionClass(extraData.values(), String.class);
    Set<String> destinations = new HashSet<>(commits.size());
    validateCollectionClass(commits, SinglePendingCommit.class);
    for (SinglePendingCommit c : commits) {
      c.validate();
      verify(!destinations.contains(c.getDestinationKey()),
          "Destination %s is written to by more than one pending commit",
          c.getDestinationKey());
      destinations.add(c.getDestinationKey());
    }
  }

  @Override
  public byte[] toBytes() throws IOException {
    return serializer().toBytes(this);
  }

  /**
   * Number of commits.
   * @return the number of commits in this structure.
   */
  public int size() {
    return commits != null ? commits.size() : 0;
  }

  @Override
  public void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException {
    serializer().save(fs, path, this, overwrite);
  }

  /** @return the version marker. */
  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  /**
   * @return commit list.
   */
  public List<SinglePendingCommit> getCommits() {
    return commits;
  }

  public void setCommits(List<SinglePendingCommit> commits) {
    this.commits = commits;
  }
}

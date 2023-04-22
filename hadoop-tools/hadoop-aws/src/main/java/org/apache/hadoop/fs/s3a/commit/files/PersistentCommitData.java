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
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.ValidationFailure;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.util.JsonSerialization;

import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;

/**
 * Class for single/multiple commit data structures.
 * The mapreduce hierarchy {@code AbstractManifestData} is a fork
 * of this; the Success data JSON format must stay compatible
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class PersistentCommitData<T extends PersistentCommitData>
    implements Serializable, IOStatisticsSource {
  private static final Logger LOG = LoggerFactory.getLogger(PersistentCommitData.class);

  /**
   * Supported version value: {@value}.
   * If this is changed the value of {@code serialVersionUID} will change,
   * to avoid deserialization problems.
   */
  public static final int VERSION = 2;

  /**
   * Validate the data: those fields which must be non empty, must be set.
   * @throws ValidationFailure if the data is invalid
   */
  public abstract void validate() throws ValidationFailure;

  /**
   * Serialize to JSON and then to a byte array, after performing a
   * preflight validation of the data to be saved.
   * @return the data in a persistable form.
   * @param serializer serializer to use
   * @throws IOException serialization problem or validation failure.
   */
  public abstract byte[] toBytes(JsonSerialization<T> serializer) throws IOException;

  /**
   * Save to a hadoop filesystem.
   * The destination file is overwritten, and on s3a stores the
   * performance flag is set to turn off all existence checks and
   * parent dir cleanup.
   * The assumption here is: the job knows what it is doing.
   *
   * @param fs filesystem
   * @param path path
   * @param serializer serializer to use
   * @return IOStats from the output stream.
   *
   * @throws IOException IO exception
   */
  public abstract IOStatistics save(FileSystem fs, Path path, JsonSerialization<T> serializer)
      throws IOException;

  /**
   * Load an instance from a status, then validate it.
   * This uses the openFile() API, which S3A supports for
   * faster load and declaring sequential access, always
   * @param <T> type of persistent format
   * @param fs filesystem
   * @param status status of file to load
   * @param serializer serializer to use
   * @return the loaded instance
   * @throws IOException IO failure
   * @throws ValidationFailure if the data is invalid
   */
  public static <T extends PersistentCommitData> T load(FileSystem fs,
      FileStatus status,
      JsonSerialization<T> serializer)
      throws IOException {
    Path path = status.getPath();
    LOG.debug("Reading commit data from file {}", path);
    T result = serializer.load(fs, path, status);
    result.validate();
    return result;
  }

  /**
   * Save to a file.
   * This uses the createFile() API, which S3A supports for
   * faster load and declaring sequential access, always
   *
   * @param <T> type of persistent format
   * @param fs filesystem
   * @param path path to save to
   * @param instance data to save
   * @param serializer serializer to use
   * @param performance skip all safety check on the write
   *
   * @return any IOStatistics from the output stream, or null
   *
   * @throws IOException IO failure
   */
  public static <T extends PersistentCommitData> IOStatistics saveFile(
      final FileSystem fs,
      final Path path,
      final T instance,
      final JsonSerialization<T> serializer,
      final boolean performance)
      throws IOException {

    FSDataOutputStreamBuilder builder = fs.createFile(path)
        .create()
        .recursive()
        .overwrite(true);
    // switch to performance mode
    builder.opt(FS_S3A_CREATE_PERFORMANCE, performance);
    return saveToStream(path, instance, builder, serializer);
  }

  /**
   * Save to a file.
   * This uses the createFile() API, which S3A supports for
   * faster load and declaring sequential access, always
   * @param <T> type of persistent format
   * @param path path to save to (used for logging)
   * @param instance data to save
   * @param builder builder already prepared for the write
   * @param serializer serializer to use
   * @return any IOStatistics from the output stream, or null
   * @throws IOException IO failure
   */
  public static <T extends PersistentCommitData> IOStatistics saveToStream(
      final Path path,
      final T instance,
      final FSDataOutputStreamBuilder builder,
      final JsonSerialization<T> serializer) throws IOException {
    LOG.debug("saving commit data to file {}", path);
    FSDataOutputStream dataOutputStream = builder.build();
    try {
      dataOutputStream.write(serializer.toBytes(instance));
    } finally {
      dataOutputStream.close();
    }
    return dataOutputStream.getIOStatistics();
  }

}

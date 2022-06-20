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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.util.JsonSerialization;

import static java.util.Objects.requireNonNull;

/**
 * Class for single/multiple commit data structures.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Private
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class AbstractManifestData<T extends AbstractManifestData>
    implements Serializable, IOStatisticsSource {


  /**
   * Convert a path to a string which can be included in the JSON.
   * @param path path
   * @return a string value, or, if path==null, null.
   */
  public static String marshallPath(@Nullable Path path) {
    return path != null
        ? path.toUri().toString()
        : null;
  }

  /**
   * Convert a string path to Path type, by way of a URI.
   * @param path path as a string
   * @return path value
   * @throws RuntimeException marshalling failure.
   */
  public static Path unmarshallPath(String path) {
    try {
      return new Path(new URI(requireNonNull(path, "No path")));
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          "Failed to parse \"" + path + "\" : " + e,
          e);
    }
  }

  /**
   * Validate the data: those fields which must be non empty, must be set.
   * @return the validated instance.
   * @throws IOException if the data is invalid
   */
  public abstract T validate() throws IOException;

  /**
   * Serialize to JSON and then to a byte array, after performing a
   * preflight validation of the data to be saved.
   * @return the data in a persistable form.
   * @throws IOException serialization problem or validation failure.
   */
  public abstract byte[] toBytes() throws IOException;

  /**
   * Save to a hadoop filesystem.
   * @param fs filesystem
   * @param path path
   * @param overwrite should any existing file be overwritten
   * @throws IOException IO exception
   */
  public abstract void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException;

  /**
   * Get a (usually shared) JSON serializer.
   * @return a serializer. Call
   */
  public abstract JsonSerialization<T> createSerializer();

  /**
   * Verify that all instances in a collection are of the given class.
   * @param it iterator
   * @param classname classname to require
   * @throws IOException on a failure
   */
  void validateCollectionClass(Iterable it, Class classname)
      throws IOException {
    for (Object o : it) {
      verify(o.getClass().equals(classname),
          "Collection element is not a %s: %s", classname, o.getClass());
    }
  }

  /**
   * Verify that a condition holds.
   * @param expression expression which must be true
   * @param message message to raise on a failure
   * @param args arguments for the message formatting
   * @throws IOException on a failure
   */

  static void verify(boolean expression,
      String message,
      Object... args) throws IOException {
    if (!expression) {
      throw new IOException(String.format(message, args));
    }
  }
}

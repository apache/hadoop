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

package org.apache.hadoop.registry.client.binding;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Support for marshalling objects to and from JSON.
 *  <p>
 * It constructs an object mapper as an instance field.
 * and synchronizes access to those methods
 * which use the mapper
 * @param <T> Type to marshal.
 */
@InterfaceAudience.Private()
@InterfaceStability.Evolving
public class JsonSerDeser<T> {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSerDeser.class);
  private static final String UTF_8 = "UTF-8";
  public static final String E_NO_DATA = "No data at path";
  public static final String E_DATA_TOO_SHORT = "Data at path too short";
  public static final String E_MISSING_MARKER_STRING =
      "Missing marker string: ";

  private final Class<T> classType;
  private final ObjectMapper mapper;

  /**
   * Create an instance bound to a specific type
   * @param classType class to marshall
   */
  public JsonSerDeser(Class<T> classType) {
    Preconditions.checkArgument(classType != null, "null classType");
    this.classType = classType;
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
        false);
  }

  /**
   * Get the simple name of the class type to be marshalled
   * @return the name of the class being marshalled
   */
  public String getName() {
    return classType.getSimpleName();
  }

  /**
   * Convert from JSON
   *
   * @param json input
   * @return the parsed JSON
   * @throws IOException IO
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  @SuppressWarnings("unchecked")
  public synchronized T fromJson(String json)
      throws IOException, JsonParseException, JsonMappingException {
    try {
      return mapper.readValue(json, classType);
    } catch (IOException e) {
      LOG.error("Exception while parsing json : " + e + "\n" + json, e);
      throw e;
    }
  }

  /**
   * Convert from a JSON file
   * @param jsonFile input file
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  @SuppressWarnings("unchecked")
  public synchronized T fromFile(File jsonFile)
      throws IOException, JsonParseException, JsonMappingException {
    try {
      return mapper.readValue(jsonFile, classType);
    } catch (IOException e) {
      LOG.error("Exception while parsing json file {}: {}", jsonFile, e);
      throw e;
    }
  }

  /**
   * Convert from a JSON file
   * @param resource input file
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  @SuppressWarnings({"IOResourceOpenedButNotSafelyClosed"})
  public synchronized T fromResource(String resource)
      throws IOException, JsonParseException, JsonMappingException {
    InputStream resStream = null;
    try {
      resStream = this.getClass().getResourceAsStream(resource);
      if (resStream == null) {
        throw new FileNotFoundException(resource);
      }
      return mapper.readValue(resStream, classType);
    } catch (IOException e) {
      LOG.error("Exception while parsing json resource {}: {}", resource, e);
      throw e;
    } finally {
      IOUtils.closeStream(resStream);
    }
  }

  /**
   * clone by converting to JSON and back again.
   * This is much less efficient than any Java clone process.
   * @param instance instance to duplicate
   * @return a new instance
   * @throws IOException problems.
   */
  public T fromInstance(T instance) throws IOException {
    return fromJson(toJson(instance));
  }

  /**
   * Load from a Hadoop filesystem
   * @param fs filesystem
   * @param path path
   * @return a loaded CD
   * @throws IOException IO problems
   * @throws EOFException if not enough bytes were read in
   * @throws JsonParseException parse problems
   * @throws JsonMappingException O/J mapping problems
   */
  public T load(FileSystem fs, Path path)
      throws IOException, JsonParseException, JsonMappingException {
    FileStatus status = fs.getFileStatus(path);
    long len = status.getLen();
    byte[] b = new byte[(int) len];
    FSDataInputStream dataInputStream = fs.open(path);
    int count = dataInputStream.read(b);
    if (count != len) {
      throw new EOFException(path.toString() + ": read finished prematurely");
    }
    return fromBytes(path.toString(), b);
  }

  /**
   * Save a cluster description to a hadoop filesystem
   * @param fs filesystem
   * @param path path
   * @param overwrite should any existing file be overwritten
   * @throws IOException IO exception
   */
  public void save(FileSystem fs, Path path, T instance,
      boolean overwrite) throws
      IOException {
    FSDataOutputStream dataOutputStream = fs.create(path, overwrite);
    writeJsonAsBytes(instance, dataOutputStream);
  }

  /**
   * Write the json as bytes -then close the file
   * @param dataOutputStream an outout stream that will always be closed
   * @throws IOException on any failure
   */
  private void writeJsonAsBytes(T instance,
      DataOutputStream dataOutputStream) throws IOException {
    try {
      byte[] b = toBytes(instance);
      dataOutputStream.write(b);
    } finally {
      dataOutputStream.close();
    }
  }

  /**
   * Convert JSON To bytes
   * @param instance instance to convert
   * @return a byte array
   * @throws IOException
   */
  public byte[] toBytes(T instance) throws IOException {
    String json = toJson(instance);
    return json.getBytes(UTF_8);
  }

  /**
   * Deserialize from a byte array
   * @param path path the data came from
   * @param bytes byte array
   * @throws IOException all problems
   * @throws EOFException not enough data
   * @throws InvalidRecordException if the parsing failed -the record is invalid
   */
  public T fromBytes(String path, byte[] bytes) throws IOException,
      InvalidRecordException {
    return fromBytes(path, bytes, "");
  }

  /**
   * Deserialize from a byte array, optionally checking for a marker string.
   * <p>
   * If the marker parameter is supplied (and not empty), then its presence
   * will be verified before the JSON parsing takes place; it is a fast-fail
   * check. If not found, an {@link InvalidRecordException} exception will be
   * raised
   * @param path path the data came from
   * @param bytes byte array
   * @param marker an optional string which, if set, MUST be present in the
   * UTF-8 parsed payload.
   * @return The parsed record
   * @throws IOException all problems
   * @throws EOFException not enough data
   * @throws InvalidRecordException if the JSON parsing failed.
   * @throws NoRecordException if the data is not considered a record: either
   * it is too short or it did not contain the marker string.
   */
  public T fromBytes(String path, byte[] bytes, String marker)
      throws IOException, NoRecordException, InvalidRecordException {
    int len = bytes.length;
    if (len == 0 ) {
      throw new NoRecordException(path, E_NO_DATA);
    }
    if (StringUtils.isNotEmpty(marker) && len < marker.length()) {
      throw new NoRecordException(path, E_DATA_TOO_SHORT);
    }
    String json = new String(bytes, 0, len, UTF_8);
    if (StringUtils.isNotEmpty(marker)
        && !json.contains(marker)) {
      throw new NoRecordException(path, E_MISSING_MARKER_STRING + marker);
    }
    try {
      return fromJson(json);
    } catch (JsonProcessingException e) {
      throw new InvalidRecordException(path, e.toString(), e);
    }
  }

  /**
   * Convert an instance to a JSON string
   * @param instance instance to convert
   * @return a JSON string description
   * @throws JsonParseException parse problems
   * @throws JsonMappingException O/J mapping problems
   */
  public synchronized String toJson(T instance) throws IOException,
      JsonGenerationException,
      JsonMappingException {
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    return mapper.writeValueAsString(instance);
  }

  /**
   * Convert an instance to a string form for output. This is a robust
   * operation which will convert any JSON-generating exceptions into
   * error text.
   * @param instance non-null instance
   * @return a JSON string
   */
  public String toString(T instance) {
    Preconditions.checkArgument(instance != null, "Null instance argument");
    try {
      return toJson(instance);
    } catch (IOException e) {
      return "Failed to convert to a string: " + e;
    }
  }
}

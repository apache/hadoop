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
import java.nio.ByteBuffer;
import java.util.Arrays;

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
  public static final String E_NO_SERVICE_RECORD = "No service record at path";

  private final Class<T> classType;
  private final ObjectMapper mapper;
  private final byte[] header;

  /**
   * Create an instance bound to a specific type
   * @param classType class to marshall
   * @param header byte array to use as header
   */
  public JsonSerDeser(Class<T> classType, byte[] header) {
    Preconditions.checkArgument(classType != null, "null classType");
    Preconditions.checkArgument(header != null, "null header");
    this.classType = classType;
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
        false);
    // make an immutable copy to keep findbugs happy.
    byte[] h = new byte[header.length];
    System.arraycopy(header, 0, h, 0, header.length);
    this.header = h;
  }

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
    return fromBytes(path.toString(), b, 0);
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
      DataOutputStream dataOutputStream) throws
      IOException {
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
   * Convert JSON To bytes, inserting the header
   * @param instance instance to convert
   * @return a byte array
   * @throws IOException
   */
  public byte[] toByteswithHeader(T instance) throws IOException {
    byte[] body = toBytes(instance);

    ByteBuffer buffer = ByteBuffer.allocate(body.length + header.length);
    buffer.put(header);
    buffer.put(body);
    return buffer.array();
  }

  /**
   * Deserialize from a byte array
   * @param path path the data came from
   * @param bytes byte array
   * @return offset in the array to read from
   * @throws IOException all problems
   * @throws EOFException not enough data
   * @throws InvalidRecordException if the parsing failed -the record is invalid
   */
  public T fromBytes(String path, byte[] bytes, int offset) throws IOException,
      InvalidRecordException {
    int data = bytes.length - offset;
    if (data <= 0) {
      throw new EOFException("No data at " + path);
    }
    String json = new String(bytes, offset, data, UTF_8);
    try {
      return fromJson(json);
    } catch (JsonProcessingException e) {
      throw new InvalidRecordException(path, e.toString(), e);
    }
  }

  /**
   * Read from a byte array to a type, checking the header first
   * @param path source of data
   * @param buffer buffer
   * @return the parsed structure
   * Null if the record was too short or the header did not match
   * @throws IOException on a failure
   * @throws NoRecordException if header checks implied there was no record
   * @throws InvalidRecordException if record parsing failed
   */
  @SuppressWarnings("unchecked")
  public T fromBytesWithHeader(String path, byte[] buffer) throws IOException {
    int hlen = header.length;
    int blen = buffer.length;
    if (hlen > 0) {
      if (blen < hlen) {
        throw new NoRecordException(path, E_NO_SERVICE_RECORD);
      }
      byte[] magic = Arrays.copyOfRange(buffer, 0, hlen);
      if (!Arrays.equals(header, magic)) {
        LOG.debug("start of entry does not match service record header at {}",
            path);
        throw new NoRecordException(path, E_NO_SERVICE_RECORD);
      }
    }
    return fromBytes(path, buffer, hlen);
  }

  /**
   * Check if a buffer has a header which matches this record type
   * @param buffer buffer
   * @return true if there is a match
   * @throws IOException
   */
  public boolean headerMatches(byte[] buffer) throws IOException {
    int hlen = header.length;
    int blen = buffer.length;
    boolean matches = false;
    if (blen > hlen) {
      byte[] magic = Arrays.copyOfRange(buffer, 0, hlen);
      matches = Arrays.equals(header, magic);
    }
    return matches;
  }

  /**
   * Convert an object to a JSON string
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

}

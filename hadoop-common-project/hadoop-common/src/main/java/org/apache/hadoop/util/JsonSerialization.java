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

package org.apache.hadoop.util;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Support for marshalling objects to and from JSON.
 *
 * It constructs an object mapper as an instance field.
 * and synchronizes access to those methods
 * which use the mapper.
 *
 * This class was extracted from
 * {@code org.apache.hadoop.registry.client.binding.JsonSerDeser},
 * which is now a subclass of this class.
 * @param <T> Type to marshal.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class JsonSerialization<T> {

  private static final Logger LOG =
      LoggerFactory.getLogger(JsonSerialization.class);
  private static final String UTF_8 = "UTF-8";

  private final Class<T> classType;
  private final ObjectMapper mapper;

  private static final ObjectWriter WRITER =
      new ObjectMapper().writerWithDefaultPrettyPrinter();

  private static final ObjectReader MAP_READER =
      new ObjectMapper().readerFor(Map.class);

  /**
   * @return an ObjectWriter which pretty-prints its output
   */
  public static ObjectWriter writer() {
    return WRITER;
  }

  /**
   * @return an ObjectReader which returns simple Maps.
   */
  public static ObjectReader mapReader() {
    return MAP_READER;
  }

  /**
   * Create an instance bound to a specific type.
   * @param classType class to marshall
   * @param failOnUnknownProperties fail if an unknown property is encountered.
   * @param pretty generate pretty (indented) output?
   */
  public JsonSerialization(Class<T> classType,
      boolean failOnUnknownProperties, boolean pretty) {
    Preconditions.checkArgument(classType != null, "null classType");
    this.classType = classType;
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
        failOnUnknownProperties);
    mapper.configure(SerializationFeature.INDENT_OUTPUT, pretty);
  }

  /**
   * Get the simple name of the class type to be marshalled.
   * @return the name of the class being marshalled
   */
  public String getName() {
    return classType.getSimpleName();
  }

  /**
   * Get the mapper of this class.
   * @return the mapper
   */
  public ObjectMapper getMapper() {
    return mapper;
  }

  /**
   * Convert from JSON.
   *
   * @param json input
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonParseException If the input is not well-formatted
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  @SuppressWarnings("unchecked")
  public synchronized T fromJson(String json)
      throws IOException, JsonParseException, JsonMappingException {
    if (json.isEmpty()) {
      throw new EOFException("No data");
    }
    try {
      return mapper.readValue(json, classType);
    } catch (IOException e) {
      LOG.error("Exception while parsing json : {}\n{}", e, json, e);
      throw e;
    }
  }

  /**
   * Read from an input stream.
   * @param stream stream to read from
   * @return the parsed entity
   * @throws IOException IO problems
   * @throws JsonParseException If the input is not well-formatted
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  public synchronized T fromJsonStream(InputStream stream) throws IOException {
    return mapper.readValue(stream, classType);
  }

  /**
   * Load from a JSON text file.
   * @param jsonFile input file
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonParseException If the input is not well-formatted
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  @SuppressWarnings("unchecked")
  public synchronized T load(File jsonFile)
      throws IOException, JsonParseException, JsonMappingException {
    if (!jsonFile.isFile()) {
      throw new FileNotFoundException("Not a file: " + jsonFile);
    }
    if (jsonFile.length() == 0) {
      throw new EOFException("File is empty: " + jsonFile);
    }
    try {
      return mapper.readValue(jsonFile, classType);
    } catch (IOException e) {
      LOG.error("Exception while parsing json file {}", jsonFile, e);
      throw e;
    }
  }

  /**
   * Save to a local file. Any existing file is overwritten unless
   * the OS blocks that.
   * @param file file
   * @param path path
   * @throws IOException IO exception
   */
  public void save(File file, T instance) throws
      IOException {
    writeJsonAsBytes(instance, new FileOutputStream(file));
  }

  /**
   * Convert from a JSON file.
   * @param resource input file
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonParseException If the input is not well-formatted
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  @SuppressWarnings({"IOResourceOpenedButNotSafelyClosed"})
  public synchronized T fromResource(String resource)
      throws IOException, JsonParseException, JsonMappingException {
    try (InputStream resStream = this.getClass()
        .getResourceAsStream(resource)) {
      if (resStream == null) {
        throw new FileNotFoundException(resource);
      }
      return mapper.readValue(resStream, classType);
    } catch (IOException e) {
      LOG.error("Exception while parsing json resource {}", resource, e);
      throw e;
    }
  }

  /**
   * clone by converting to JSON and back again.
   * This is much less efficient than any Java clone process.
   * @param instance instance to duplicate
   * @return a new instance
   * @throws IOException IO problems.
   */
  public T fromInstance(T instance) throws IOException {
    return fromJson(toJson(instance));
  }

  /**
   * Load from a Hadoop filesystem.
   * There's a check for data availability after the file is open, by
   * raising an EOFException if stream.available == 0.
   * This allows for a meaningful exception without the round trip overhead
   * of a getFileStatus call before opening the file. It may be brittle
   * against an FS stream which doesn't return a value here, but the
   * standard filesystems all do.
   * JSON parsing and mapping problems
   * are converted to IOEs.
   * @param fs filesystem
   * @param path path
   * @return a loaded object
   * @throws IOException IO or JSON parse problems
   */
  public T load(FileSystem fs, Path path) throws IOException {
    try (FSDataInputStream dataInputStream = fs.open(path)) {
      // throw an EOF exception if there is no data available.
      if (dataInputStream.available() == 0) {
        throw new EOFException("No data in " + path);
      }
      return fromJsonStream(dataInputStream);
    } catch (JsonProcessingException e) {
      throw new IOException(
          String.format("Failed to read JSON file \"%s\": %s", path, e),
          e);
    }
  }

  /**
   * Save to a Hadoop filesystem.
   * @param fs filesystem
   * @param path path
   * @param overwrite should any existing file be overwritten
   * @throws IOException IO exception
   */
  public void save(FileSystem fs, Path path, T instance,
      boolean overwrite) throws
      IOException {
    writeJsonAsBytes(instance, fs.create(path, overwrite));
  }

  /**
   * Write the JSON as bytes, then close the file.
   * @param dataOutputStream an output stream that will always be closed
   * @throws IOException on any failure
   */
  private void writeJsonAsBytes(T instance,
      OutputStream dataOutputStream) throws IOException {
    try {
      dataOutputStream.write(toBytes(instance));
    } finally {
      dataOutputStream.close();
    }
  }

  /**
   * Convert JSON to bytes.
   * @param instance instance to convert
   * @return a byte array
   * @throws IOException IO problems
   */
  public byte[] toBytes(T instance) throws IOException {
    return mapper.writeValueAsBytes(instance);
  }

  /**
   * Deserialize from a byte array.
   * @param bytes byte array
   * @throws IOException IO problems
   * @throws EOFException not enough data
   */
  public T fromBytes(byte[] bytes) throws IOException {
    return fromJson(new String(bytes, 0, bytes.length, UTF_8));
  }

  /**
   * Convert an instance to a JSON string.
   * @param instance instance to convert
   * @return a JSON string description
   * @throws JsonProcessingException Json generation problems
   */
  public synchronized String toJson(T instance) throws JsonProcessingException {
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
    } catch (JsonProcessingException e) {
      return "Failed to convert to a string: " + e;
    }
  }
}

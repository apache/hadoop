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

package org.apache.hadoop.yarn.service.utils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Support for marshalling objects to and from JSON.
 * This class is NOT thread safe; it constructs an object mapper
 * as an instance field.
 * @param <T>
 */
public class JsonSerDeser<T> {

  private static final Logger log = LoggerFactory.getLogger(JsonSerDeser.class);

  private final Class<T> classType;
  private final ObjectMapper mapper;

  /**
   * Create an instance bound to a specific type
   * @param classType class type
   */
  @SuppressWarnings("deprecation")
  public JsonSerDeser(Class<T> classType) {
    this.classType = classType;
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
  }

  public JsonSerDeser(Class<T> classType, PropertyNamingStrategy namingStrategy) {
    this(classType);
    mapper.setPropertyNamingStrategy(namingStrategy);
  }

  /**
   * Convert from JSON
   * @param json input
   * @return the parsed JSON
   * @throws IOException IO
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  public T fromJson(String json)
    throws IOException, JsonParseException, JsonMappingException {
    try {
      return mapper.readValue(json, classType);
    } catch (IOException e) {
      log.error("Exception while parsing json : " + e + "\n" + json, e);
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
  public T fromFile(File jsonFile)
    throws IOException, JsonParseException, JsonMappingException {
    File absoluteFile = jsonFile.getAbsoluteFile();
    try {
      return mapper.readValue(absoluteFile, classType);
    } catch (IOException e) {
      log.error("Exception while parsing json file {}", absoluteFile, e);
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
 public T fromResource(String resource)
    throws IOException, JsonParseException, JsonMappingException {
    try(InputStream resStream = this.getClass().getResourceAsStream(resource)) {
      if (resStream == null) {
        throw new FileNotFoundException(resource);
      }
      return (T) (mapper.readValue(resStream, classType));
    } catch (IOException e) {
      log.error("Exception while parsing json resource {}", resource, e);
      throw e;
    }
  }

  /**
   * Convert from an input stream, closing the stream afterwards.
   * @param stream
   * @return the parsed JSON
   * @throws IOException IO problems
   */
  public T fromStream(InputStream stream) throws IOException {
    try {
      return (T) (mapper.readValue(stream, classType));
    } catch (IOException e) {
      log.error("Exception while parsing json input stream", e);
      throw e;
    } finally {
      IOUtils.closeStream(stream);
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
   * Deserialize from a byte array
   * @param b
   * @return the deserialized value
   * @throws IOException parse problems
   */
  public T fromBytes(byte[] b) throws IOException {
    String json = new String(b, 0, b.length, StandardCharsets.UTF_8);
    return fromJson(json);
  }
  
  /**
   * Load from a Hadoop filesystem
   * @param fs filesystem
   * @param path path
   * @return a loaded CD
   * @throws IOException IO problems
   * @throws JsonParseException parse problems
   * @throws JsonMappingException O/J mapping problems
   */
  public T load(FileSystem fs, Path path) throws IOException {
    FSDataInputStream dataInputStream = fs.open(path);
    return fromStream(dataInputStream);
  }


  /**
   * Save to a hadoop filesystem
   * @param fs filesystem
   * @param path path
   * @param instance instance to save
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
   * Save an instance to a file
   * @param instance instance to save
   * @param file file
   * @throws IOException
   */
  public void save(T instance, File file) throws
      IOException {
    writeJsonAsBytes(instance, new FileOutputStream(file.getAbsoluteFile()));
  }

  /**
   * Write the json as bytes -then close the file
   * @param dataOutputStream an outout stream that will always be closed
   * @throws IOException on any failure
   */
  private void writeJsonAsBytes(T instance,
      OutputStream dataOutputStream) throws IOException {
    try {
      String json = toJson(instance);
      byte[] b = json.getBytes(StandardCharsets.UTF_8);
      dataOutputStream.write(b);
      dataOutputStream.flush();
      dataOutputStream.close();
    } finally {
      IOUtils.closeStream(dataOutputStream);
    }
  }

  /**
   * Convert an object to a JSON string
   * @param instance instance to convert
   * @return a JSON string description
   * @throws JsonProcessingException parse problems
   */
  public String toJson(T instance) throws JsonProcessingException {
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    return mapper.writeValueAsString(instance);
  }

}

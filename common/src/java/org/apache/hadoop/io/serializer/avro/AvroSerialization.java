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

package org.apache.hadoop.io.serializer.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.io.serializer.DeserializerBase;
import org.apache.hadoop.io.serializer.SerializationBase;
import org.apache.hadoop.io.serializer.SerializerBase;

/**
 * Base class for providing serialization to Avro types.
 */
public abstract class AvroSerialization<T> extends SerializationBase<T> {
  
  public static final String AVRO_SCHEMA_KEY = "Avro-Schema";

  public DeserializerBase<T> getDeserializer(Map<String, String> metadata) {
    return new AvroDeserializer(metadata);
  }

  public SerializerBase<T> getSerializer(Map<String, String> metadata) {
    return new AvroSerializer(metadata);
  }

  /**
   * Return an Avro Schema instance for the given class and metadata.
   */
  protected abstract Schema getSchema(T t, Map<String, String> metadata);

  /**
   * Create and return Avro DatumWriter for the given metadata.
   */
  protected abstract DatumWriter<T> getWriter(Map<String, String> metadata);

  /**
   * Create and return Avro DatumReader for the given metadata.
   */
  protected abstract DatumReader<T> getReader(Map<String, String> metadata);

  class AvroSerializer extends SerializerBase<T> {

    private Map<String, String> metadata;
    private DatumWriter<T> writer;
    private BinaryEncoder encoder;
    private OutputStream outStream;

    AvroSerializer(Map<String, String> metadata) {
      this.metadata = metadata;
      writer = getWriter(metadata);
    }

    @Override
    public void close() throws IOException {
      encoder.flush();
      outStream.close();
    }

    @Override
    public void open(OutputStream out) throws IOException {
      outStream = out;
      encoder = new BinaryEncoder(out);
    }

    @Override
    public void serialize(T t) throws IOException {
      writer.setSchema(getSchema(t, metadata));
      writer.write(t, encoder);
    }

    @Override
    public Map<String, String> getMetadata() throws IOException {
      return metadata;
    }

  }

  class AvroDeserializer extends DeserializerBase<T> {

    private DatumReader<T> reader;
    private BinaryDecoder decoder;
    private InputStream inStream;

    AvroDeserializer(Map<String, String> metadata) {
      this.reader = getReader(metadata);
    }

    @Override
    public void close() throws IOException {
      inStream.close();
    }

    @Override
    public T deserialize(T t) throws IOException {
      return reader.read(t, decoder);
    }

    @Override
    public void open(InputStream in) throws IOException {
      inStream = in;
      decoder = new BinaryDecoder(in);
    }

  }

}

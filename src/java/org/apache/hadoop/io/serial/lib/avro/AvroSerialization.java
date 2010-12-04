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

package org.apache.hadoop.io.serial.lib.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serial.RawComparator;
import org.apache.hadoop.io.serial.TypedSerialization;
import org.apache.hadoop.io.serial.lib.SerializationMetadata.AvroMetadata;
import org.apache.hadoop.util.ReflectionUtils;
import org.yaml.snakeyaml.Yaml;

/**
 * A binding for Avro binary serialization. It handles generic, specific, and
 * reflection Java Avro serialization.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AvroSerialization extends TypedSerialization<Object> 
                                  implements Configurable {
  /**
   * Key to configure packages that contain classes to be serialized and 
   * deserialized using this class. Multiple packages can be specified using 
   * comma-separated list.
   */
  public static final String AVRO_REFLECT_PACKAGES = "avro.reflect.pkgs";

  public static enum Kind {
    GENERIC(AvroMetadata.Kind.GENERIC), 
    SPECIFIC(AvroMetadata.Kind.SPECIFIC), 
    REFLECTION(AvroMetadata.Kind.REFLECTION);

    private static final EnumMap<AvroMetadata.Kind, Kind> translation =
      new EnumMap<AvroMetadata.Kind,Kind>(AvroMetadata.Kind.class);
    static {
      for (Kind value: Kind.class.getEnumConstants()) {
        translation.put(value.kind, value);
      }
    }

    private AvroMetadata.Kind kind;
    private Kind(AvroMetadata.Kind kind) {
      this.kind = kind;
    }
 
    /**
     * Get the serialized form of the given enumeration.
     * @return the serializable kind
     */
    @InterfaceAudience.Private
    public AvroMetadata.Kind getMetadataKind() {
      return kind;
    }
    
    /**
     * Get the kind from the serialization enumeration.
     * @param kind the serialization enumeration
     * @return the internal kind
     */
    @InterfaceAudience.Private
    public static Kind fromMetadataKind(AvroMetadata.Kind kind) {
      return translation.get(kind);
    }
  }

  private static final DecoderFactory decoderFactory = 
    DecoderFactory.defaultFactory();

  private Configuration conf;
  private Set<String> packages;
  private Kind kind;
  private Schema schema;
  private DatumWriter<Object> writer;
  private DatumReader<Object> reader;
  private Encoder encoder;
  private Decoder decoder;

  private void setKind(Kind kind) {
    this.kind = kind;
    if (kind != null) {
      switch (kind) {
      case GENERIC:
        writer = new GenericDatumWriter<Object>();
        reader = new GenericDatumReader<Object>();
        break;
      case SPECIFIC:
        writer = new SpecificDatumWriter<Object>();
        reader = new SpecificDatumReader<Object>();
        break;
      case REFLECTION:
        writer = new ReflectDatumWriter<Object>();
        reader = new ReflectDatumReader<Object>();
        break;
      }
    }
  }
  
  public AvroSerialization() {
    this(null);
  }

  public AvroSerialization(Kind kind) {
    setKind(kind);
    encoder = new BinaryEncoder(null);
    decoder = decoderFactory.createBinaryDecoder((InputStream) null, null);
  }

  /**
   * Get the schema.
   * @return the avro schema
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Set the schema to the given value.
   * @param schema the new schema
   * @return returns this serialization so that you can use it like a builder
   */
  public AvroSerialization setSchema(Schema schema) {
    this.schema = schema;
    if (kind != null) {
      writer.setSchema(schema);
      reader.setSchema(schema);
    }
    return this;
  }

  @Override
  public void serialize(OutputStream out, Object obj) throws IOException {
    encoder.init(out);
    writer.write(obj, encoder);
    encoder.flush();
  }
  
  @Override
  public Object deserialize(InputStream in, Object reuse, Configuration conf
                            ) throws IOException {
    decoder.init(in);
    Object result = reader.read(reuse, decoder);
    // configure the object, if it wants to be
    if (result != reuse) {
      ReflectionUtils.setConf(result, conf);
    }
    return result;
  }

  /**
   * Provides a raw comparator for Avro-encoded serialized data.
   * @return a RawComparator parameterized for the specified Avro schema.
   */
  @Override
  public RawComparator getRawComparator() {
    return new AvroComparator(schema);
  }

  @Override
  public AvroSerialization clone() {
    AvroSerialization result = (AvroSerialization) super.clone();
    result.setKind(kind);
    result.setSchema(schema);
    return result;
  }

  @Override
  public void deserializeSelf(InputStream in, 
                              Configuration conf) throws IOException {
    AvroMetadata meta = AvroMetadata.parseFrom(in);
    if (kind == null) {
      setKind(Kind.fromMetadataKind(meta.getKind()));
    }
    setSchema(Schema.parse(meta.getSchema()));
  }

  @Override
  public void serializeSelf(OutputStream out) throws IOException {
    AvroMetadata.newBuilder().setKind(kind.kind).setSchema(schema.toString()).
      build().writeTo(out);
  }

  private static final String KIND_ATTRIBUTE = "kind";
  private static final String SCHEMA_ATTRIBUTE = "schema";
  
  @SuppressWarnings("unchecked")
  @Override
  public void fromString(String meta) throws IOException {
    Yaml yaml = new Yaml();
    Map<String, String> map = (Map<String,String>) yaml.load(meta);
    String value = map.get(KIND_ATTRIBUTE);
    if (kind == null && value != null) {
      setKind(Kind.valueOf(value));
    }
    value = map.get(SCHEMA_ATTRIBUTE);
    setSchema(Schema.parse(value));
  }

  public String toString() {
    Yaml yaml = new Yaml();
    Map<String,String> map = new HashMap<String,String>();
    if (kind != null) {
      map.put(KIND_ATTRIBUTE, kind.toString());
    }
    map.put(SCHEMA_ATTRIBUTE, schema.toString());
    return yaml.dump(map);
  }

  private boolean isReflection(Class<?> cls) {
    return AvroReflectSerializable.class.isAssignableFrom(cls) || 
      getPackages().contains(cls.getPackage().getName());
  }

  private Set<String> getPackages() {
    if (packages == null) {
      String[] pkgList  = conf.getStrings(AVRO_REFLECT_PACKAGES);
      packages = new HashSet<String>();
      if (pkgList != null) {
        for (String pkg : pkgList) {
          packages.add(pkg.trim());
        }
      }      
    }
    return packages;
  }

  private boolean isSpecific(Class<?> cls) {
    return SpecificRecord.class.isAssignableFrom(cls);
  }

  @Override
  public boolean accept(Class<?> cls) {
    return isSpecific(cls) || isReflection(cls);
  }

  @Override
  public void setSpecificType(Class<? extends Object> cls) {
    super.setSpecificType(cls);
    if (isSpecific(cls)) {
      setKind(Kind.SPECIFIC);
      setSchema(SpecificData.get().getSchema(cls));
    } else if (isReflection(cls)) {
      setKind(Kind.REFLECTION);
      setSchema(ReflectData.get().getSchema(cls));
    } else {
      throw new IllegalArgumentException("class " + cls.getName() + 
                                         " can't infer schema.");
    }
  }

  @Override
  public Class<Object> getBaseType() {
    // Unlike most of the typed serializations, we don't have a 
    // single base type and the work has to be done in a special accept method.
    return Object.class;
  }
  
  @Override
  public String getName() {
    return "avro";
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != this.conf) { 
      this.conf = conf;
      // clear the cache of packages
      packages = null;
    }
  }
  
  public boolean equals(Object right) {
    if (this == right) {
      return true;
    } else if (right == null || right.getClass() != getClass()) {
      return false;
    } else {
      AvroSerialization rightTyped = (AvroSerialization) right;
      return rightTyped.kind == kind && rightTyped.schema.equals(schema);
    }
  }
  
  public int hashCode() {
    return schema.hashCode() * 5 + kind.hashCode();
  }
}

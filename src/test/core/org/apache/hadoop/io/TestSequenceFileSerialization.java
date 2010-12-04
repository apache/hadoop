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

package org.apache.hadoop.io;

import junit.framework.TestCase;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ProtoTest.ProtoKey;
import org.apache.hadoop.io.ProtoTest.ProtoValue;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.serial.Serialization;
import org.apache.hadoop.io.serial.SerializationFactory;
import org.apache.hadoop.io.serial.lib.CompatibilitySerialization;
import org.apache.hadoop.io.serial.lib.JavaSerialization;
import org.apache.hadoop.io.serial.lib.avro.AvroSerialization;
import org.apache.hadoop.io.serial.lib.avro.AvroSerialization.Kind;

public class TestSequenceFileSerialization extends TestCase {
  
  private Configuration conf;
  private FileSystem fs;
  private Path file;

  @Override
  protected void setUp() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);  
    file = new Path(System.getProperty("test.build.data",".") + "/test.seq");
  }
  
  @Override
  protected void tearDown() throws Exception {
    fs.close();
  }

  public void writeAvroSpecificSerialization(CompressionType kind
                                             ) throws Exception {
    AvroKey key = new AvroKey();
    AvroValue value = new AvroValue();
    fs.delete(file, true);
    Writer writer = 
      SequenceFile.createWriter(conf,
                                SequenceFile.Writer.file(file),
                                SequenceFile.Writer.compression(kind),
                                SequenceFile.Writer.keyClass(AvroKey.class),
                                SequenceFile.Writer.valueClass(AvroValue.class));
    key.value = 1;
    value.value = new Utf8("one");
    writer.append(key, value);
    key.value = 2;
    value.value = new Utf8("two");
    writer.append(key, value);
    String writerKeySerialStr = writer.getKeySerialization().toString();
    String writerValueSerialStr = writer.getValueSerialization().toString();
    writer.close();

    assertEquals("{schema: '{\"type\":\"record\",\"name\":\"AvroKey\"," +
                 "\"namespace\":\"org.apache.hadoop.io\",\"fields\":[{" +
                 "\"name\":\"value\",\"type\":\"int\"}]}',\n" +
                 "  kind: SPECIFIC}\n", 
                 writerKeySerialStr);
    assertEquals("{schema: '{\"type\":\"record\",\"name\":\"AvroValue\"," +
                 "\"namespace\":\"org.apache.hadoop.io\",\"fields\":[{" +
                 "\"name\":\"value\",\"type\":\"string\"}]}',\n" +
                 "  kind: SPECIFIC}\n", 
                 writerValueSerialStr);
    
    SerializationFactory factory = SerializationFactory.getInstance(conf);
    Serialization<?> keySerialClone = factory.getSerialization("avro");
    keySerialClone.fromString(writerKeySerialStr);
    Serialization<?> valueSerialClone = factory.getSerialization("avro");
    valueSerialClone.fromString(writerValueSerialStr);

    Reader reader = new Reader(conf, SequenceFile.Reader.file(file));
    Serialization<?> keySerial = reader.getKeySerialization();
    Serialization<?> valueSerial = reader.getValueSerialization();
    assertEquals(kind, reader.getCompressionType());
    assertEquals("avro", keySerial.getName());
    assertEquals(writerKeySerialStr, keySerial.toString());
    assertEquals(keySerialClone, keySerial);
    assertEquals("avro", valueSerial.getName());
    assertEquals(writerValueSerialStr, valueSerial.toString());
    assertEquals(valueSerialClone, valueSerial);

    assertEquals(1, ((AvroKey) reader.nextKey(key)).value);
    assertEquals(new Utf8("one"), 
                 ((AvroValue) reader.getCurrentValue(value)).value);
    assertEquals(2, ((AvroKey) reader.nextKey(key)).value);
    assertEquals(new Utf8("two"), 
                 ((AvroValue) reader.getCurrentValue(value)).value);
    assertNull(reader.nextKey(null));
    reader.close();    
  }

  public void readAvroGenericSerialization() throws Exception {
    Serialization<?> serial = new AvroSerialization(Kind.GENERIC);
    Reader reader = new Reader(conf, SequenceFile.Reader.file(file),
                               SequenceFile.Reader.keySerialization(serial),
                               SequenceFile.Reader.valueSerialization(serial.clone()));
    
    assertEquals(1, ((GenericRecord) reader.nextKey(null)).get("value"));
    assertEquals(new Utf8("one"), 
                 ((GenericRecord) reader.getCurrentValue(null)).get("value"));
    assertEquals(2, ((GenericRecord) reader.nextKey(null)).get("value"));
    assertEquals(new Utf8("two"), 
                 ((GenericRecord) reader.getCurrentValue(null)).get("value"));
    assertNull(reader.nextKey(null));
    reader.close();        
  }

  public void writeProtobufSerialization(CompressionType kind
                                         ) throws Exception {
    fs.delete(file, true);
    Writer writer = 
      SequenceFile.createWriter(conf,
                                SequenceFile.Writer.file(file),
                                SequenceFile.Writer.compression(kind),
                                SequenceFile.Writer.keyClass(ProtoKey.class),
                                SequenceFile.Writer.valueClass(ProtoValue.class));
    writer.append(ProtoKey.newBuilder().setValue(1).build(), 
                  ProtoValue.newBuilder().setValue("one").build());
    writer.append(ProtoKey.newBuilder().setValue(2).build(), 
                  ProtoValue.newBuilder().setValue("two").build());
    String keySerialStr = writer.getKeySerialization().toString();
    assertEquals("{class: org.apache.hadoop.io.ProtoTest$ProtoKey}\n", 
                 keySerialStr);
    String valueSerialStr = writer.getValueSerialization().toString();
    assertEquals("{class: org.apache.hadoop.io.ProtoTest$ProtoValue}\n", 
                 valueSerialStr);
    writer.close();

    // build serializers from the string form
    SerializationFactory factory = SerializationFactory.getInstance(conf);
    Serialization<?> keySerial = factory.getSerialization("protobuf");
    keySerial.fromString(keySerialStr);
    Serialization<?> valueSerial = factory.getSerialization("protobuf");
    valueSerial.fromString(valueSerialStr);

    Reader reader = new Reader(conf, SequenceFile.Reader.file(file));
    assertEquals(kind, reader.getCompressionType());
    Serialization<?> readerKeySerial = reader.getKeySerialization();
    Serialization<?> readerValueSerial = reader.getValueSerialization();
    assertEquals("protobuf", readerKeySerial.getName());
    assertEquals(keySerialStr, readerKeySerial.toString());
    assertEquals(keySerial, readerKeySerial);
    assertEquals("protobuf", readerValueSerial.getName());
    assertEquals(valueSerialStr, readerValueSerial.toString());
    assertEquals(valueSerial, readerValueSerial);

    assertEquals(ProtoKey.newBuilder().setValue(1).build(), 
                 reader.nextKey(null));
    assertEquals(ProtoValue.newBuilder().setValue("one").build(), 
                 reader.getCurrentValue(null));
    assertEquals(ProtoKey.newBuilder().setValue(2).build(), 
                 reader.nextKey(null));
    assertEquals(ProtoValue.newBuilder().setValue("two").build(), 
                 reader.getCurrentValue(null));
    assertNull(reader.nextKey(null));
    reader.close();    
  }

  public void writeThriftSerialization(CompressionType kind) throws Exception {
    fs.delete(file, true);
    Writer writer = 
      SequenceFile.createWriter(conf,
                                SequenceFile.Writer.file(file),
                                SequenceFile.Writer.compression(kind),
                                SequenceFile.Writer.keyClass(ThriftKey.class),
                                SequenceFile.Writer.valueClass(ThriftValue.class));
    writer.append(new ThriftKey(1), new ThriftValue("one"));
    writer.append(new ThriftKey(2), new ThriftValue("two"));
    writer.close();

    Reader reader = new Reader(conf, SequenceFile.Reader.file(file));
    assertEquals(kind, reader.getCompressionType());
    assertEquals("thrift", reader.getKeySerialization().getName());
    assertEquals("thrift", reader.getValueSerialization().getName());
    assertEquals(new ThriftKey(1), reader.nextKey(null));
    assertEquals(new ThriftValue("one"), reader.getCurrentValue(null));
    assertEquals(new ThriftKey(2), reader.nextKey(null));
    assertEquals(new ThriftValue("two"), reader.getCurrentValue(null));
    assertNull(reader.nextKey(null));
    reader.close();    
  }
  
  public void writeWritableSerialization(CompressionType kind
                                         ) throws Exception {
    fs.delete(file, true);
    Writer writer = 
      SequenceFile.createWriter(conf,
                                SequenceFile.Writer.file(file),
                                SequenceFile.Writer.compression(kind),
                                SequenceFile.Writer.keyClass(IntWritable.class),
                                SequenceFile.Writer.valueClass(Text.class));
    writer.append(new IntWritable(1), new Text("one"));
    writer.append(new IntWritable(2), new Text("two"));
    writer.close();

    Reader reader = new Reader(conf, SequenceFile.Reader.file(file));
    assertEquals(kind, reader.getCompressionType());
    assertEquals("writable", reader.getKeySerialization().getName());
    assertEquals("writable", reader.getValueSerialization().getName());
    assertEquals(new IntWritable(1), reader.nextKey(null));
    assertEquals(new Text("one"), reader.getCurrentValue(null));
    assertEquals(new IntWritable(2), reader.nextKey(null));
    assertEquals(new Text("two"), reader.getCurrentValue(null));
    assertNull(reader.nextKey(null));
    reader.close();    
  }

  public void writeJavaSerialization(CompressionType kind) throws Exception {
    fs.delete(file, true);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SERIALIZATIONS_KEY,
             JavaSerialization.class.getName());
    
    Writer writer = 
      SequenceFile.createWriter(conf,
                                SequenceFile.Writer.file(file),
                                SequenceFile.Writer.compression(kind),
                                SequenceFile.Writer.keyClass(Long.class),
                                SequenceFile.Writer.valueClass(String.class));
    writer.append(1L, "one");
    writer.append(2L, "two");
    writer.close();
    
    Reader reader = new Reader(conf, SequenceFile.Reader.file(file));
    assertEquals(kind, reader.getCompressionType());
    assertEquals("java", reader.getKeySerialization().getName());
    assertEquals("java", reader.getValueSerialization().getName());
    assertEquals(1L, reader.nextKey(null));
    assertEquals("one", reader.getCurrentValue((Object) null));
    assertEquals(2L, reader.nextKey(null));
    assertEquals("two", reader.getCurrentValue((Object) null));
    assertNull(reader.nextKey(null));
    reader.close();
    
  }

  /**
   * Test the compatibility layer to load the old java serialization.
   */
  public void writeOldJavaSerialization(CompressionType kind
                                        ) throws Exception {
    fs.delete(file, true);
    // set the old attribute to include the java serialization
    conf.set("io.serializations",
             "org.apache.hadoop.io.serializer.JavaSerialization");
    SerializationFactory factory = SerializationFactory.getInstance(conf);
    Serialization<?> serial = factory.getSerializationByType(Long.class);
    assertEquals(CompatibilitySerialization.class, serial.getClass());
    
    Writer writer = 
      SequenceFile.createWriter(conf,
                                SequenceFile.Writer.file(file),
                                SequenceFile.Writer.compression(kind),
                                SequenceFile.Writer.keyClass(Long.class),
                                SequenceFile.Writer.valueClass(String.class));
    writer.append(1L, "one");
    writer.append(2L, "two");
    writer.close();
    
    Reader reader = new Reader(conf, SequenceFile.Reader.file(file));
    assertEquals("compatibility", reader.getKeySerialization().getName());
    assertEquals("compatibility", reader.getValueSerialization().getName());
    assertEquals(kind, reader.getCompressionType());
    assertEquals(1L, reader.nextKey(null));
    assertEquals("one", reader.getCurrentValue((Object) null));
    assertEquals(2L, reader.nextKey(null));
    assertEquals("two", reader.getCurrentValue((Object) null));
    assertNull(reader.nextKey(null));
    reader.close();
  }
  
  public void testAvro() throws Exception {
    writeAvroSpecificSerialization(CompressionType.NONE);
    readAvroGenericSerialization();
    writeAvroSpecificSerialization(CompressionType.RECORD);
    writeAvroSpecificSerialization(CompressionType.BLOCK);
  }

  public void testProtobuf() throws Exception {
    writeProtobufSerialization(CompressionType.NONE);
    writeProtobufSerialization(CompressionType.RECORD);
    writeProtobufSerialization(CompressionType.BLOCK);
  }

  public void testThrift() throws Exception {
    writeThriftSerialization(CompressionType.NONE);
    writeThriftSerialization(CompressionType.RECORD);
    writeThriftSerialization(CompressionType.BLOCK);
  }

  public void testWritable() throws Exception {
    writeWritableSerialization(CompressionType.NONE);
    writeWritableSerialization(CompressionType.RECORD);
    writeWritableSerialization(CompressionType.BLOCK);
  }

  public void testJava() throws Exception {
    writeJavaSerialization(CompressionType.NONE);
    writeJavaSerialization(CompressionType.RECORD);
    writeJavaSerialization(CompressionType.BLOCK);
  }

  public void testOldJava() throws Exception {
    writeOldJavaSerialization(CompressionType.NONE);
    writeOldJavaSerialization(CompressionType.RECORD);
    writeOldJavaSerialization(CompressionType.BLOCK);
  }
}

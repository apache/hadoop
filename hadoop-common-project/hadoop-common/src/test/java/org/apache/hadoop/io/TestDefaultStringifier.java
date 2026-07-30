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

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDefaultStringifier {

  private static final Configuration CONF = new Configuration();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDefaultStringifier.class);

  private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyz".toCharArray();

  @Test
  public void testWithWritable() throws Exception {

    CONF.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization");

    LOG.info("Testing DefaultStringifier with Text");

    Random random = new Random();

    //test with a Text
    for(int i=0;i<10;i++) {
      //generate a random string
      StringBuilder builder = new StringBuilder();
      int strLen = random.nextInt(40);
      for(int j=0; j< strLen; j++) {
        builder.append(ALPHABET[random.nextInt(ALPHABET.length)]);
      }
      Text text = new Text(builder.toString());
      DefaultStringifier<Text> stringifier = new DefaultStringifier<>(CONF, Text.class);

      String str = stringifier.toString(text);
      Text claimedText = stringifier.fromString(str);
      LOG.info("Object: {}", text);
      LOG.info("String representation of the object: {}", str);
      assertThat(claimedText).isEqualTo(text);
    }
  }

  @Test
  public void testStoreLoad() throws IOException {

    LOG.info("Testing DefaultStringifier#store() and #load()");
    CONF.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization");
    Text text = new Text("uninteresting test string");
    String keyName = "test.defaultstringifier.key1";

    DefaultStringifier.store(CONF, text, keyName);

    Text claimedText = DefaultStringifier.load(CONF, keyName, Text.class);
    assertThat(claimedText)
        .describedAs("DefaultStringifier round trip")
        .isEqualTo(text);
  }

  @Test
  public void testStoreLoadArray() throws Exception {
    LOG.info("Testing DefaultStringifier#storeArray() and #loadArray()");
    CONF.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization");

    String keyName = "test.defaultstringifier.key2";

    IntWritable[] array = new IntWritable[] {
        new IntWritable(1), new IntWritable(2), new IntWritable(3),
        new IntWritable(4), new IntWritable(5)};


    intercept(IndexOutOfBoundsException.class, () ->
        DefaultStringifier.storeArray(CONF, new IntWritable[] {}, keyName));
    DefaultStringifier.storeArray(CONF, array, keyName);

    IntWritable[] claimedArray =
        DefaultStringifier.loadArray(CONF, keyName, IntWritable.class);
    assertThat(claimedArray).isEqualTo(array);

  }

}

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
package org.apache.hadoop.mrunit;


import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mrunit.mock.MockOutputCollector;
import org.apache.hadoop.mrunit.mock.MockReporter;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness that allows you to test a Mapper instance. You provide the input
 * key and value that should be sent to the Mapper, and outputs you expect to
 * be sent by the Mapper to the collector for those inputs. By calling
 * runTest(), the harness will deliver the input to the Mapper and will check
 * its outputs against the expected results. This is designed to handle a
 * single (k, v) -> (k, v)* case from the Mapper, representing a single unit
 * test. Multiple input (k, v) pairs should go in separate unit tests.
 */
public class MapDriver<K1, V1, K2, V2> extends TestDriver<K1, V1, K2, V2> {

  public static final Log LOG = LogFactory.getLog(MapDriver.class);

  private Mapper<K1, V1, K2, V2> myMapper;

  private K1 inputKey;
  private V1 inputVal;

  public MapDriver(final Mapper<K1, V1, K2, V2> m) {
    myMapper = m;
  }

  public MapDriver() {
  }


  /**
   * Set the Mapper instance to use with this test driver
   *
   * @param m the Mapper instance to use
   */
  public void setMapper(Mapper<K1, V1, K2, V2> m) {
    myMapper = m;
  }

  /** Sets the Mapper instance to use and returns self for fluent style */
  public MapDriver<K1, V1, K2, V2> withMapper(Mapper<K1, V1, K2, V2> m) {
    setMapper(m);
    return this;
  }

  /**
   * @return the Mapper object being used by this test
   */
  public Mapper<K1, V1, K2, V2> getMapper() {
    return myMapper;
  }

  /**
   * Sets the input key to send to the mapper
   *
   */
  public void setInputKey(K1 key) {
    inputKey = key;
  }

  public K1 getInputKey() {
    return inputKey;
  }

  /**
   * Identical to setInputKey() but with fluent programming style
   *
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInputKey(K1 key) {
    setInputKey(key);
    return this;
  }

  /**
   * Sets the input value to send to the mapper
   *
   * @param val
   */
  public void setInputValue(V1 val) {
    inputVal = val;
  }

  public V1 getInputValue() {
    return inputVal;
  }

  /**
   * Identical to setInputValue() but with fluent programming style
   *
   * @param val
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInputValue(V1 val) {
    setInputValue(val);
    return this;
  }

  /**
   * Sets the input to send to the mapper
   *
   */
  public void setInput(K1 key, V1 val) {
    setInputKey(key);
    setInputValue(val);
  }

  /**
   * Identical to setInput() but returns self for fluent programming style
   *
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInput(K1 key, V1 val) {
    setInput(key, val);
    return this;
  }

  /**
   * Sets the input to send to the mapper
   *
   * @param inputRecord
   *          a (key, val) pair
   */
  public void setInput(Pair<K1, V1> inputRecord) {
    if (null != inputRecord) {
      setInputKey(inputRecord.getFirst());
      setInputValue(inputRecord.getSecond());
    } else {
      throw new IllegalArgumentException("null inputRecord in setInput()");
    }
  }

  /**
   * Identical to setInput() but returns self for fluent programming style
   *
   * @param inputRecord
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInput(Pair<K1, V1> inputRecord) {
    setInput(inputRecord);
    return this;
  }

  /**
   * Adds an output (k, v) pair we expect from the Mapper
   *
   * @param outputRecord
   *          The (k, v) pair to add
   */
  public void addOutput(Pair<K2, V2> outputRecord) {
    if (null != outputRecord) {
      expectedOutputs.add(outputRecord);
    } else {
      throw new IllegalArgumentException("Tried to add null outputRecord");
    }
  }

  /**
   * Works like addOutput(), but returns self for fluent style
   *
   * @param outputRecord
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withOutput(Pair<K2, V2> outputRecord) {
    addOutput(outputRecord);
    return this;
  }

  /**
   * Adds a (k, v) pair we expect as output from the mapper
   *
   */
  public void addOutput(K2 key, V2 val) {
    addOutput(new Pair<K2, V2>(key, val));
  }

  /**
   * Functions like addOutput() but returns self for fluent programming
   * style
   *
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withOutput(K2 key, V2 val) {
    addOutput(key, val);
    return this;
  }

  /**
   * Expects an input of the form "key \t val" Forces the Mapper input types
   * to Text.
   *
   * @param input
   *          A string of the form "key \t val".
   */
  public void setInputFromString(String input) {
    if (null == input) {
      throw new IllegalArgumentException("null input given to setInputFromString");
    } else {
      Pair<Text, Text> inputPair = parseTabbedPair(input);
      if (null != inputPair) {
        // I know this is not type-safe, but I don't know a better way to do
        // this.
        setInputKey((K1) inputPair.getFirst());
        setInputValue((V1) inputPair.getSecond());
      } else {
        throw new IllegalArgumentException(
            "Could not parse input pair in setInputFromString");
      }
    }
  }

  /**
   * Identical to setInputFromString, but with a fluent programming style
   *
   * @param input
   *          A string of the form "key \t val". Trims any whitespace.
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInputFromString(String input) {
    setInputFromString(input);
    return this;
  }

  /**
   * Expects an input of the form "key \t val" Forces the Mapper output types
   * to Text.
   *
   * @param output
   *          A string of the form "key \t val". Trims any whitespace.
   */
  public void addOutputFromString(String output) {
    if (null == output) {
      throw new IllegalArgumentException("null input given to setOutput");
    } else {
      Pair<Text, Text> outputPair = parseTabbedPair(output);
      if (null != outputPair) {
        // I know this is not type-safe, but I don't know a better way to do
        // this.
        addOutput((Pair<K2, V2>) outputPair);
      } else {
        throw new IllegalArgumentException("Could not parse output pair in setOutput");
      }
    }
  }

  /**
   * Identical to addOutputFromString, but with a fluent programming style
   *
   * @param output
   *          A string of the form "key \t val". Trims any whitespace.
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withOutputFromString(String output) {
    addOutputFromString(output);
    return this;
  }

  @Override
  public List<Pair<K2, V2>> run() throws IOException {
    MockOutputCollector<K2, V2> outputCollector =
      new MockOutputCollector<K2, V2>();
    MockReporter reporter = new MockReporter(MockReporter.ReporterType.Mapper);

    myMapper.map(inputKey, inputVal, outputCollector, reporter);

    return outputCollector.getOutputs();
  }

  @Override
  public void runTest() throws RuntimeException {
    String inputKeyStr = "(null)";
    String inputValStr = "(null)";

    if (null != inputKey) {
      inputKeyStr = inputKey.toString();
    }

    if (null != inputVal) {
      inputValStr = inputVal.toString();
    }

    LOG.debug("Mapping input (" + inputKeyStr + ", " + inputValStr + ")");

    List<Pair<K2, V2>> outputs = null;

    try {
      outputs = run();
      validate(outputs);
    } catch (IOException ioe) {
      LOG.error("IOException in mapper: " + ioe.toString());
      LOG.debug("Setting success to false based on IOException");
      throw new RuntimeException();
    }
  }

  @Override
  public String toString() {
    return "MapDriver (" + myMapper + ")";
  }
}


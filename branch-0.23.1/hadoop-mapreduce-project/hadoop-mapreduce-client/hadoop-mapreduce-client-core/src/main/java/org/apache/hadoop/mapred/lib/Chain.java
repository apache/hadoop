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
package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.GenericsUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * The Chain class provides all the common functionality for the
 * {@link ChainMapper} and the {@link ChainReducer} classes.
 */
class Chain extends org.apache.hadoop.mapreduce.lib.chain.Chain {

  private static final String MAPPER_BY_VALUE = "chain.mapper.byValue";
  private static final String REDUCER_BY_VALUE = "chain.reducer.byValue";

  private JobConf chainJobConf;

  private List<Mapper> mappers = new ArrayList<Mapper>();
  private Reducer reducer;

  // to cache the key/value output class serializations for each chain element
  // to avoid everytime lookup.
  private List<Serialization> mappersKeySerialization =
    new ArrayList<Serialization>();
  private List<Serialization> mappersValueSerialization =
    new ArrayList<Serialization>();
  private Serialization reducerKeySerialization;
  private Serialization reducerValueSerialization;

  /**
   * Creates a Chain instance configured for a Mapper or a Reducer.
   *
   * @param isMap TRUE indicates the chain is for a Mapper, FALSE that is for a
   *              Reducer.
   */
  Chain(boolean isMap) {
    super(isMap);
  }

  /**
   * Adds a Mapper class to the chain job's JobConf.
   * <p/>
   * The configuration properties of the chain job have precedence over the
   * configuration properties of the Mapper.
   *
   * @param isMap            indicates if the Chain is for a Mapper or for a
   * Reducer.
   * @param jobConf              chain job's JobConf to add the Mapper class.
   * @param klass            the Mapper class to add.
   * @param inputKeyClass    mapper input key class.
   * @param inputValueClass  mapper input value class.
   * @param outputKeyClass   mapper output key class.
   * @param outputValueClass mapper output value class.
   * @param byValue          indicates if key/values should be passed by value
   * to the next Mapper in the chain, if any.
   * @param mapperConf       a JobConf with the configuration for the Mapper
   * class. It is recommended to use a JobConf without default values using the
   * <code>JobConf(boolean loadDefaults)</code> constructor with FALSE.
   */
  public static <K1, V1, K2, V2> void addMapper(boolean isMap, JobConf jobConf,
                           Class<? extends Mapper<K1, V1, K2, V2>> klass,
                           Class<? extends K1> inputKeyClass,
                           Class<? extends V1> inputValueClass,
                           Class<? extends K2> outputKeyClass,
                           Class<? extends V2> outputValueClass,
                           boolean byValue, JobConf mapperConf) {
    String prefix = getPrefix(isMap);

    // if a reducer chain check the Reducer has been already set
    checkReducerAlreadySet(isMap, jobConf, prefix, true);
	    
    // set the mapper class
    int index = getIndex(jobConf, prefix);
    jobConf.setClass(prefix + CHAIN_MAPPER_CLASS + index, klass, Mapper.class);
	    
    validateKeyValueTypes(isMap, jobConf, inputKeyClass, inputValueClass,
      outputKeyClass, outputValueClass, index, prefix);
	    
    // if the Mapper does not have a private JobConf create an empty one
    if (mapperConf == null) {
    // using a JobConf without defaults to make it lightweight.
    // still the chain JobConf may have all defaults and this conf is
    // overlapped to the chain JobConf one.
      mapperConf = new JobConf(true);
    }
    // store in the private mapper conf if it works by value or by reference
    mapperConf.setBoolean(MAPPER_BY_VALUE, byValue);
    
    setMapperConf(isMap, jobConf, inputKeyClass, inputValueClass,
	      outputKeyClass, outputValueClass, mapperConf, index, prefix);
  }

  /**
   * Sets the Reducer class to the chain job's JobConf.
   * <p/>
   * The configuration properties of the chain job have precedence over the
   * configuration properties of the Reducer.
   *
   * @param jobConf              chain job's JobConf to add the Reducer class.
   * @param klass            the Reducer class to add.
   * @param inputKeyClass    reducer input key class.
   * @param inputValueClass  reducer input value class.
   * @param outputKeyClass   reducer output key class.
   * @param outputValueClass reducer output value class.
   * @param byValue          indicates if key/values should be passed by value
   * to the next Mapper in the chain, if any.
   * @param reducerConf      a JobConf with the configuration for the Reducer
   * class. It is recommended to use a JobConf without default values using the
   * <code>JobConf(boolean loadDefaults)</code> constructor with FALSE.
   */
  public static <K1, V1, K2, V2> void setReducer(JobConf jobConf,
                          Class<? extends Reducer<K1, V1, K2, V2>> klass,
                          Class<? extends K1> inputKeyClass,
                          Class<? extends V1> inputValueClass,
                          Class<? extends K2> outputKeyClass,
                          Class<? extends V2> outputValueClass,
                          boolean byValue, JobConf reducerConf) {
    String prefix = getPrefix(false);
    checkReducerAlreadySet(false, jobConf, prefix, false);

    jobConf.setClass(prefix + CHAIN_REDUCER_CLASS, klass, Reducer.class);
    
    // if the Reducer does not have a private JobConf create an empty one
    if (reducerConf == null) {
      // using a JobConf without defaults to make it lightweight.
      // still the chain JobConf may have all defaults and this conf is
      // overlapped to the chain JobConf one.
      reducerConf = new JobConf(false);
    }

    // store in the private reducer conf the input/output classes of the reducer
    // and if it works by value or by reference
    reducerConf.setBoolean(REDUCER_BY_VALUE, byValue);

    setReducerConf(jobConf, inputKeyClass, inputValueClass, outputKeyClass,
      outputValueClass, reducerConf, prefix);
  }

  /**
   * Configures all the chain elements for the task.
   *
   * @param jobConf chain job's JobConf.
   */
  public void configure(JobConf jobConf) {
    String prefix = getPrefix(isMap);
    chainJobConf = jobConf;
    SerializationFactory serializationFactory =
      new SerializationFactory(chainJobConf);
    int index = jobConf.getInt(prefix + CHAIN_MAPPER_SIZE, 0);
    for (int i = 0; i < index; i++) {
      Class<? extends Mapper> klass =
        jobConf.getClass(prefix + CHAIN_MAPPER_CLASS + i, null, Mapper.class);
      JobConf mConf = new JobConf(
        getChainElementConf(jobConf, prefix + CHAIN_MAPPER_CONFIG + i));
      Mapper mapper = ReflectionUtils.newInstance(klass, mConf);
      mappers.add(mapper);

      if (mConf.getBoolean(MAPPER_BY_VALUE, true)) {
        mappersKeySerialization.add(serializationFactory.getSerialization(
          mConf.getClass(MAPPER_OUTPUT_KEY_CLASS, null)));
        mappersValueSerialization.add(serializationFactory.getSerialization(
          mConf.getClass(MAPPER_OUTPUT_VALUE_CLASS, null)));
      } else {
        mappersKeySerialization.add(null);
        mappersValueSerialization.add(null);
      }
    }
    Class<? extends Reducer> klass =
      jobConf.getClass(prefix + CHAIN_REDUCER_CLASS, null, Reducer.class);
    if (klass != null) {
      JobConf rConf = new JobConf(
        getChainElementConf(jobConf, prefix + CHAIN_REDUCER_CONFIG));
      reducer = ReflectionUtils.newInstance(klass, rConf);
      if (rConf.getBoolean(REDUCER_BY_VALUE, true)) {
        reducerKeySerialization = serializationFactory
          .getSerialization(rConf.getClass(REDUCER_OUTPUT_KEY_CLASS, null));
        reducerValueSerialization = serializationFactory
          .getSerialization(rConf.getClass(REDUCER_OUTPUT_VALUE_CLASS, null));
      } else {
        reducerKeySerialization = null;
        reducerValueSerialization = null;
      }
    }
  }

  /**
   * Returns the chain job conf.
   *
   * @return the chain job conf.
   */
  protected JobConf getChainJobConf() {
    return chainJobConf;
  }

  /**
   * Returns the first Mapper instance in the chain.
   *
   * @return the first Mapper instance in the chain or NULL if none.
   */
  public Mapper getFirstMap() {
    return (mappers.size() > 0) ? mappers.get(0) : null;
  }

  /**
   * Returns the Reducer instance in the chain.
   *
   * @return the Reducer instance in the chain or NULL if none.
   */
  public Reducer getReducer() {
    return reducer;
  }

  /**
   * Returns the OutputCollector to be used by a Mapper instance in the chain.
   *
   * @param mapperIndex index of the Mapper instance to get the OutputCollector.
   * @param output      the original OutputCollector of the task.
   * @param reporter    the reporter of the task.
   * @return the OutputCollector to be used in the chain.
   */
  @SuppressWarnings({"unchecked"})
  public OutputCollector getMapperCollector(int mapperIndex,
                                            OutputCollector output,
                                            Reporter reporter) {
    Serialization keySerialization = mappersKeySerialization.get(mapperIndex);
    Serialization valueSerialization =
      mappersValueSerialization.get(mapperIndex);
    return new ChainOutputCollector(mapperIndex, keySerialization,
                                    valueSerialization, output, reporter);
  }

  /**
   * Returns the OutputCollector to be used by a Mapper instance in the chain.
   *
   * @param output   the original OutputCollector of the task.
   * @param reporter the reporter of the task.
   * @return the OutputCollector to be used in the chain.
   */
  @SuppressWarnings({"unchecked"})
  public OutputCollector getReducerCollector(OutputCollector output,
                                             Reporter reporter) {
    return new ChainOutputCollector(reducerKeySerialization,
                                    reducerValueSerialization, output,
                                    reporter);
  }

  /**
   * Closes all the chain elements.
   *
   * @throws IOException thrown if any of the chain elements threw an
   *                     IOException exception.
   */
  public void close() throws IOException {
    for (Mapper map : mappers) {
      map.close();
    }
    if (reducer != null) {
      reducer.close();
    }
  }

  // using a ThreadLocal to reuse the ByteArrayOutputStream used for ser/deser
  // it has to be a thread local because if not it would break if used from a
  // MultiThreadedMapRunner.
  private ThreadLocal<DataOutputBuffer> threadLocalDataOutputBuffer =
    new ThreadLocal<DataOutputBuffer>() {
      protected DataOutputBuffer initialValue() {
        return new DataOutputBuffer(1024);
      }
    };

  /**
   * OutputCollector implementation used by the chain tasks.
   * <p/>
   * If it is not the end of the chain, a {@link #collect} invocation invokes
   * the next Mapper in the chain. If it is the end of the chain the task
   * OutputCollector is called.
   */
  private class ChainOutputCollector<K, V> implements OutputCollector<K, V> {
    private int nextMapperIndex;
    private Serialization<K> keySerialization;
    private Serialization<V> valueSerialization;
    private OutputCollector output;
    private Reporter reporter;

    /*
     * Constructor for Mappers
     */
    public ChainOutputCollector(int index, Serialization<K> keySerialization,
                                Serialization<V> valueSerialization,
                                OutputCollector output, Reporter reporter) {
      this.nextMapperIndex = index + 1;
      this.keySerialization = keySerialization;
      this.valueSerialization = valueSerialization;
      this.output = output;
      this.reporter = reporter;
    }

    /*
     * Constructor for Reducer
     */
    public ChainOutputCollector(Serialization<K> keySerialization,
                                Serialization<V> valueSerialization,
                                OutputCollector output, Reporter reporter) {
      this.nextMapperIndex = 0;
      this.keySerialization = keySerialization;
      this.valueSerialization = valueSerialization;
      this.output = output;
      this.reporter = reporter;
    }

    @SuppressWarnings({"unchecked"})
    public void collect(K key, V value) throws IOException {
      if (nextMapperIndex < mappers.size()) {
        // there is a next mapper in chain

        // only need to ser/deser if there is next mapper in the chain
        if (keySerialization != null) {
          key = makeCopyForPassByValue(keySerialization, key);
          value = makeCopyForPassByValue(valueSerialization, value);
        }

        // gets ser/deser and mapper of next in chain
        Serialization nextKeySerialization =
          mappersKeySerialization.get(nextMapperIndex);
        Serialization nextValueSerialization =
          mappersValueSerialization.get(nextMapperIndex);
        Mapper nextMapper = mappers.get(nextMapperIndex);

        // invokes next mapper in chain
        nextMapper.map(key, value,
                       new ChainOutputCollector(nextMapperIndex,
                                                nextKeySerialization,
                                                nextValueSerialization,
                                                output, reporter),
                       reporter);
      } else {
        // end of chain, user real output collector
        output.collect(key, value);
      }
    }

    private <E> E makeCopyForPassByValue(Serialization<E> serialization,
                                          E obj) throws IOException {
      Serializer<E> ser =
        serialization.getSerializer(GenericsUtil.getClass(obj));
      Deserializer<E> deser =
        serialization.getDeserializer(GenericsUtil.getClass(obj));

      DataOutputBuffer dof = threadLocalDataOutputBuffer.get();

      dof.reset();
      ser.open(dof);
      ser.serialize(obj);
      ser.close();
      obj = ReflectionUtils.newInstance(GenericsUtil.getClass(obj),
                                        getChainJobConf());
      ByteArrayInputStream bais =
        new ByteArrayInputStream(dof.getData(), 0, dof.getLength());
      deser.open(bais);
      deser.deserialize(obj);
      deser.close();
      return obj;
    }

  }

}

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
package org.apache.hadoop.io.erasurecode;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeXORRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureCoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class registers all coder implementations.
 *
 * {@link CodecRegistry} maps codec names to coder factories. All coder
 * factories are dynamically identified and loaded using ServiceLoader.
 */
@InterfaceAudience.Private
public final class CodecRegistry {

  private static final Logger LOG =
      LoggerFactory.getLogger(CodecRegistry.class);

  private static CodecRegistry instance = new CodecRegistry();

  public static CodecRegistry getInstance() {
    return instance;
  }

  private Map<String, List<RawErasureCoderFactory>> coderMap;

  private Map<String, String[]> coderNameMap;

  // Protobuffer 2.5.0 doesn't support map<String, String[]> type well, so use
  // the compact value instead
  private HashMap<String, String> coderNameCompactMap;

  private CodecRegistry() {
    coderMap = new HashMap<>();
    coderNameMap = new HashMap<>();
    coderNameCompactMap = new HashMap<>();
    final ServiceLoader<RawErasureCoderFactory> coderFactories =
        ServiceLoader.load(RawErasureCoderFactory.class);
    updateCoders(coderFactories);
  }

  /**
   * Update coderMap and coderNameMap with iterable type of coder factories.
   * @param coderFactories
   */
  @VisibleForTesting
  void updateCoders(Iterable<RawErasureCoderFactory> coderFactories) {
    for (RawErasureCoderFactory coderFactory : coderFactories) {
      String codecName = coderFactory.getCodecName();
      List<RawErasureCoderFactory> coders = coderMap.get(codecName);
      if (coders == null) {
        coders = new ArrayList<>();
        coders.add(coderFactory);
        coderMap.put(codecName, coders);
        LOG.debug("Codec registered: codec = {}, coder = {}",
            coderFactory.getCodecName(), coderFactory.getCoderName());
      } else {
        Boolean hasConflit = false;
        for (RawErasureCoderFactory coder : coders) {
          if (coder.getCoderName().equals(coderFactory.getCoderName())) {
            hasConflit = true;
            LOG.error("Coder {} cannot be registered because its coder name " +
                "{} has conflict with {}", coderFactory.getClass().getName(),
                coderFactory.getCoderName(), coder.getClass().getName());
            break;
          }
        }
        if (!hasConflit) {
          // set native coders as default if user does not
          // specify a fallback order
          if (coderFactory instanceof NativeRSRawErasureCoderFactory ||
                  coderFactory instanceof NativeXORRawErasureCoderFactory) {
            coders.add(0, coderFactory);
          } else {
            coders.add(coderFactory);
          }
          LOG.debug("Codec registered: codec = {}, coder = {}",
              coderFactory.getCodecName(), coderFactory.getCoderName());
        }
      }
    }

    // update coderNameMap accordingly
    coderNameMap.clear();
    for (Map.Entry<String, List<RawErasureCoderFactory>> entry :
        coderMap.entrySet()) {
      String codecName = entry.getKey();
      List<RawErasureCoderFactory> coders = entry.getValue();
      coderNameMap.put(codecName, coders.stream().
          map(RawErasureCoderFactory::getCoderName).
          collect(Collectors.toList()).toArray(new String[0]));
      coderNameCompactMap.put(codecName, coders.stream().
          map(RawErasureCoderFactory::getCoderName)
          .collect(Collectors.joining(", ")));
    }
  }

  /**
   * Get all coder names of the given codec.
   * @param codecName the name of codec
   * @return an array of all coder names, null if not exist
   */
  public String[] getCoderNames(String codecName) {
    String[] coderNames = coderNameMap.get(codecName);
    return coderNames;
  }

  /**
   * Get all coder factories of the given codec.
   * @param codecName the name of codec
   * @return a list of all coder factories, null if not exist
   */
  public List<RawErasureCoderFactory> getCoders(String codecName) {
    List<RawErasureCoderFactory> coders = coderMap.get(codecName);
    return coders;
  }

  /**
   * Get all codec names.
   * @return a set of all codec names
   */
  public Set<String> getCodecNames() {
    return coderMap.keySet();
  }

  /**
   * Get a specific coder factory defined by codec name and coder name.
   * @param codecName name of the codec
   * @param coderName name of the coder
   * @return the specific coder, null if not exist
   */
  public RawErasureCoderFactory getCoderByName(
      String codecName, String coderName) {
    List<RawErasureCoderFactory> coders = getCoders(codecName);

    // find the RawErasureCoderFactory with the name of coderName
    for (RawErasureCoderFactory coder : coders) {
      if (coder.getCoderName().equals(coderName)) {
        return coder;
      }
    }
    return null;
  }

  /**
   * Get all codec names and their corresponding coder list.
   * @return a map of all codec names, and their corresponding code list
   * separated by ','.
   */
  public Map<String, String> getCodec2CoderCompactMap() {
    return coderNameCompactMap;
  }
}

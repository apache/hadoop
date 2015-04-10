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

import java.util.Collections;
import java.util.Map;

/**
 * Erasure coding schema to housekeeper relevant information.
 */
public final class ECSchema {
  public static final String NUM_DATA_UNITS_KEY = "k";
  public static final String NUM_PARITY_UNITS_KEY = "m";
  public static final String CODEC_NAME_KEY = "codec";
  public static final String CHUNK_SIZE_KEY = "chunkSize";
  public static final int DEFAULT_CHUNK_SIZE = 256 * 1024; // 256K

  private String schemaName;
  private String codecName;
  private Map<String, String> options;
  private int numDataUnits;
  private int numParityUnits;
  private int chunkSize;

  /**
   * Constructor with schema name and provided options. Note the options may
   * contain additional information for the erasure codec to interpret further.
   * @param schemaName schema name
   * @param options schema options
   */
  public ECSchema(String schemaName, Map<String, String> options) {
    assert (schemaName != null && ! schemaName.isEmpty());

    this.schemaName = schemaName;

    if (options == null || options.isEmpty()) {
      throw new IllegalArgumentException("No schema options are provided");
    }

    String codecName = options.get(CODEC_NAME_KEY);
    if (codecName == null || codecName.isEmpty()) {
      throw new IllegalArgumentException("No codec option is provided");
    }

    int dataUnits = 0, parityUnits = 0;
    try {
      if (options.containsKey(NUM_DATA_UNITS_KEY)) {
        dataUnits = Integer.parseInt(options.get(NUM_DATA_UNITS_KEY));
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Option value " +
          options.get(NUM_DATA_UNITS_KEY) + " for " + NUM_DATA_UNITS_KEY +
          " is found. It should be an integer");
    }

    try {
      if (options.containsKey(NUM_PARITY_UNITS_KEY)) {
        parityUnits = Integer.parseInt(options.get(NUM_PARITY_UNITS_KEY));
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Option value " +
          options.get(NUM_PARITY_UNITS_KEY) + " for " + NUM_PARITY_UNITS_KEY +
          " is found. It should be an integer");
    }

    initWith(codecName, dataUnits, parityUnits, options);
  }

  /**
   * Constructor with key parameters provided.
   * @param schemaName
   * @param codecName
   * @param numDataUnits
   * @param numParityUnits
   */
  public ECSchema(String schemaName, String codecName,
                  int numDataUnits, int numParityUnits) {
    this(schemaName, codecName, numDataUnits, numParityUnits, null);
  }

  /**
   * Constructor with key parameters provided. Note the options may contain
   * additional information for the erasure codec to interpret further.
   * @param schemaName
   * @param codecName
   * @param numDataUnits
   * @param numParityUnits
   * @param options
   */
  public ECSchema(String schemaName, String codecName,
                  int numDataUnits, int numParityUnits,
                  Map<String, String> options) {
    assert (schemaName != null && ! schemaName.isEmpty());
    assert (codecName != null && ! codecName.isEmpty());

    this.schemaName = schemaName;
    initWith(codecName, numDataUnits, numParityUnits, options);
  }

  private void initWith(String codecName, int numDataUnits, int numParityUnits,
                        Map<String, String> options) {
    this.codecName = codecName;
    this.numDataUnits = numDataUnits;
    this.numParityUnits = numParityUnits;

    this.options = options != null ? Collections.unmodifiableMap(options) :
        Collections.EMPTY_MAP;

    this.chunkSize = DEFAULT_CHUNK_SIZE;
    try {
      if (this.options.containsKey(CHUNK_SIZE_KEY)) {
        this.chunkSize = Integer.parseInt(options.get(CHUNK_SIZE_KEY));
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Option value " +
          this.options.get(CHUNK_SIZE_KEY) + " for " + CHUNK_SIZE_KEY +
          " is found. It should be an integer");
    }

    boolean isFine = numDataUnits > 0 && numParityUnits > 0 && chunkSize > 0;
    if (! isFine) {
      throw new IllegalArgumentException("Bad codec options are found");
    }
  }

  /**
   * Get the schema name
   * @return schema name
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * Get the codec name
   * @return codec name
   */
  public String getCodecName() {
    return codecName;
  }

  /**
   * Get erasure coding options
   * @return encoding options
   */
  public Map<String, String> getOptions() {
    return options;
  }

  /**
   * Get required data units count in a coding group
   * @return count of data units
   */
  public int getNumDataUnits() {
    return numDataUnits;
  }

  /**
   * Get required parity units count in a coding group
   * @return count of parity units
   */
  public int getNumParityUnits() {
    return numParityUnits;
  }

  /**
   * Get chunk buffer size for the erasure encoding/decoding.
   * @return chunk buffer size
   */
  public int getChunkSize() {
    return chunkSize;
  }

  /**
   * Make a meaningful string representation for log output.
   * @return string representation
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ECSchema=[");

    sb.append("Name=" + schemaName + ",");
    sb.append(NUM_DATA_UNITS_KEY + "=" + numDataUnits + ",");
    sb.append(NUM_PARITY_UNITS_KEY + "=" + numParityUnits + ",");
    sb.append(CHUNK_SIZE_KEY + "=" + chunkSize + ",");

    for (String opt : options.keySet()) {
      boolean skip = (opt.equals(NUM_DATA_UNITS_KEY) ||
          opt.equals(NUM_PARITY_UNITS_KEY) ||
          opt.equals(CHUNK_SIZE_KEY));
      if (! skip) {
        sb.append(opt + "=" + options.get(opt) + ",");
      }
    }

    sb.append("]");

    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ECSchema ecSchema = (ECSchema) o;

    if (numDataUnits != ecSchema.numDataUnits) {
      return false;
    }
    if (numParityUnits != ecSchema.numParityUnits) {
      return false;
    }
    if (chunkSize != ecSchema.chunkSize) {
      return false;
    }
    if (!schemaName.equals(ecSchema.schemaName)) {
      return false;
    }
    if (!codecName.equals(ecSchema.codecName)) {
      return false;
    }
    return options.equals(ecSchema.options);
  }

  @Override
  public int hashCode() {
    int result = schemaName.hashCode();
    result = 31 * result + codecName.hashCode();
    result = 31 * result + options.hashCode();
    result = 31 * result + numDataUnits;
    result = 31 * result + numParityUnits;
    result = 31 * result + chunkSize;

    return result;
  }
}

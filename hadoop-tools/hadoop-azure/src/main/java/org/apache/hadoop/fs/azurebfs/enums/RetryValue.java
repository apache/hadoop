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

package org.apache.hadoop.fs.azurebfs.enums;

import org.apache.hadoop.fs.azurebfs.services.AbfsBackoffMetrics;

/**
 * Enum for retry values.
 * Used in {@link AbfsBackoffMetrics} to store metrics based on the retry count.
 */
public enum RetryValue {
    ONE("1"),
    TWO("2"),
    THREE("3"),
    FOUR("4"),
    FIVE_FIFTEEN("5_15"),
    FIFTEEN_TWENTY_FIVE("15_25"),
    TWENTY_FIVE_AND_ABOVE("25AndAbove");

    private static final int FIVE = 5;
    private static final int FIFTEEN = 15;
    private static final int TWENTY_FIVE = 25;

    private final String value;

    /**
     * Constructor for RetryValue enum.
     *
     * @param value the string representation of the retry value
     */
    RetryValue(String value) {
        this.value = value;
    }

    /**
     * Gets the string representation of the retry value.
     *
     * @return the string representation of the retry value
     */
    public String getValue() {
        return value;
    }

    /**
     * Gets the RetryValue enum based on the retry count.
     *
     * @param retryCount the retry count
     * @return the corresponding RetryValue enum
     */
    public static RetryValue getRetryValue(int retryCount) {
        if (retryCount == 1) {
            return ONE;
        } else if (retryCount == 2) {
            return TWO;
        } else if (retryCount == 3) {
            return THREE;
        } else if (retryCount == 4) {
            return FOUR;
        } else if (retryCount >= FIVE && retryCount < FIFTEEN) {
            return FIVE_FIFTEEN;
        } else if (retryCount >= FIFTEEN && retryCount < TWENTY_FIVE) {
            return FIFTEEN_TWENTY_FIVE;
        } else {
            return TWENTY_FIVE_AND_ABOVE;
        }
    }
}

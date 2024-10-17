package org.apache.hadoop.fs.azurebfs.enums;

public enum RetryValue {
    ONE("1"),
    TWO("2"),
    THREE("3"),
    FOUR("4"),
    FIVE_FIFTEEN("5_15"),
    FIFTEEN_TWENTY_FIVE("15_25"),
    TWENTY_FIVE_AND_ABOVE("25AndAbove");

    private final String value;

    RetryValue(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static RetryValue getRetryValue(int retryCount) {
        if (retryCount == 1) {
            return ONE;
        } else if (retryCount == 2) {
            return TWO;
        } else if (retryCount == 3) {
            return THREE;
        } else if (retryCount == 4) {
            return FOUR;
        } else if (retryCount >= 5 && retryCount < 15) {
            return FIVE_FIFTEEN;
        } else if (retryCount >= 15 && retryCount < 25) {
            return FIFTEEN_TWENTY_FIVE;
        } else {
            return TWENTY_FIVE_AND_ABOVE;
        }
    }
}

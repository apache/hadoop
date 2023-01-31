package org.apache.hadoop.fs.qiniu.kodo.config;

public class MissingConfigFieldException extends Exception {
    public MissingConfigFieldException(String key) {
        super("miss config field on key: " + key);
    }
}
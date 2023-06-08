package org.apache.hadoop.fs.qiniu.kodo.client;


public class QiniuKodoFileInfo {
    public final String key;
    public final long size;
    /**
     * File upload time, in units of 1 millisecond
     */
    public final long putTime;


    public QiniuKodoFileInfo(String key, long size, long putTime) {
        this.key = key;
        this.size = size;
        this.putTime = putTime;
    }
}

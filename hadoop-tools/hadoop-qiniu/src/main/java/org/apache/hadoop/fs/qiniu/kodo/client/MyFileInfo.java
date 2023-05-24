package org.apache.hadoop.fs.qiniu.kodo.client;


public class MyFileInfo {
    public final String key;
    public final long size;
    public final long putTime;


    public MyFileInfo(String key, long size, long putTime) {
        this.key = key;
        this.size = size;
        this.putTime = putTime;
    }
}

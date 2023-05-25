package org.apache.hadoop.fs.qiniu.kodo.client;


public class QiniuKodoFileInfo {
    public final String key;
    public final long size;
    // 文件的上传时间，单位为1毫秒
    public final long putTime;


    public QiniuKodoFileInfo(String key, long size, long putTime) {
        this.key = key;
        this.size = size;
        this.putTime = putTime;
    }
}

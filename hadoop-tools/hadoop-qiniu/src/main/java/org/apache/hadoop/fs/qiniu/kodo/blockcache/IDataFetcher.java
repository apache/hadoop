package org.apache.hadoop.fs.qiniu.kodo.blockcache;

public interface IDataFetcher {
    byte[] fetch(String key, long offset, int size);
}
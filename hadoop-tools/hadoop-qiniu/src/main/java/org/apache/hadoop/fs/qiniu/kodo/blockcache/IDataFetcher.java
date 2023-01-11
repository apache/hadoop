package org.apache.hadoop.fs.qiniu.kodo.blockcache;

public interface IDataFetcher {
    byte[] fetch(long from, long to);
}
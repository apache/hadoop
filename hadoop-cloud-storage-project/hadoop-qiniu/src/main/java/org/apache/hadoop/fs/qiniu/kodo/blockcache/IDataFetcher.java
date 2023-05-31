package org.apache.hadoop.fs.qiniu.kodo.blockcache;

import java.io.IOException;

public interface IDataFetcher {
    byte[] fetch(String key, long offset, int size) throws IOException;
}
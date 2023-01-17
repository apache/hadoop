package org.apache.hadoop.fs.qiniu.kodo.blockcache;

import java.io.Closeable;

public interface IBlockReader extends Closeable {
    int getBlockSize();
    byte[] readBlock(String key, int blockId);
}

package org.apache.hadoop.fs.qiniu.kodo.blockcache;

public interface IBlockReader {
    int getBlockSize();
    byte[] readBlock(String key, int blockId);
}

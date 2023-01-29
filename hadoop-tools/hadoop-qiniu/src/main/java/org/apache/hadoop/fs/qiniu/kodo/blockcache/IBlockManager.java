package org.apache.hadoop.fs.qiniu.kodo.blockcache;

public interface IBlockManager {
    // 删除该存储块
    void deleteBlocks(String key);
}

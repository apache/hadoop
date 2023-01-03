package org.apache.hadoop.fs.qiniu.kodo;

public class Constants {
    public static final String SCHEME = "kodo";

    /**
     * Qiniu access key
     */
    public static final String QINIU_PARAMETER_ACCESS_KEY = "fs.qiniu.access.key";

    /**
     * Qiniu secret key
     */
    public static final String QINIU_PARAMETER_SECRET_KEY = "fs.qiniu.secret.key";

    /**
     * Qiniu region id,
     */
    public static final String QINIU_PARAMETER_REGION_ID = "fs.qiniu.region.id";


    /**
     * 默认副本数量
     */
    static final int QINIU_DEFAULT_VALUE_BLOCK_REPLICATION = 3;

    /**
     * 默认块大小
     */
    static final int QINIU_DEFAULT_VALUE_BLOCK_SIZE = 4 * 1024 * 1024;

    /**
     * Qiniu 存储块大小,
     */
    public static final String QINIU_PARAMETER_BLOCK_SIZE = "fs.qiniu.block.size";
}

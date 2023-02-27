package org.apache.hadoop.fs.qiniu.kodo.client.batch;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import org.apache.hadoop.fs.qiniu.kodo.util.QiniuKodoUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ListingProducer implements Callable<Exception> {
    private final BlockingQueue<FileInfo> queue;
    private final BucketManager bucketManager;
    private final String keyPrefix;
    private final String bucketName;

    private final int singleRequestLimit;
    private final boolean useDirectory;
    private final boolean useV2;
    private final long offerTimeout;
    private final boolean containKeyPrefixSelf;

    /**
     * 对象列举生产者
     *
     * @param queue              列举结果放入消息队列
     * @param bucketManager      bucketManager
     * @param bucketName         列举的bucket名称
     * @param keyPrefix          列举的key前缀
     * @param singleRequestLimit 单次请求获取最大限制的列举数目
     * @param useDirectory       是否使用文件夹目录结构，将会列举出文件夹结构
     */
    public ListingProducer(
            BlockingQueue<FileInfo> queue,
            BucketManager bucketManager,
            String bucketName,
            String keyPrefix,
            boolean containKeyPrefixSelf,
            int singleRequestLimit,
            boolean useDirectory,
            boolean useV2,
            long offerTimeout
    ) {
        this.queue = queue;
        this.bucketManager = bucketManager;
        this.keyPrefix = keyPrefix;
        this.bucketName = bucketName;
        this.singleRequestLimit = singleRequestLimit;
        this.useDirectory = useDirectory;
        this.useV2 = useV2;
        this.offerTimeout = offerTimeout;
        this.containKeyPrefixSelf = containKeyPrefixSelf;
    }


    private FileListing listFiles(String marker) throws QiniuException {
        if (useV2) {
            return bucketManager.listFilesV2(bucketName,
                    keyPrefix,
                    marker,
                    singleRequestLimit,
                    useDirectory ? QiniuKodoUtils.PATH_SEPARATOR : ""
            );
        } else {
            return bucketManager.listFiles(
                    bucketName,
                    keyPrefix,
                    marker,
                    singleRequestLimit,
                    useDirectory ? QiniuKodoUtils.PATH_SEPARATOR : ""
            );
        }

    }

    private void offer(FileInfo file) throws InterruptedException {
        boolean success;
        do {
            success = queue.offer(file, offerTimeout, TimeUnit.MILLISECONDS);
        } while (!success);
    }

    private void list() throws QiniuException, InterruptedException {
        FileListing fileListing;
        String marker = null;

        do {
            fileListing = listFiles(marker);
            if (fileListing.items != null) {
                for (FileInfo file : fileListing.items) {
                    if (containKeyPrefixSelf) {
                        offer(file);
                    } else {
                        if (!file.key.equals(keyPrefix)) {
                            offer(file);
                        }
                    }
                }
            }

            if (fileListing.commonPrefixes != null) {
                for (String dirPath : fileListing.commonPrefixes) {
                    FileInfo dir = new FileInfo();
                    dir.key = dirPath;

                    if (containKeyPrefixSelf) {
                        offer(dir);
                    } else {
                        if (!dir.key.equals(keyPrefix)) {
                            offer(dir);
                        }
                    }
                }
            }

            marker = fileListing.marker;
        } while (!fileListing.isEOF());
    }

    @Override
    public Exception call() {
        try {
            list();
            return null;
        } catch (Exception e) {
            return e;
        }
    }
}

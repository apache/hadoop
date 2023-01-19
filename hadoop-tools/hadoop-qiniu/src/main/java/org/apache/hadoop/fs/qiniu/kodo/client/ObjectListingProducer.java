package org.apache.hadoop.fs.qiniu.kodo.client;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ObjectListingProducer implements Runnable{
    private final BlockingQueue<FileInfo> queue;
    private final BucketManager bucketManager;
    private final String keyPrefix;
    private final String bucketName;
    private final boolean containsSelf;
    private final int singleRequestLimit;

    public ObjectListingProducer(
            BlockingQueue<FileInfo> queue,
            BucketManager bucketManager,
            String bucketName,
            String keyPrefix,
            boolean containsSelf,
            int singleRequestLimit) {
        this.queue = queue;
        this.bucketManager = bucketManager;
        this.keyPrefix = keyPrefix;
        this.bucketName = bucketName;
        this.containsSelf = containsSelf;
        this.singleRequestLimit = singleRequestLimit;
    }


    private void list() throws QiniuException {
        FileListing fileListing;
        String marker = null;

        do {
            fileListing = bucketManager.listFilesV2(bucketName, keyPrefix, marker, singleRequestLimit, "");
            if (fileListing.items == null) {
                marker = fileListing.marker;
                continue;
            }
            for (FileInfo file : fileListing.items) {
                if (!containsSelf && file.key.equals(keyPrefix)) {
                    continue;
                }

                try{
                    boolean success;
                    do{
                        success = queue.offer(file, 1, TimeUnit.SECONDS);
                    } while (!success);
                }catch (InterruptedException e) {
                    break;
                }

            }
            marker = fileListing.marker;
        } while (!fileListing.isEOF());
    }

    @Override
    public void run() {
        try {
            list();
        } catch (QiniuException e) {
            throw new RuntimeException(e);
        }
    }
}

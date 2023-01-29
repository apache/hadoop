package org.apache.hadoop.fs.qiniu.kodo.upload;

import com.qiniu.common.QiniuException;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class QiniuKodoOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoOutputStream.class);

    private final String key;
    private final PipedOutputStream pos;
    private final PipedInputStream pis;

    private final Thread thread;

    private volatile QiniuException uploadException;

    private final IBlockManager blockManager;

    public QiniuKodoOutputStream(QiniuKodoClient client, String key, boolean overwrite, IBlockManager blockManager) {
        this.key = key;
        this.blockManager = blockManager;
        this.pos = new PipedOutputStream();
        try {
            this.pis = new PipedInputStream(pos);
            this.thread = new Thread(() -> {
                try {
                    client.upload(pis, key, overwrite);
                } catch (QiniuException e) {
                    this.uploadException = e;
                }
            });
            this.thread.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(int b) throws IOException {
        pos.write(b);
    }

    @Override
    public void close() throws IOException {
        pos.close();
        try {
            thread.join();
            if (uploadException == null) {
                // 无异常退出
                blockManager.deleteBlocks(key); // 上传完毕，清理旧缓存
                return;
            }
            if (uploadException.response.statusCode == 614) {
                throw new FileAlreadyExistsException("key exists " + key);
            }
            throw uploadException;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

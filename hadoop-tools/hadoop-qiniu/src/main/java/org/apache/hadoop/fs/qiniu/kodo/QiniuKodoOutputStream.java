package org.apache.hadoop.fs.qiniu.kodo;

import com.qiniu.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class QiniuKodoOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoOutputStream.class);

    private final String key;
    private final String token;
    private final int bufferSize;

    private volatile boolean isComplete;
    private volatile IOException uploadException;

    private final QiniuKodoUploader uploader;

    QiniuKodoOutputStream(QiniuKodoClient client, String key, String token, int bufferSize) {
        this.isComplete = false;
        this.uploadException = null;
        this.key = key;
        this.token = token;
        this.bufferSize = bufferSize;
        this.uploader = new QiniuKodoUploader(client, bufferSize);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info("create QiniuKodoOutputStream start, key:" + key + " token:" + token + " bufferSize:" + bufferSize);
                    Response response = uploader.upload(key, token);
                    if (response == null) {
                        uploadException = new IOException("qiniu uploader, unknown error");
                    } else if (!response.isOK() && response.statusCode == 612) {
                        uploadException = new IOException(response + "");
                    }

                    if (response != null) {
                        LOG.info("create QiniuKodoOutputStream end01, key:" + key + " token:" + token + " bufferSize:" + bufferSize + " response:" + response);
                    } else {
                        LOG.info("create QiniuKodoOutputStream end01, key:" + key + " token:" + token + " bufferSize:" + bufferSize + " exception:" + uploadException);
                    }
                    isComplete = true;
                } catch (IOException e) {
                    uploadException = e;
                    isComplete = true;
                    LOG.info("create QiniuKodoOutputStream end02, key:" + key + " token:" + token + " bufferSize:" + bufferSize + " exception:" + uploadException);
                }
            }
        }).start();
    }

    @Override
    public void write(int b) throws IOException {
        uploader.write(b);
    }

    @Override
    public void close() throws IOException {
        super.close();
        uploader.close();
        LOG.info("create QiniuKodoOutputStream close start, key:" + key + " token:" + token + " bufferSize:" + bufferSize + " exception:" + uploadException);

        // 等待数据上传
        for (; ; ) {
            if (isComplete) {
                break;
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        LOG.info("create QiniuKodoOutputStream close end, key:" + key + " token:" + token + " bufferSize:" + bufferSize + " exception:" + uploadException);

        if (uploadException != null) {
            throw uploadException;
        }
    }


    private static class QiniuKodoUploader extends InputStream {

        private final QiniuKodoBuffer buffer;

        private final QiniuKodoClient client;

        QiniuKodoUploader(QiniuKodoClient client, int bufferSize) {
            this.client = client;
            this.buffer = new QiniuKodoBuffer(bufferSize);
        }

        Response upload(String key, String token) throws IOException {
            return client.upload(this, key, token);
        }

        void write(int b) throws IOException {
            buffer.write(b);
        }

        @Override
        public void close() throws IOException {
            super.close();
            buffer.close();
        }

        @Override
        public int read() throws IOException {
            return buffer.read();
        }
    }
}

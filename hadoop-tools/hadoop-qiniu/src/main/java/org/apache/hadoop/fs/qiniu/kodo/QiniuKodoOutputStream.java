package org.apache.hadoop.fs.qiniu.kodo;

import com.qiniu.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public class QiniuKodoOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoOutputStream.class);

    private static class Uploader {
        private final String key;
        private final String token;

        private final QiniuKodoClient client;

        public Uploader(QiniuKodoClient client, String key, String token) {
            this.key = key;
            this.token = token;
            this.client = client;
        }

        void upload(InputStream is) throws IOException {
            LOG.debug("create QiniuKodoOutputStream start, key:" + key + " token:" + token);
            Response response = client.upload(is, key, token);

            if (response != null) {
                LOG.debug("create QiniuKodoOutputStream end01, key:" + key + " token:" + token + " response:" + response);
                throw new IOException(response + "");
            } else {
                LOG.debug("create QiniuKodoOutputStream end01, key:" + key + " token:" + token);
                throw new IOException("qiniu uploader, unknown error");
            }
        }

        void upload(byte[] bs) throws IOException {
            LOG.debug(" == Upload file {}", key);
            client.upload(bs, key, token);
        }
    }

    private final Uploader uploader;
    private final byte[] buffer;
    private int bufferPos = 0;

    public QiniuKodoOutputStream(QiniuKodoClient client, String key, String token, int bufferSize) {
        this.uploader = new Uploader(client, key, token);
        this.buffer = new byte[bufferSize];
    }

    @Override
    public void write(int b) throws IOException {
        try {
            buffer[bufferPos++] = (byte) b;
        }catch (ArrayIndexOutOfBoundsException e) {
            throw new IOException("Buffer overflow!!!");
        }
    }

    @Override
    public void close() throws IOException {
        byte[] bs = Arrays.copyOfRange(buffer, 0, bufferPos);
        this.uploader.upload(bs);
    }
}

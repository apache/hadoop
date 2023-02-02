package org.apache.hadoop.fs.qinu.kodo.performance.openfile;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;

public class OpenBigFileCommonUtil {
    public static Path makeSureExistsBigFile(String testDir, FileSystem fs, int blockSize, int blocks) throws IOException {
        byte[] bs = new byte[blockSize];

        Path p = new Path(testDir + "/bigFile");

        try {
            FileStatus stat = fs.getFileStatus(p);
            // 文件存在且大小正确
            if (stat.getLen() == (long) blocks * (long) blockSize) {
                return p;
            }
        } catch (FileNotFoundException ignored) {
        }
        // 建立父目录
        fs.mkdirs(p.getParent());

        // 目标文件路径
        FSDataOutputStream fos = fs.create(p);
        for (int i = 0; i < blocks; i++) {
            fos.write(bs);
        }
        fos.close();
        return p;
    }

}

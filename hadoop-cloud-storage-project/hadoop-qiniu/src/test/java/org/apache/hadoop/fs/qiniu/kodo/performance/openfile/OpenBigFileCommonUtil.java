package org.apache.hadoop.fs.qiniu.kodo.performance.openfile;

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
            if (stat.getLen() == (long) blocks * (long) blockSize) {
                return p;
            }
        } catch (FileNotFoundException ignored) {
        }
        fs.mkdirs(p.getParent());

        FSDataOutputStream fos = fs.create(p);
        for (int i = 0; i < blocks; i++) {
            fos.write(bs);
        }
        fos.close();
        return p;
    }

}

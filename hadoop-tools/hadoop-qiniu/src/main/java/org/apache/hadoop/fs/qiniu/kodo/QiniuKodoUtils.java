package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;


public final class QiniuKodoUtils {

    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoUtils.class);

    public static final String PATH_SEPARATOR = "/";

    /**
     * 对象存储非文件系统，对象存储需要通过key来定位文件，文件系统需要通过path来定位文件
     * 故为了基于对象存储模拟文件系统，需要进行路径的转换
     * 如 /p1/c1 需要转换成 p1/c1
     * hadoop 允许用户使用相对路径，相对路径将基于 workingDir 指定的路径
     */
    public static String pathToKey(Path workingDir, Path path) {
        if (!path.isAbsolute()) {
            path = new Path(workingDir, path);
        }
        return path.toUri().getPath().substring(1);
    }

    /**
     * 将对象存储中的 key 还原为一个 hadoop 文件系统中的绝对路径
     */
    public static Path keyToPath(URI uri, Path workingDir, String key) {
        return new Path(PATH_SEPARATOR + key).makeQualified(uri, workingDir);
    }

    /**
     * 去除一个字符串的尾部路径分隔符，对象存储没有文件夹的概念，文件夹属于文件系统的概念
     * 为了模拟文件夹的存在，需要创建一个以 / 结尾的文件来表明其存在性
     * 该函数的作用为将 key 标准化成表示文件的 key，即末尾必定不存在路径分隔符
     */
    public static String keyToFileKey(String key) {
        if (key == null || !key.endsWith(PATH_SEPARATOR)) {
            return key;
        }
        return key.substring(0, key.length() - PATH_SEPARATOR.length());
    }

    /**
     * 该函数的作用为将 key 标准化成表示文件夹的 key，即末尾必定存在路径分隔符
     */
    public static String keyToDirKey(String key) {
        if (key == null || key.length() == 0 || key.endsWith(PATH_SEPARATOR)) {
            return key;
        }
        return key + PATH_SEPARATOR;
    }

    /**
     * 判断一个key是不是文件夹，只需要判断其是否以 / 字符结尾
     */
    public static boolean isKeyDir(String key) {
        if (key == null || key.length() == 0) {
            return false;
        }

        return key.endsWith(PATH_SEPARATOR);
    }
}

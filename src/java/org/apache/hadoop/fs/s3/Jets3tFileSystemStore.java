package org.apache.hadoop.fs.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.INode.FileType;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

class Jets3tFileSystemStore implements FileSystemStore {

  private static final String PATH_DELIMITER = urlEncode(Path.SEPARATOR);
  private static final String BLOCK_PREFIX = "block_";

  private S3Service s3Service;

  private S3Bucket bucket;

  public void initialize(URI uri, Configuration conf) throws IOException {
    try {
      String accessKey = null;
      String secretAccessKey = null;
      String userInfo = uri.getUserInfo();
      if (userInfo != null) {
          int index = userInfo.indexOf(':');
          if (index != -1) {
	          accessKey = userInfo.substring(0, index);
	          secretAccessKey = userInfo.substring(index + 1);
          } else {
        	  accessKey = userInfo;
          }
      }
      if (accessKey == null) {
    	  accessKey = conf.get("fs.s3.awsAccessKeyId");
      }
      if (secretAccessKey == null) {
    	  secretAccessKey = conf.get("fs.s3.awsSecretAccessKey");
      }
      if (accessKey == null && secretAccessKey == null) {
    	  throw new IllegalArgumentException("AWS " +
    	  		"Access Key ID and Secret Access Key " +
    	  		"must be specified as the username " +
    	  		"or password (respectively) of a s3 URL, " +
    	  		"or by setting the " +
	  		    "fs.s3.awsAccessKeyId or " +    	  		
    	  		"fs.s3.awsSecretAccessKey properties (respectively).");
      } else if (accessKey == null) {
    	  throw new IllegalArgumentException("AWS " +
      	  		"Access Key ID must be specified " +
      	  		"as the username of a s3 URL, or by setting the " +
      	  		"fs.s3.awsAccessKeyId property.");
      } else if (secretAccessKey == null) {
    	  throw new IllegalArgumentException("AWS " +
    	  		"Secret Access Key must be specified " +
    	  		"as the password of a s3 URL, or by setting the " +
    	  		"fs.s3.awsSecretAccessKey property.");    	  
      }
      AWSCredentials awsCredentials = new AWSCredentials(accessKey, secretAccessKey);
      this.s3Service = new RestS3Service(awsCredentials);
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
    bucket = new S3Bucket(uri.getHost());

    createBucket(bucket.getName());
  }  

  private void createBucket(String bucketName) throws IOException {
    try {
      s3Service.createBucket(bucketName);
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  private void delete(String key) throws IOException {
    try {
      s3Service.deleteObject(bucket, key);
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  public void deleteINode(Path path) throws IOException {
    delete(pathToKey(path));
  }

  public void deleteBlock(Block block) throws IOException {
    delete(blockToKey(block));
  }

  public boolean inodeExists(Path path) throws IOException {
    InputStream in = get(pathToKey(path));
    if (in == null) {
      return false;
    }
    in.close();
    return true;
  }
  
  public boolean blockExists(long blockId) throws IOException {
    InputStream in = get(blockToKey(blockId));
    if (in == null) {
      return false;
    }
    in.close();
    return true;
  }

  private InputStream get(String key) throws IOException {
    try {
      S3Object object = s3Service.getObject(bucket, key);
      return object.getDataInputStream();
    } catch (S3ServiceException e) {
      if (e.getS3ErrorCode().equals("NoSuchKey")) {
        return null;
      }
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  private InputStream get(String key, long byteRangeStart) throws IOException {
    try {
      S3Object object = s3Service.getObject(bucket, key, null, null, null,
          null, byteRangeStart, null);
      return object.getDataInputStream();
    } catch (S3ServiceException e) {
      if (e.getS3ErrorCode().equals("NoSuchKey")) {
        return null;
      }
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  public INode getINode(Path path) throws IOException {
    return INode.deserialize(get(pathToKey(path)));
  }

  public InputStream getBlockStream(Block block, long byteRangeStart)
      throws IOException {
    return get(blockToKey(block), byteRangeStart);
  }

  public Set<Path> listSubPaths(Path path) throws IOException {
    try {
      String prefix = pathToKey(path);
      if (!prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }
      S3Object[] objects = s3Service.listObjects(bucket, prefix, PATH_DELIMITER, 0);
      Set<Path> prefixes = new TreeSet<Path>();
      for (int i = 0; i < objects.length; i++) {
        prefixes.add(keyToPath(objects[i].getKey()));
      }
      prefixes.remove(path);
      return prefixes;
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }
  
  public Set<Path> listDeepSubPaths(Path path) throws IOException {
    try {
      String prefix = pathToKey(path);
      if (!prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }
      S3Object[] objects = s3Service.listObjects(bucket, prefix, null);
      Set<Path> prefixes = new TreeSet<Path>();
      for (int i = 0; i < objects.length; i++) {
        prefixes.add(keyToPath(objects[i].getKey()));
      }
      prefixes.remove(path);
      return prefixes;
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }    
  }

  private void put(String key, InputStream in, long length) throws IOException {
    try {
      S3Object object = new S3Object(key);
      object.setDataInputStream(in);
      object.setContentType("binary/octet-stream");
      object.setContentLength(length);
      s3Service.putObject(bucket, object);
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  public void storeINode(Path path, INode inode) throws IOException {
    put(pathToKey(path), inode.serialize(), inode.getSerializedLength());
  }

  public void storeBlock(Block block, InputStream in) throws IOException {
    put(blockToKey(block), in, block.getLength());
  }

  private String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    return urlEncode(path.toUri().getPath());
  }

  private Path keyToPath(String key) {
    return new Path(urlDecode(key));
  }
  
  private static String urlEncode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // Should never happen since every implementation of the Java Platform
      // is required to support UTF-8.
      // See http://java.sun.com/j2se/1.5.0/docs/api/java/nio/charset/Charset.html
      throw new IllegalStateException(e);
    }
  }
  
  private static String urlDecode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // Should never happen since every implementation of the Java Platform
      // is required to support UTF-8.
      // See http://java.sun.com/j2se/1.5.0/docs/api/java/nio/charset/Charset.html
      throw new IllegalStateException(e);
    }
  }

  private String blockToKey(long blockId) {
    return BLOCK_PREFIX + blockId;
  }

  private String blockToKey(Block block) {
    return blockToKey(block.getId());
  }

  public void purge() throws IOException {
    try {
      S3Object[] objects = s3Service.listObjects(bucket);
      for (int i = 0; i < objects.length; i++) {
        s3Service.deleteObject(bucket, objects[i].getKey());
      }
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
  }

  public void dump() throws IOException {
    StringBuilder sb = new StringBuilder("S3 Filesystem, ");
    sb.append(bucket.getName()).append("\n");
    try {
      S3Object[] objects = s3Service.listObjects(bucket, PATH_DELIMITER, null);
      for (int i = 0; i < objects.length; i++) {
        Path path = keyToPath(objects[i].getKey());
        sb.append(path).append("\n");
        INode m = getINode(path);
        sb.append("\t").append(m.getFileType()).append("\n");
        if (m.getFileType() == FileType.DIRECTORY) {
          continue;
        }
        for (int j = 0; j < m.getBlocks().length; j++) {
          sb.append("\t").append(m.getBlocks()[j]).append("\n");
        }
      }
    } catch (S3ServiceException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new S3Exception(e);
    }
    System.out.println(sb);
  }

}

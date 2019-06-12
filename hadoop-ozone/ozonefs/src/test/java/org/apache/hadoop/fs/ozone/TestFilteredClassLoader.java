package org.apache.hadoop.fs.ozone;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ FilteredClassLoader.class, OzoneFSInputStream.class})
public class TestFilteredClassLoader {
  @Test
  public void testFilteredClassLoader() {
    PowerMockito.mockStatic(System.class);
    when(System.getenv("HADOOP_OZONE_DELEGATED_CLASSES"))
        .thenReturn("org.apache.hadoop.fs.ozone.OzoneFSInputStream");

    ClassLoader currentClassLoader =
      TestFilteredClassLoader.class.getClassLoader();

    List<URL> urls = new ArrayList<>();
    ClassLoader classLoader = new FilteredClassLoader(
      urls.toArray(new URL[0]), currentClassLoader);

    try {
      classLoader.loadClass(
        "org.apache.hadoop.fs.ozone.OzoneFSInputStream");
      ClassLoader expectedClassLoader = OzoneFSInputStream.class.getClassLoader();
      assertEquals( expectedClassLoader, currentClassLoader);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}

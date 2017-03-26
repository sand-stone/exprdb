package exprdb.store;

import java.nio.*;
import java.io.*;
import java.nio.file.*;
import java.lang.reflect.Array;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.google.gson.*;
import java.util.*;
import java.util.stream.*;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

class Utils {
  private static Logger log = LogManager.getLogger(Utils.class);

  public static boolean mkdir(String dir) {
    File d = new File(dir);
    boolean ret = d.exists();
    if(ret && d.isFile())
      throw new RuntimeException("wrong directory:" + dir);
    if(!ret) {
      d.mkdirs();
    }
    return ret;
  }

  public static boolean checkFile(String file) {
    File d = new File(file);
    return d.exists();
  }

  public static void deleteFile(String path) {
    try {
    Path rootPath = Paths.get(path);
    Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
      .sorted(Comparator.reverseOrder())
      .map(Path::toFile)
      .forEach(File::delete);
    } catch(IOException e) {
      log.info(e);
    }
  }

  public static ByteBuffer serialize(Object msg) {
    try {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
           ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(msg);
        oos.close();
        return ByteBuffer.wrap(bos.toByteArray());
      }
    } catch(IOException e) {}
    return null;
  }

  public static Object deserialize(byte[] data) {
    return deserialize(ByteBuffer.wrap(data));
  }

  public static Object deserialize(ByteBuffer bb) {
    byte[] bytes = new byte[bb.remaining()];
    bb.get(bytes);
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
         ObjectInputStream ois = new ObjectInputStream(bis)) {
      return ois.readObject();
    } catch (ClassNotFoundException|IOException ex) {
      log.error("Failed to deserialize: {}", bb, ex);
      throw new RuntimeException("Failed to deserialize ByteBuffer");
    }
  }

  public static int memcmp(final byte[] a, final byte[] b) {
    final int length = Math.min(a.length, b.length);
    if (a == b) {  // Do this after accessing a.length and b.length
      return 0;    // in order to NPE if either a or b is null.
    }
    for (int i = 0; i < length; i++) {
      if (a[i] != b[i]) {
        return (a[i] & 0xFF) - (b[i] & 0xFF);
      }
    }
    return a.length - b.length;
  }

  public static int memcmp(final byte[] a, final byte[] b, int length) {
    for (int i = 0; i < length; i++) {
      if (a[i] != b[i]) {
        return (a[i] & 0xFF) - (b[i] & 0xFF);
      }
    }
    return 0;
  }

  public static int memcmp(final byte[] a, final byte[] b, int off, int length) {
    for (int i = 0; i < length; i++) {
      if (a[i] != b[i+off]) {
        return (a[i] & 0xFF) - (b[i+off] & 0xFF);
      }
    }
    return 0;
  }

}

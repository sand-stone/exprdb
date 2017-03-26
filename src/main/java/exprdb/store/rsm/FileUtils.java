package exprdb.store.rsm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Properties;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * A static class that contains file-related utility methods.
 */
final class FileUtils {
  private static final Logger LOG = LogManager.getLogger(FileUtils.class);

  /**
   * Disables the constructor.
   */
  private FileUtils() {
  }

  /**
   * Atomically writes a long integer to a file.
   *
   * This method writes a long integer to a file by first writing the long
   * integer to a temporary file and then atomically moving it to the
   * destination, overwriting the destination file if it already exists.
   *
   * @param value a long integer value to write.
   * @param file file to write the value to.
   * @throws IOException if an I/O error occurs.
   */
  public static void writeLongToFile(long value, File file) throws IOException {
    // Create a temp file in the same directory as the file parameter.
    File temp = File.createTempFile(file.getName(), null,
                                    file.getAbsoluteFile().getParentFile());
    try (FileOutputStream fos = new FileOutputStream(temp);
         OutputStreamWriter os =
          new OutputStreamWriter(fos, Charset.forName("UTF-8"));
         PrintWriter pw = new PrintWriter(os)) {
      pw.print(Long.toString(value));
      fos.getChannel().force(true);
    }
    atomicMove(temp, file);
    LOG.debug("Atomically moved {} to {}", temp, file);
  }

  /**
   * Reads a long integer from a file that was created by the
   * {@link #writeLongToFile(long, File) writeIntToFile} method.
   *
   * @param file file to read the integer value from.
   * @return the long integer value in the file
   * @throws IOException if an I/O error occurs.
   */
  public static long readLongFromFile(File file) throws IOException {
    try (FileInputStream fis = new FileInputStream(file);
         BufferedReader br = new BufferedReader(
           new InputStreamReader(fis, Charset.forName("UTF-8")))) {
      long value = Long.parseLong(br.readLine());
      return value;
    }
  }

  /**
   * Atomically writes properties to a file.
   *
   * This method writes properties to a file by first writing it to a
   * temporary file and then atomically moving it to the destination,
   * overwriting the destination file if it already exists.
   *
   * @param prop a Properties object to write.
   * @param file file to write the value to.
   * @throws IOException if an I/O error occurs.
   */
  public static void writePropertiesToFile(Properties prop, File file)
      throws IOException {
    // Create a temp file in the same directory as the file parameter.
    File temp = File.createTempFile(file.getName(), null,
                                    file.getAbsoluteFile().getParentFile());
    try (FileOutputStream fos = new FileOutputStream(temp)) {
      prop.store(fos, "");
      fos.getChannel().force(true);
    }
    atomicMove(temp, file);
    LOG.debug("Atomically moved {} to {}", temp, file);
  }

  /**
   * Reads the Properties from a file that was created by the
   * {@link #writePropertiesToFile(Properties, File) writePropertiesToFile}
   * method.
   *
   * @param file file to read the Properties object from.
   * @return the Properties object in the file
   * @throws IOException if an I/O error occurs.
   */
  public static Properties readPropertiesFromFile(File file)
      throws IOException {
    try (FileInputStream fis = new FileInputStream(file)) {
      Properties prop = new Properties();
      prop.load(fis);
      return prop;
    }
  }

  /**
   * Atomically move one file to another file.
   *
   * @param source the source file.
   * @param dest the destination file.
   * @throws IOException if an I/O error occurs.
   */
  public static void atomicMove(File source, File dest) throws IOException {
    Files.move(source.toPath(), dest.toPath(), ATOMIC_MOVE, REPLACE_EXISTING);
  }
}

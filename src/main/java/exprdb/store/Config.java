package exprdb.store;

import org.apache.commons.configuration2.*;

class Config {

  static PropertiesConfiguration config = null;

  private Config() {}

  public static void init(PropertiesConfiguration props) {
    config = props;
  }

  public static PropertiesConfiguration get() {
    return config;
  }

}

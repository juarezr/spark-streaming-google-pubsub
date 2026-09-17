package io.github.juarezr.spark.pubsub.structured;

import java.io.InputStream;
import java.util.Properties;

/** Compile-time connector identity. Fat-JAR MANIFEST version is the host app, not this. */
final class PubSubBuildInfo {

  private static final Properties PROPS = load();

  private PubSubBuildInfo() {}

  static String version() {
    return value("version");
  }

  static String built() {
    return value("built");
  }

  static String git() {
    return value("git");
  }

  private static String value(String key) {
    String raw = PROPS.getProperty(key, "");
    if (raw.isBlank() || raw.startsWith("${")) {
      return "unknown";
    }
    return raw;
  }

  private static Properties load() {
    Properties properties = new Properties();
    try (InputStream in = PubSubBuildInfo.class.getResourceAsStream("/pubsub-build.properties")) {
      if (in != null) {
        properties.load(in);
      }
    } catch (Exception ignored) {
      // keep defaults
    }
    return properties;
  }
}

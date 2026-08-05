package io.github.juarezr.spark.pubsub.dstream;

import com.google.auth.Credentials;
import io.github.juarezr.spark.pubsub.auth.PubSubCredentialsProvider;
import java.io.Serializable;
import java.util.Objects;

/**
 * Legacy-compatible credentials builder. Defaults to Application Default Credentials when {@link
 * #builder()}.build() is called without other configuration.
 */
public final class SparkGCPCredentials implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String jsonKeyFilePath;

  private SparkGCPCredentials(String jsonKeyFilePath) {
    this.jsonKeyFilePath = jsonKeyFilePath;
  }

  public CredentialsProvider provider() {
    return new CredentialsProvider(jsonKeyFilePath);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String jsonKeyFilePath;

    /** Use a service-account JSON key file. */
    public Builder jsonServiceAccount(String jsonKeyFilePath) {
      this.jsonKeyFilePath = jsonKeyFilePath;
      return this;
    }

    /**
     * No-op retained for Legacy API compatibility. ADC / metadata service account is already the
     * default on Dataproc.
     */
    public Builder metadataServiceAccount() {
      this.jsonKeyFilePath = null;
      return this;
    }

    public SparkGCPCredentials build() {
      return new SparkGCPCredentials(jsonKeyFilePath);
    }
  }

  /** Serializable credentials factory used by the receiver. */
  public static final class CredentialsProvider implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String jsonKeyFilePath;

    CredentialsProvider(String jsonKeyFilePath) {
      this.jsonKeyFilePath = jsonKeyFilePath;
    }

    public Credentials getCredentials() {
      return new PubSubCredentialsProvider(jsonKeyFilePath).getCredentials();
    }

    public String credentialsFile() {
      return jsonKeyFilePath;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CredentialsProvider)) {
        return false;
      }
      CredentialsProvider that = (CredentialsProvider) o;
      return Objects.equals(jsonKeyFilePath, that.jsonKeyFilePath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jsonKeyFilePath);
    }
  }
}

package io.github.juarezr.spark.pubsub.structured;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Collections;

/** Resolves Google credentials, defaulting to Application Default Credentials (ADC). */
final class PubSubCredentialsProvider implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final String PUBSUB_SCOPE = "https://www.googleapis.com/auth/pubsub";

  private final String credentialsFile;

  PubSubCredentialsProvider(String credentialsFile) {
    this.credentialsFile = credentialsFile;
  }

  Credentials getCredentials() {
    try {
      if (credentialsFile != null && !credentialsFile.isBlank()) {
        try (FileInputStream in = new FileInputStream(credentialsFile)) {
          GoogleCredentials credentials = ServiceAccountCredentials.fromStream(in);
          return credentials.createScoped(Collections.singletonList(PUBSUB_SCOPE));
        }
      }
      return GoogleCredentials.getApplicationDefault()
          .createScoped(Collections.singletonList(PUBSUB_SCOPE));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load Google credentials (ADC or file)", e);
    }
  }
}

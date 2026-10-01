package io.github.juarezr.spark.pubsub.structured;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
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

  private static final long serialVersionUID = 8759814691L;

  private static final String PUBSUB_SCOPE = "https://www.googleapis.com/auth/pubsub";

  private final String credentialsFile;

  PubSubCredentialsProvider(String credentialsFile) {
    this.credentialsFile = credentialsFile;
  }

  private Credentials getCredentials() {
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

  CredentialsProvider getProvider() {
    Credentials credentials = getCredentials();
    return FixedCredentialsProvider.create(credentials);
  }
}

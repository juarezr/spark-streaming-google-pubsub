package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.api.gax.core.CredentialsProvider;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class PubSubCredentialsProviderTest {

  @Test
  void providerForEmulatorDoesNotLoadApplicationDefaultCredentials() throws IOException {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .emulatorHost("localhost:8085")
            .build();

    CredentialsProvider provider = new PubSubCredentialsProvider(null).providerFor(config);
    assertNull(provider.getCredentials());
  }
}

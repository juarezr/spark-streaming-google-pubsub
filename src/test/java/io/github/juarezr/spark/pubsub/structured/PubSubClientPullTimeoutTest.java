package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.junit.jupiter.api.Test;

class PubSubClientPullTimeoutTest {

  @Test
  void clientPullTimeoutAddsBoundedSlack() {
    assertEquals(Duration.ofSeconds(22), PubSubClient.clientPullTimeout(Duration.ofSeconds(20)));
    assertEquals(Duration.ofSeconds(11), PubSubClient.clientPullTimeout(Duration.ofSeconds(10)));
    assertEquals(Duration.ofMillis(520), PubSubClient.clientPullTimeout(Duration.ofMillis(20)));
    assertEquals(Duration.ofMillis(501), PubSubClient.clientPullTimeout(Duration.ofMillis(1)));
  }

  @Test
  void pullTimeoutReturnsEmptyWithoutRetry() throws Exception {
    SubscriberStub stub = stubThatThrowsOnPull(deadlineExceeded());
    PubSubClient client = clientWithStub(stub);

    List<PulledMessage> pulled = client.pull(Duration.ofMillis(20), 10);

    assertTrue(pulled.isEmpty());
    assertEquals(0L, client.retryAttempts());
  }

  @Test
  void batchGatherSurvivesIdleThenPullTimeout() throws Exception {
    SubscriberStub stub =
        stubThatPullsThenTimesOut(PullResponse.getDefaultInstance(), deadlineExceeded());
    PubSubClient client = clientWithStub(stub);
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .batchTime(Duration.ofMillis(80))
            .pullDeadline(Duration.ofMillis(20))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset initial = stream.initialOffset();
    Offset latest = stream.latestOffset();

    assertEquals(initial, latest);
    assertEquals(0L, client.retryAttempts());
  }

  @Test
  void batchGatherKeepsMessagesWhenFollowUpTimesOut() throws Exception {
    SubscriberStub stub = stubThatPullsThenTimesOut(messagesResponse(2), deadlineExceeded());
    stubModifyAck(stub);
    PubSubClient client = clientWithStub(stub);
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .batchCount(100)
            .batchTime(Duration.ofMillis(80))
            .pullDeadline(Duration.ofMillis(20))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset();
    InputPartition[] partitions = stream.planInputPartitions(stream.initialOffset(), latest);

    assertEquals(2, ((PubSubInputPartition) partitions[0]).messages().size());
    assertEquals(0L, client.retryAttempts());
  }

  @SuppressWarnings("unchecked")
  private static SubscriberStub stubThatThrowsOnPull(DeadlineExceededException error) {
    SubscriberStub stub = mock(SubscriberStub.class);
    UnaryCallable<PullRequest, PullResponse> pull = mock(UnaryCallable.class);
    when(stub.pullCallable()).thenReturn(pull);
    when(pull.call(any(PullRequest.class), any())).thenThrow(error);
    return stub;
  }

  @SuppressWarnings("unchecked")
  private static SubscriberStub stubThatPullsThenTimesOut(
      PullResponse first, DeadlineExceededException error) {
    SubscriberStub stub = mock(SubscriberStub.class);
    UnaryCallable<PullRequest, PullResponse> pull = mock(UnaryCallable.class);
    when(stub.pullCallable()).thenReturn(pull);
    when(pull.call(any(PullRequest.class), any())).thenReturn(first).thenThrow(error);
    return stub;
  }

  @SuppressWarnings("unchecked")
  private static void stubModifyAck(SubscriberStub stub) {
    UnaryCallable<ModifyAckDeadlineRequest, Empty> modify = mock(UnaryCallable.class);
    when(stub.modifyAckDeadlineCallable()).thenReturn(modify);
    when(modify.call(any(ModifyAckDeadlineRequest.class))).thenReturn(Empty.getDefaultInstance());
  }

  private static PubSubClient clientWithStub(SubscriberStub stub) throws Exception {
    PubSubClient client =
        new PubSubClient(PubSubConfig.builder().projectId("p").subscription("s").build());
    Field stubField = PubSubClient.class.getDeclaredField("subscriberStub");
    stubField.setAccessible(true);
    stubField.set(client, stub);
    Field bytesField = PubSubClient.class.getDeclaredField("outstandingBytes");
    bytesField.setAccessible(true);
    bytesField.set(client, new AtomicLong(0));
    return client;
  }

  private static PullResponse messagesResponse(int count) {
    PullResponse.Builder builder = PullResponse.newBuilder();
    for (int i = 0; i < count; i++) {
      builder.addReceivedMessages(
          ReceivedMessage.newBuilder()
              .setAckId("ack-" + i)
              .setMessage(
                  PubsubMessage.newBuilder()
                      .setMessageId("message-" + i)
                      .setData(ByteString.copyFrom(new byte[] {1}))
                      .setPublishTime(Timestamp.newBuilder().setSeconds(1).build())
                      .build()));
    }
    return builder.build();
  }

  private static DeadlineExceededException deadlineExceeded() {
    return new DeadlineExceededException(
        "CallOptions deadline exceeded after 0.999s",
        new StatusRuntimeException(Status.DEADLINE_EXCEEDED),
        GrpcStatusCode.of(Status.Code.DEADLINE_EXCEEDED),
        true);
  }
}

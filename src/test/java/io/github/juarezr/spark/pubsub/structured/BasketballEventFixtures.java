package io.github.juarezr.spark.pubsub.structured;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Generates JSON payloads for basketball-themed dynamic schema integration tests. */
final class BasketballEventFixtures {

  static final int MESSAGE_COUNT = 100;
  static final int GAME_COUNT = 4;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final List<Matchup> MATCHUPS =
      List.of(
          new Matchup("game-1", "Lakers", "Celtics"),
          new Matchup("game-2", "Bulls", "Heat"),
          new Matchup("game-3", "Warriors", "Nuggets"),
          new Matchup("game-4", "Knicks", "Bucks"));

  private static final String[] EVENT_TYPES =
      new String[] {"SCORE", "FOUL", "TIMEOUT", "SUBSTITUTION"};

  private BasketballEventFixtures() {}

  static String avroDefinition() throws IOException {
    try (InputStream in =
        BasketballEventFixtures.class.getResourceAsStream("/avro/basketball-game-event.avsc")) {
      if (in == null) {
        throw new IOException("Missing /avro/basketball-game-event.avsc");
      }
      JsonNode parsed = MAPPER.readTree(in);
      return MAPPER.writeValueAsString(parsed);
    }
  }

  static byte[] jsonPayload(int index) {
    Matchup matchup = MATCHUPS.get(index % GAME_COUNT);
    String eventType = EVENT_TYPES[index % EVENT_TYPES.length];
    int points = eventType.equals("SCORE") ? 1 + (index % 3) : 0;
    int period = 1 + (index % 4);
    long clockMillis = 720_000L - (index * 1000L);

    ObjectNode root = MAPPER.createObjectNode();
    root.put("eventId", "evt-" + index);
    root.put("gameId", matchup.gameId);
    root.put("homeTeam", matchup.homeTeam);
    root.put("awayTeam", matchup.awayTeam);
    root.put("scoringTeam", points > 0 ? matchup.homeTeam : matchup.awayTeam);
    root.put("eventType", eventType);
    root.put("points", points);
    root.put("period", period);
    root.put("clockMillis", clockMillis);
    root.put("isFastBreak", index % 5 == 0);
    root.put("shotDistanceFt", 5.5 + (index % 20));
    root.put("foulCount", (index % 3) + 0.5);
    root.put("playerId", "p-" + (index % 12));
    root.put("playerName", "Player " + (index % 12));
    root.put("jersey", 10 + (index % 15));
    root.put("lineup", "p-" + (index % 12) + ",p-" + ((index + 1) % 12));
    root.put("rebounds", index % 2);
    root.put("assists", index % 3);

    return root.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static final class Matchup {
    final String gameId;
    final String homeTeam;
    final String awayTeam;

    Matchup(String gameId, String homeTeam, String awayTeam) {
      this.gameId = gameId;
      this.homeTeam = homeTeam;
      this.awayTeam = awayTeam;
    }
  }
}

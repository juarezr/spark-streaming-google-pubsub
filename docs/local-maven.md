# Maven Wrapper notes

This repository expects Maven 3.9+. CI uses `actions/setup-java` with Maven from the runner
cache. For local builds without a system Maven install, use any Maven 3.9+ distribution.

Example with a local download under `.tools/` (gitignored):

```bash
export PATH="$PWD/.tools/maven/bin:$PATH"
mvn -Pspark35 clean verify
```

## Spark profiles and integration tests

Integration tests are compiled against the active Spark profile (`spark35`, `spark40`, etc.).
Each profile sets `${log4j.version}` to match Log4j on that Spark line (3.5 → 2.20.0, 4.0/4.1 →
2.24.3, 4.2/4.3 → 2.25.4). SLF4J and Log4j stay `provided` like on a cluster; gson is bundled only in
the `*-all.jar` from `mvn package`.

After changing the active profile, run `mvn clean test-compile` or `mvn clean verify` if you see
stale test classes or `NoSuchMethodError` on Spark APIs.

If you still see `NoSuchMethodError` on `DataStreamWriter.option`, run `mvn clean verify` once
to drop stale classes under `target/test-classes`.

# Maven Wrapper notes

This repository expects Maven 3.9+. CI uses `actions/setup-java` with Maven from the runner
cache. For local builds without a system Maven install, use any Maven 3.9+ distribution.

Example with a local download under `.tools/` (gitignored):

```bash
export PATH="$PWD/.tools/maven/bin:$PATH"
mvn -Pspark35 clean verify
```

When switching Spark profiles (`spark35`, `spark40`, `spark41`, `spark42`), run **`mvn clean`**
once if you did not start from a fresh tree. Integration tests call Spark streaming APIs that
differ between 3.5 and 4.x; stale `target/test-classes` from another profile can cause
`NoSuchMethodError` at runtime. CI always checks out clean; locally, prefer
`mvn clean -Pspark41 verify` when changing profiles.

# Maven Wrapper notes

This repository expects Maven 3.9+. CI uses `actions/setup-java` with Maven from the runner
cache. For local builds without a system Maven install, use any Maven 3.9+ distribution.

Example with a local download under `.tools/` (gitignored):

```bash
export PATH="$PWD/.tools/maven/bin:$PATH"
mvn -Pspark35 clean verify
```

# Code coverage

This project uses [JaCoCo](https://www.jacoco.org/jacoco/trunk/doc/maven.html) for measurement and
[Coveralls](https://coveralls.io/) for published reports. Upload is handled by
[hazendaz/coveralls-maven-plugin](https://github.com/hazendaz/coveralls-maven-plugin).

## Maven profiles

| Profile | Purpose |
| ------- | ------- |
| `coverage` | Instrument tests and generate `target/site/jacoco/` (HTML + `jacoco.xml`) |
| `coveralls` | Submit an existing JaCoCo XML report to Coveralls (`coveralls:report`) |
| `coverage-report` | Convenience default goal: `clean verify` (use with `coverage` + `spark35`) |

## Local workflow

```bash
# Unit + integration tests with coverage (emulator required for IT — see README Contributing)
mvn -Pcoverage,spark35 clean verify

# View HTML report
xdg-open target/site/jacoco/index.html   # Linux
open target/site/jacoco/index.html       # macOS

# Optional: upload to Coveralls (after verify)
export COVERALLS_REPO_TOKEN=…   # from Coveralls → repo → Settings → REPO TOKEN
mvn -Pcoveralls coveralls:report -DrepoToken="$COVERALLS_REPO_TOKEN"
```

One-shot rebuild and upload:

```bash
mvn -Pcoverage,coveralls,spark35 clean verify coveralls:report -DrepoToken="$COVERALLS_REPO_TOKEN"
```

## CI (GitHub Actions)

Only the **Spark 3.5** matrix job runs tests with `-Pcoverage`. After `verify`, if the repository
secret **`COVERALLS_REPO_TOKEN`** is configured, CI runs:

```bash
mvn -B -Pcoveralls coveralls:report -DrepoToken="$COVERALLS_REPO_TOKEN"
```

Add the secret under **Settings → Secrets and variables → Actions → New repository secret**.
Use the repo token from the Coveralls project for `juarezr/spark-streaming-google-pubsub`.

Fork pull requests do not receive secrets; the Coveralls step is skipped and the build still passes.

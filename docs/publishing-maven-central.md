# Publishing to Maven Central and GitHub Packages

This project publishes via the [Sonatype Central Publisher Portal](https://central.sonatype.com/) using the `central-publishing-maven-plugin` (OSSRH is retired), and also deploys the same coordinates to [GitHub Packages](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry).

Releases are cut from a `v*` tag (GitHub Portal or `git push`). The workflow publishes thin artifacts to Maven Central, the same coordinates (including `*-all`) to GitHub Packages, and attaches fat JARs to the GitHub Release.

See also how to [register a Maven Central Account](register-maven-account.md)

## What is published where

Maven Central receives only the **thin** artifacts (main JAR, sources, javadoc, POM, signatures). The shaded `*-all.jar` (Google client libraries bundled) is **not** uploaded to Central — it would blow the monthly release-size limit.

GitHub Packages receives the Maven publication **including** the `-all` classifier (no `-Prelease`, so shade stays bound). Fat JARs are also attached as files on the GitHub Release for that tag.

```mermaid
flowchart LR
  portal["GitHub Portal tag or Release vX"] --> tagPush["push tag v*"]
  tagPush --> ga["release.yml matrix"]
  ga --> s35["spark35 _2.12"]
  ga --> s41["spark41 _2.13"]
  s35 --> ghPkg["GitHub Packages"]
  s41 --> ghPkg
  s35 --> thin["Central: main + sources + javadoc"]
  s41 --> thin2["Central: main + sources + javadoc"]
  ga --> fatBuild["package without release profile"]
  fatBuild --> ghRel["GitHub Release: both -all.jar files"]
```

## GitHub Actions Release

### Release Workflow

```mermaid
flowchart TB
  src[Java Data Source V2]
  src --> p35["profile spark35 Scala 2.12"]
  src --> p41["profile spark41 Scala 2.13 (release)"]
  p35 --> c35["Central _2.12"]
  p41 --> c413["Central _2.13"]
  p35 --> g35["GitHub Packages _2.12"]
  p41 --> g413["GitHub Packages _2.13"]
  p41 --> t40["CI Spark 4.0"]
  p41 --> t41["CI Spark 4.1"]
  p41 --> t42["CI Spark 4.2"]
```

### How to make a release

Create a version tag (and optionally a GitHub Release) in the GitHub Portal, or push a `v*` tag:

```bash
git tag v0.4.1
git push origin v0.4.1
```

Creating a Release in the GitHub UI with a new `v*` tag pushes the tag and starts [`release.yml`](../.github/workflows/release.yml). Prefer that Portal-first path: the workflow then **uploads** `*-all.jar` onto the existing Release. If only the tag is pushed and no Release exists yet, the workflow creates one.

The workflow:

1. Sets the Maven version from the tag (`v` prefix stripped).
2. Deploys thin artifacts for Spark 3.5 (`_2.12`) and Spark 4.1 (`_2.13`) to **Maven Central** (`-Prelease` skips the shade plugin and signs). The `_2.13` JAR is tested in CI against Spark 4.0, 4.1, and 4.2.
3. Deploys to **GitHub Packages** with `-Pgithub-packages` (no `-Prelease`): flattened POM, thin JAR, sources, javadoc, and `-all`.
4. After both matrix jobs finish, attaches `*-all.jar` to the GitHub Release for the tag.

Consumers should use `--packages` / a Maven dependency against Central when possible. Use the GitHub Release fat JAR when a single `--jars` file is required (for example Dataproc without Maven resolution). GitHub Packages is an extra Maven registry for the same coordinates; **Maven still requires authentication** to download even public packages.

Example `settings.xml` server for GitHub Packages:

```xml
<server>
  <id>github</id>
  <username>YOUR_GITHUB_USERNAME</username>
  <password>YOUR_GITHUB_PAT_OR_TOKEN</password>
</server>
```

Repository URL: `https://maven.pkg.github.com/juarezr/spark-streaming-google-pubsub`

### Verification

After the Portal shows **Published**, the coordinates appear on Maven Central within minutes to a few hours:

```text
io.github.juarezr:spark-streaming-google-pubsub_2.12:0.4.1
io.github.juarezr:spark-streaming-google-pubsub_2.13:0.4.1
```

The same coordinates should appear under the repo’s GitHub Packages. The same tag’s GitHub Release should list `spark-streaming-google-pubsub_2.12-0.4.1-all.jar` and `spark-streaming-google-pubsub_2.13-0.4.1-all.jar`.

Do not republish an already-released version without the fat JAR (or with a different set of files). Central is immutable; the next cut must be a new version.

### Thin JAR dependencies

The published POM pins `google-cloud-pubsub`, `google-auth-library-oauth2-http`, and `gson` to versions that stay compatible with Spark 3.5 / Dataproc 2.3. Those pins are not version ranges: a range would float consumers onto newer GAX/gRPC/Guava. Override the Google clients in your own POM or BOM if you need a newer stack.

### Snapshot builds

Snapshots are useful internally (`0.4.1-SNAPSHOT`) but are **not** published to Maven Central.
Use GitHub Packages or GCS for snapshot distribution if needed (`-Pgithub-packages`).

# Publishing to Maven Central

This project publishes via the [Sonatype Central Publisher Portal](https://central.sonatype.com/) using the `central-publishing-maven-plugin` (OSSRH is retired).

## 1. Create a Central Portal account

1. Register at <https://central.sonatype.com/>
2. Prefer signing in with GitHub so namespaces can be verified automatically.

## 2. Namespace

Published groupId: **`io.github.juarezr`**.

This namespace is already verified in the [Central Portal Namespaces](https://central.sonatype.com/publishing/namespaces) page for this publisher account.

## 3. Generate a portal user token

Account → **Generate User Token**. Store:

- username → GitHub secret `MAVEN_USERNAME`
- password/token → GitHub secret `MAVEN_PASSWORD`

## 4. Create a GPG signing key

```bash
gpg --list-secret-keys
gpg --full-generate-key
gpg --list-secret-keys --keyid-format LONG
gpg --export-secret-keys -a YOUR_KEY_ID > secring.asc
gpg --keyserver keyserver.ubuntu.com --send-keys YOUR_KEY_ID
```

GitHub secrets:

| Secret            | Value                     |
|-------------------|---------------------------|
| `GPG_PRIVATE_KEY` | Contents of `secring.asc` |
| `GPG_PASSPHRASE`  | Key passphrase            |
| `MAVEN_USERNAME`  | Portal token username     |
| `MAVEN_PASSWORD`  | Portal token password     |

## 5. Local dry-run

```bash
# Spark 3.5 artifact
mvn -Pspark35,release clean deploy -DskipTests

# Spark 4.0 artifact
mvn -Pspark40,release clean deploy -DskipTests
```

Ensure `~/.m2/settings.xml` contains:

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username>${env.MAVEN_USERNAME}</username>
      <password>${env.MAVEN_PASSWORD}</password>
    </server>
  </servers>
</settings>
```

## 6. GitHub Actions release

Push a version tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The [`release.yml`](../.github/workflows/release.yml) workflow builds both Spark profiles, signs artifacts, and publishes to Central.

## 7. Verify

After the Portal shows **Published**, the coordinates appear on Maven Central within minutes to a few hours:

```text
io.github.juarezr:spark-streaming-google-pubsub_2.12:0.1.0
io.github.juarezr:spark-streaming-google-pubsub_2.13:0.1.0
```

## Snapshot builds

Snapshots are useful internally (`0.1.0-SNAPSHOT`) but are **not** published to Maven Central.
Use GitHub Packages or GCS for snapshot distribution if needed.

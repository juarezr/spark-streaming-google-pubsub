# Registering a Maven Central Account

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
```

GitHub secrets:

| Secret            | Value                     |
|-------------------|---------------------------|
| `GPG_PRIVATE_KEY` | Contents of `secring.asc` |
| `GPG_PASSPHRASE`  | Key passphrase            |
| `MAVEN_USERNAME`  | Portal token username     |
| `MAVEN_PASSWORD`  | Portal token password     |

## 5. Publish public GPG key is not on a keyserver Central can use

1. From the machine that created the key used in GPG_PRIVATE_KEY:

```bash
gpg --list-secret-keys --keyid-format LONG
gpg --keyserver keyserver.ubuntu.com --send-keys YOUR_KEY_ID
gpg --keyserver keys.openpgp.org --send-keys YOUR_KEY_ID
```

1. Confirm it is searchable (wait a few minutes, then):

```bash
gpg --keyserver keyserver.ubuntu.com --recv-keys YOUR_KEY_ID
```

## 6. Local dry-run

```bash
# Thin artifacts only (shade is skipped). Do not use this to produce *-all.jar.
mvn -Pspark35,release clean deploy -DskipTests
mvn -Pspark40,release clean deploy -DskipTests
```

Fat JAR for local use: `mvn -Pspark35 -DskipTests package` (no `release` profile).

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

# Docker / WSL2 Setup for Integration Tests

The integration test (`StreamingJobTest`) uses Testcontainers to spin up LocalStack (Kinesis). On Windows with Docker installed inside WSL2, three configuration steps are required.

---

## 1. Enable TCP listener in WSL2 Docker

By default Docker only listens on the Unix socket (`/var/run/docker.sock`), which is not accessible from a Windows JVM.

**`/etc/docker/daemon.json`** (inside WSL2 Ubuntu):
```json
{
  "hosts": ["unix:///var/run/docker.sock", "tcp://0.0.0.0:2375"]
}
```

**`/etc/systemd/system/docker.service.d/override.conf`** (inside WSL2 Ubuntu):
```ini
[Service]
ExecStart=
ExecStart=/usr/bin/dockerd --containerd=/run/containerd/containerd.sock
```

The override is required because the default systemd unit passes `-H fd://` on the `ExecStart` line, which conflicts with `hosts` in `daemon.json` and prevents Docker from starting.

Apply:
```bash
sudo systemctl daemon-reload && sudo systemctl restart docker
```

---

## 2. Set DOCKER_HOST before running tests

WSL2 runs in a separate VM with its own IP address. `localhost` from a Windows JVM does not reach Docker inside WSL2 — you must use the WSL2 VM IP explicitly.

**PowerShell:**
```powershell
$wsl2ip = (wsl hostname -I).Trim().Split(' ')[0]
$env:DOCKER_HOST = "tcp://${wsl2ip}:2375"
```

**Git Bash / MSYS2:**
```bash
export DOCKER_HOST="tcp://$(wsl hostname -I | awk '{print $1}' | tr -d '\r'):2375"
```

> The WSL2 VM IP changes on every WSL restart — set this variable again after each restart.

---

## 3. Docker API version (pom.xml)

Docker 29.x requires a minimum API version of 1.44. The copy of docker-java shaded inside Testcontainers defaults to 1.32 and the `DOCKER_API_VERSION` environment variable has no effect on it. The correct fix is the `api.version` system property, set permanently in `pom.xml`:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <version>3.0.0-M9</version>
    <configuration>
        <argLine>
            --add-opens java.base/java.lang=ALL-UNNAMED
            --add-opens java.base/java.util=ALL-UNNAMED
            --add-opens java.base/java.io=ALL-UNNAMED
        </argLine>
        <systemPropertyVariables>
            <api.version>1.44</api.version>
        </systemPropertyVariables>
    </configuration>
</plugin>
```

> Do **not** use `<environmentVariables>` in the surefire config — it overrides inherited shell environment variables, including `DOCKER_HOST`.

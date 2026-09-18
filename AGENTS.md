<!-- SPDX-License-Identifier: Apache-2.0 -->

# Apache Hadoop — Guidance for AI Agents

This file orients automated and AI-assisted tools working in the Apache Hadoop repository.

## Building and testing

Ask the user before running Maven or tests directly on the host. Pure Java changes and
tests with no native library requirement are fine to run on the host. For anything that
needs the native profile or `-Drequire.*` libraries, prefer the Docker build environment
from `start-build-env.sh`, which is the image Hadoop CI uses; [BUILDING.txt](BUILDING.txt)
documents it, including the non-interactive form:

```
DOCKER_INTERACTIVE_RUN= ./start-build-env.sh ubuntu_24 <command> [args...]
```

- Without `-Dmaven.test.failure.ignore=false` (`hadoop-project/pom.xml` sets it to `true`),
  Maven exits 0 even when Java tests fail. Check the result from
  `<module>/target/surefire-reports/` or the `Tests run: ..., Failures: ...` line.

## Security

Before investigating, reporting, or acting on any security issue in this repository, you
MUST read [SECURITY.md](SECURITY.md).

It defines the Hadoop threat model — the deployments the project defends, the trust
boundaries, and what is and is not a vulnerability — along with the rules for vulnerability
reports, including additional requirements for AI-generated reports. Findings or reports that
fall outside that model will be rejected.

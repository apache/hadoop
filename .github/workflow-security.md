<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

# Important Security Information for GitHub Actions

This guide aims to help contributors understand and author secure CI workflows.
This guide is not complete, but provides many links to articles where you can
learn more specifics about vulnerabilities and
their mitigations.

**Note**: _The details here are changing, especially with the recent supply-chain
attacks that are taking advantage of insecure actions. Please open a pull
request or Jira issue if this document needs updates._

## Required Reading

- _Apache Infra. GitHub Actions Policy_[^1]. All changes MUST comply with this policy.

## Suggested Reading

- _Understand GitHub Actions_[^2] is a good high-level overview of GitHub
  Actions if you are not familiar or need a refresher.
- _Securing the open source supply chain across Github_[^5].
- Github Actions _Secure Use Reference_[^4]

## Security Conceptual Model

__TL;DR: GitHub Actions are difficult to secure properly, especially for public
repositories.__

Since we allow the public to open pull requests against our repository, we are
at risk of offering "remote code execution as a service". If we are not careful
in how we build these workflows, attackers can:

- Steal secrets granting write access to our repository.
- Poison builds and/or container repositories with malicious code.
- Execute arbitrary code on GitHub's infrastructure, which could be used to
  attack other repositories or services.

See [Recent Supply Chain Attacks](#recent-supply-chain-attacks) for real examples.

### How Trigger Events Influence Security

These are some common _events used to trigger workflows_[^6] via the `on:` predicate.
Understanding the behavior of these is critical to securing our workflows.

#### push

`push` events run a workflow when a commit or tag is pushed to the repository.
`push` events run in the context of the branch that was pushed to. Although a
`push` is typically performed by a trusted user (with write access), care is
still required as these workflows can run with elevated permissions (i.e.
default read + write GITHUB_TOKEN), making them vulnerable to things like
variable injection attacks and secret exfiltration.

#### pull_request

_By default_[^14], a `pull_request` event runs a workflow when a pull request is
opened, synchronized, or reopened. These workflows run in the context of the
merge commit between the PR branch and the base branch. This means that if the
PR is from a fork, the workflow will not have access to secrets and the
GITHUB_TOKEN will have read-only permissions. This makes `pull_request` events
generally safe to use for untrusted code, such as from forks.

According to _GitHub's docs_[^15]:

> Workflows don't run in forked repositories by default. You must enable GitHub
> Actions in the Actions tab of the forked repository.
>
> With the exception of GITHUB_TOKEN, secrets are not passed to the runner when a
> workflow is triggered from a forked repository. The GITHUB_TOKEN has read-only
> permissions in pull requests from forked repositories.


#### pull_request_target

***privileged***

The `pull_request_target` event runs when activity on a pull request (PR) in
the workflow's repository occurs. _By default_[^16], the workflow runs when a
pull request is opened, reopened, or when the head of the pull request branch
is
updated.

`pull_request_target` runs in the context of the default branch of the base
repository, unlike `pull_request`, which runs in the context of the merge commit.
While this feature originally aimed to prevent execution of unsafe code from
the head of the pull request, it created a trap for many users who expected it
to be a more secure version of `pull_request`. The elevated permissions for
`pull_request_target` events (access to secrets and write permissions) opened
new attack vectors.

If the workflow is configured to run on `pull_request_target`, it will have
access to secrets and a `GITHUB_TOKEN` with write permissions, even for pull
requests from forks. This means that if an attacker can find a way to trigger
this workflow (e.g. by opening a pull request from a fork), they could
potentially execute code with elevated permissions, which could lead to
repository compromise or secret exfiltration.

Note that the behavior of this event recently changed: Prior to Dec. 2025,
these events ran in the context of the PR's base branch, which could be used
to target older versions of the codebase with vulnerabilities. Now it always
uses the default base repo. branch.

#### workflow_run

***privileged***

`workflow_run` events _are triggered when_[^17] a workflow run is requested or completed.
These events allow you to execute a workflow based on execution or completion
of another workflow.

`workflow_run` events run in a privileged context: They can access secrets, and
have write permissions, even if the previous workflow did not.

This is good and bad from a security perspective. These are useful in when you
have a non-privileged workflow that you need to follow with a privileged one. This is
Since they are privileged, though, you must be careful not to run untrusted
code or depend on untrusted variables.

#### Comparison

| Trigger            | Context           | Secrets Access? | GITHUB_TOKEN Permissions | Risk Level
|--------------------|-------------------|-----------------|-------------------------|-----------
| push               | The branch pushed to | Yes             | Write (usually)        | Low (Only trusted users push)
| pull_request       | The merge commit  | No (from forks) | Read-only | Low (Safe for untrusted code)
| pull_request_target | The ~~base~~ default branch | Yes | Write (usually) | CRITICAL (Dangerous if misconfigured)
| workflow_run       | The default branch | Yes | Write (usually) | High (Runs after another workflow)

## Recent Supply Chain Attacks

Here are a few of the more significant attacks recently exploiting insecure actions:

*tj-actions/changed-files* (March 2025): A massive attack _affecting over 23,000_[^7]
repositories. Attackers compromised a personal access token (PAT) to update
version tags with a malicious commit ([CVE-2025-30066][^8]) that dumped secrets into
workflow logs.

*reviewdog/action-setup* (March 2025): _Used as a "stepping stone"_[^9] to compromise
tj-actions, this incident (CVE-2025-30154[^10]) highlighted how vulnerabilities
in one action can cascade through the supply chain.

*Trivy-action* (March 2026): Attackers _force-pushed version tags_[^11] after
compromising credentials with write access, exfiltrating secrets from every
pipeline running a Trivy scan (CVE-2026-33634[^13]). Setting aside the irony
of a security scanning action becoming a vector for supply chain attacks, their
_final writeup_[^12] contains good practices for mitigation.

## Further Reading

We recommend learning more about specific attack techniques and their
mitigations. This _openssf.org blog post_[^18] is a good overview, in addition
to the rest of the links in this document.

## References

[^1]: https://infra.apache.org/github-actions-policy.html
[^2]: https://docs.github.com/en/actions/get-started/understand-github-actions
[^3]: https://github.blog/changelog/2025-11-07-actions-pull_request_target-and-environment-branch-protections-changes/
[^4]: https://docs.github.com/en/actions/reference/security/secure-use
[^5]: https://github.blog/security/supply-chain-security/securing-the-open-source-supply-chain-across-github/
[^6]: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows
[^7]: https://www.stepsecurity.io/blog/harden-runner-detection-tj-actions-changed-files-action-is-compromised
[^8]: https://nvd.nist.gov/vuln/detail/CVE-2025-30066
[^9]: https://www.cisa.gov/news-events/alerts/2025/03/18/supply-chain-compromise-third-party-tj-actionschanged-files-cve-2025-30066-and-reviewdogaction
[^10]: https://www.cve.org/CVERecord?id=CVE-2025-30154
[^11]: https://github.com/aquasecurity/trivy/discussions/10425
[^12]: https://github.com/aquasecurity/trivy/discussions/10462
[^13]: https://nvd.nist.gov/vuln/detail/CVE-2026-33634
[^14]: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request
[^15]: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflows-in-forked-repositories
[^16]: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request_target
[^17]: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_run
[^18]: https://openssf.org/blog/2024/08/12/mitigating-attack-vectors-in-github-workflows/
[^19]: https://securitylab.github.com/resources/github-actions-preventing-pwn-requests/


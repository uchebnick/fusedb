# Security policy

FuseDB is research-stage software and does not yet carry a production support
or security-response SLA. Please do not use public issues for a suspected
vulnerability.

## Reporting

Report vulnerabilities through GitHub's private **Security advisories → Report
a vulnerability** flow for this repository. Include the affected revision,
platform, minimal reproduction, impact, and any known workaround. Do not attach
real database contents, credentials, or customer data.

The maintainers will acknowledge the report, reproduce it, assess affected
versions, and coordinate disclosure. A fix is not considered complete until it
has a regression test and the durability/security gates pass.

## Supported versions

| Version | Supported |
|---|---|
| `0.1.x` | Yes |
| `< 0.1.0` | No |

Only the latest patch of the current pre-1.0 minor line and the current `main`
revision are eligible for fixes. Benchmark containers and experimental internal
packages are unsupported. Security-relevant release artifacts and dependency
policy are documented in [`docs/release.md`](./docs/release.md).

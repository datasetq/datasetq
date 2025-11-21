# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in DSQ, please report it to us as soon as possible. We take security seriously and appreciate your efforts to responsibly disclose your findings.

**Please DO NOT open a public GitHub issue for security vulnerabilities.**

### How to Report

Send an email to: **security@durableprogramming.com**

Include the following information in your report:
- Description of the vulnerability
- Steps to reproduce the issue
- Potential impact
- Any suggested fixes or mitigations (if available)

### Response Timeline

- **Initial Response**: We aim to acknowledge receipt of your vulnerability report within 48 hours.
- **Status Updates**: We will provide regular updates on our progress, typically within 7 days of the initial report.
- **Resolution**: We will work to release a fix as quickly as possible, depending on the complexity and severity of the issue.

## Coordinated Disclosure

We believe in coordinated disclosure and request that you:
- Give us reasonable time to investigate and address the vulnerability before public disclosure
- Make a good faith effort to avoid privacy violations, data destruction, and service disruption
- Do not access or modify data that does not belong to you

We commit to:
- Work with you to understand and validate the vulnerability
- Keep you informed of our progress toward a fix
- Credit you for the discovery in our security advisories (unless you prefer to remain anonymous)

## Security Considerations for Users

### Input Validation

DSQ processes various data formats (CSV, JSON, Parquet, etc.). When using DSQ:
- Be cautious when processing data from untrusted sources
- Large or malformed files may consume significant memory or processing time
- Use resource limits when processing untrusted data

### Data Privacy

- DSQ processes data locally and does not transmit data to external servers
- Be aware of file permissions when reading and writing sensitive data
- Temporary files may be created during processing - ensure proper cleanup

### Dependencies

- We regularly audit dependencies for known vulnerabilities using `cargo audit`
- Keep DSQ updated to the latest version to receive security patches
- Review the CHANGELOG.md for security-related updates

## Supported Versions

We provide security updates for:
- The latest stable release
- The previous stable release (for a limited time after a new major version)

Older versions may not receive security updates. We recommend upgrading to the latest stable version.

## Security Update Process

When a security vulnerability is fixed:
1. A security advisory will be published
2. A new version will be released with the fix
3. The CHANGELOG.md will be updated with security-related notes
4. Affected users will be notified through our communication channels

## Known Security Considerations

### Memory Usage

DSQ may consume significant memory when processing large datasets. Consider:
- Using streaming operations where possible
- Monitoring memory usage for untrusted input
- Setting appropriate resource limits in production environments

### File System Access

DSQ requires read/write access to specified files. Ensure:
- Proper file permissions are set
- DSQ is not run with unnecessary elevated privileges
- Input/output paths are validated before use

## Security Best Practices

When using DSQ in production:
1. Run with minimal required permissions
2. Validate and sanitize file paths from user input
3. Set resource limits (memory, CPU time) for processing untrusted data
4. Keep dependencies updated
5. Monitor for security advisories
6. Use the latest stable version

## Contact

For security-related questions or concerns:
- Email: security@durableprogramming.com
- For general questions: See CONTRIBUTING.md

Thank you for helping keep DSQ and our users secure!

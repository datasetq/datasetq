# Contributing to dsq

Thank you for your interest in contributing to dsq! We welcome contributions from the community and are committed to providing a welcoming and inclusive environment for all contributors.

## Code of Conduct

This project follows a code of conduct to ensure a positive experience for all contributors. By participating, you agree to:

- Be respectful and inclusive
- Focus on constructive feedback
- Accept responsibility for mistakes
- Show empathy towards other contributors
- Help create a positive community

## How to Contribute

### Reporting Issues

If you find a bug or have a feature request:

1. Check the [existing issues](https://github.com/durableprogramming/dsq/issues) to see if it's already reported
2. If not, create a new issue with:
   - Clear title and description
   - Steps to reproduce (for bugs)
   - Expected vs actual behavior
   - Environment details (OS, Rust version, etc.)

### Contributing Code

1. **Fork the repository** on GitHub
2. **Clone your fork** locally
3. **Create a feature branch** from `main`
4. **Make your changes** following our guidelines
5. **Run tests** to ensure everything works
6. **Commit your changes** with clear, descriptive messages
7. **Push to your fork** and create a pull request

### Development Setup

1. Ensure you have Rust 1.69+ installed
2. Clone the repository
3. Run `cargo build` to build all crates
4. Run `cargo test` to run the test suite

### Code Style

- Follow Rust's standard formatting (we use `rustfmt`)
- Use `clippy` for linting
- Write comprehensive documentation for public APIs
- Include tests for new functionality

### Commit Messages

Use clear, descriptive commit messages that explain the "why" rather than just the "what":

- Good: "Add validation for CSV headers to prevent data corruption"
- Less good: "Fix CSV bug"

### Pull Request Process

1. **Update documentation** if your changes affect user-facing behavior
2. **Add tests** for new features or bug fixes
3. **Ensure CI passes** - all tests and checks must pass
4. **Request review** from maintainers
5. **Address feedback** and make necessary changes

### Testing Requirements

All contributions must include appropriate tests:

- **Unit tests**: Write unit tests for new functions and methods
- **Integration tests**: Add integration tests for complex features
- **Coverage**: Aim for >90% code coverage (we use `cargo-tarpaulin`)
- **Regression tests**: Include tests that prevent regressions for bug fixes
- **Property-based tests**: Use `proptest` for complex logic when appropriate
- **Cross-platform**: Test on multiple platforms if possible (Linux, macOS, Windows)

Run the test suite:

```bash
cargo test --workspace
```

Check test coverage:

```bash
cargo install cargo-tarpaulin
cargo tarpaulin --workspace --out Html --output-dir coverage
```

### Documentation Requirements

Documentation is essential for all contributions:

- **Public APIs**: All public functions, structs, and modules must have `///` rustdoc comments
- **Examples**: Include code examples in documentation that can be tested with `cargo test --doc`
- **README updates**: Update README files for user-facing changes
- **CHANGELOG**: Add entries to CHANGELOG.md for notable changes
- **Error messages**: Ensure error messages are clear and actionable

### Code Review Expectations

When submitting a pull request:

- **Be responsive**: Address review feedback promptly
- **Be open**: Accept constructive criticism and suggestions
- **Be thorough**: Ensure your changes are complete and well-tested
- **Be patient**: Reviews may take time, especially for large changes

When reviewing others' code:

- **Be respectful**: Provide constructive, actionable feedback
- **Be specific**: Point out issues clearly with examples
- **Be positive**: Acknowledge good work and improvements

## Areas for Contribution

### Code Contributions
- Bug fixes
- Performance improvements
- New features
- Code refactoring
- Test improvements

### Documentation
- Improve existing documentation
- Add usage examples
- Create tutorials
- Translate documentation

### Testing
- Add missing test coverage
- Improve test reliability
- Add benchmarks

### Tooling
- Build improvements
- CI/CD enhancements
- Development tools

## Getting Help

- **Issues**: Use GitHub issues for bugs and feature requests
- **Discussions**: Use GitHub discussions for questions and ideas
- **Documentation**: Check the README and docs/ directory first

## Recognition

Contributors are recognized through:
- GitHub's contributor insights
- Attribution in CHANGELOG.md
- Mention in release notes

## Developer Certificate of Origin (DCO)

By submitting code contributions to this project, you certify that:

1. The contribution was created in whole or in part by you and you have the right to submit it under the open source license indicated in the file; or
2. The contribution is based upon previous work that, to the best of your knowledge, is covered under an appropriate open source license and you have the right under that license to submit that work with modifications, whether created in whole or in part by you, under the same open source license; or
3. The contribution was provided directly to you by some other person who certified (1), (2) or (3) and you have not modified it.

You understand and agree that this project and the contribution are public and that a record of the contribution (including all personal information you submit with it) is maintained indefinitely and may be redistributed consistent with this project or the open source license(s) involved.

## Security Considerations

When contributing code:

- **Input validation**: Always validate and sanitize user input
- **Error handling**: Handle errors appropriately without exposing sensitive information
- **Dependencies**: Be cautious about adding new dependencies
- **Unsafe code**: Minimize use of `unsafe` and document safety invariants thoroughly
- **Security issues**: Report security vulnerabilities privately (see SECURITY.md)

## Performance Considerations

For performance-critical code:

- Profile before optimizing
- Include benchmarks for performance improvements
- Document performance characteristics
- Consider memory usage and allocation patterns
- Test with large datasets when relevant

## Continuous Integration

Our CI pipeline checks:

- ✓ All tests pass (`cargo test --workspace`)
- ✓ Code formatting (`cargo fmt --check`)
- ✓ Linting (`cargo clippy -- -D warnings`)
- ✓ Documentation builds (`cargo doc --workspace --no-deps`)
- ✓ Security audit (`cargo audit`)
- ✓ Test coverage (target: >90%)

Ensure your changes pass all CI checks before requesting review.

## Crate Publication Policy

The dsq project is organized as a workspace with multiple crates. Our publication strategy:

### Published Crates

The following crates are published to crates.io for public use:

- **`dsq-core`** - Core data structures and types (public library)
- **`dsq-shared`** - Shared utilities and types (public library)
- **`dsq-parser`** - Query parsing (public library)
- **`dsq-filter`** - Data filtering logic (public library)
- **`dsq-functions`** - Built-in functions (public library)
- **`dsq-formats`** - Format readers/writers (public library)
- **`dsq-io`** - I/O operations (public library)
- **`datasetq`** - Main CLI application (public binary)

### Internal-Only Crates

The following crates have `publish = false` and are for internal use only:

- **`dsq-cli`** - CLI-specific logic (internal, used by main `datasetq` crate)

### For Contributors

When adding new workspace crates:

1. **Determine publication status**: Decide if the crate should be public or internal
2. **Add `publish = false`** to `Cargo.toml` for internal crates:
   ```toml
   [package]
   name = "my-internal-crate"
   publish = false  # Internal use only
   ```
3. **Document the decision**: Update this section if adding a new crate
4. **Follow naming conventions**: Public crates use `dsq-*` prefix, internal crates may vary

### Versioning Strategy

- All published crates follow semantic versioning (SemVer)
- Workspace crates are versioned together for consistency
- Breaking changes in any crate trigger a major version bump for all
- See RELEASING.md for detailed versioning procedures

## Regression Testing Policy

To maintain code quality and prevent bugs from reoccurring:

### Requirements

**Every bug fix MUST include a regression test.**

When fixing a bug:

1. **Write a failing test** that reproduces the bug
2. **Fix the bug** in the codebase
3. **Verify the test passes** with your fix
4. **Include the test** in your pull request

### Test Organization

Place regression tests in appropriate locations:

- **Unit-level bugs**: Add test to the module where the bug was fixed
- **Integration-level bugs**: Add test to `tests/regression/` directory
- **CLI bugs**: Add test using `assert_cmd` in `tests/cli/`

### Regression Test Format

Mark regression tests clearly:

```rust
#[test]
fn test_issue_123_csv_header_parsing() {
    // Regression test for issue #123
    // Bug: CSV headers with spaces were incorrectly parsed
    // ...test implementation...
}
```

### Benefits

This policy ensures:
- Bugs don't resurface in future releases
- Test coverage increases over time
- Bug fixes are verified and documented
- Contributors understand the fix's impact

## Release Process

For maintainers releasing new versions:

1. Update version numbers in all Cargo.toml files
2. Update CHANGELOG.md with release notes
3. Create a git tag for the version
4. Publish crates to crates.io (skip internal crates)
5. Create a GitHub release

See RELEASING.md for detailed release procedures.

## Questions?

If you have questions about contributing:

- Check the documentation in the `docs/` directory
- Look at existing code for examples
- Ask in GitHub discussions
- Open an issue for clarification

## License

By contributing to dsq, you agree that your contributions will be licensed under the same license as the project (MIT OR Apache-2.0).

---

Thank you for contributing to dsq! Your efforts help make this project better for everyone.
# Pull Request

## Description

Provide a clear and concise description of what this PR does.

Fixes #(issue number)

## Type of Change

Please check the type of change your PR introduces:

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Code refactoring (no functional changes)
- [ ] Build/CI configuration change
- [ ] Other (please describe):

## Changes Made

List the main changes made in this PR:

- Change 1
- Change 2
- Change 3

## Testing

### Test Coverage

- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All existing tests pass
- [ ] New tests cover the changes made

### Manual Testing

Describe the testing you have performed:

```bash
# Commands used for testing
dsq '...' test.csv
```

**Test results**:
- [ ] Tested on Linux
- [ ] Tested on macOS
- [ ] Tested on Windows
- [ ] Tested with multiple data formats (CSV, JSON, etc.)

## Documentation

- [ ] Updated README.md (if user-facing changes)
- [ ] Updated CHANGELOG.md
- [ ] Added/updated rustdoc comments for public APIs
- [ ] Added/updated code examples
- [ ] Documentation builds without warnings

## Code Quality

- [ ] Code follows the project's style guidelines (`cargo fmt`)
- [ ] No new clippy warnings (`cargo clippy -- -D warnings`)
- [ ] Code is well-commented and understandable
- [ ] Appropriate error handling is in place
- [ ] No new unsafe code (or properly documented if necessary)

## Performance Impact

Does this change affect performance?

- [ ] No performance impact
- [ ] Performance improvement (include benchmarks)
- [ ] Potential performance regression (justified because...)

**Benchmark results** (if applicable):

```
Before: [benchmark output]
After: [benchmark output]
```

## Breaking Changes

Does this PR introduce any breaking changes?

- [ ] No breaking changes
- [ ] Breaking changes (describe migration path):

**Migration guide** (if applicable):

```rust
// Before
old_api();

// After
new_api();
```

## Dependencies

- [ ] No new dependencies added
- [ ] New dependencies added (list and justify):
  - `dependency-name`: reason for adding

## Screenshots/Examples

If applicable, add screenshots or example output to demonstrate the changes:

```
[example output or screenshots]
```

## Checklist

- [ ] I have read the [CONTRIBUTING.md](../CONTRIBUTING.md) guidelines
- [ ] My code follows the project's code style
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] My changes generate no new warnings
- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] New and existing unit tests pass locally with my changes
- [ ] Any dependent changes have been merged and published

## Additional Notes

Add any additional notes, context, or concerns for reviewers:

## Reviewer Notes

Areas that need special attention during review:

- Area 1
- Area 2

Questions for reviewers:

- Question 1
- Question 2

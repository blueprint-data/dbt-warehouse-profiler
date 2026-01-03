# Release Workflow

This package uses semantic-release to automate versioning and releases.

## How It Works

1. **Commit Convention**: Use conventional commits to trigger releases:
   - `feat:` - New feature (minor version bump, e.g., 0.1.0 → 0.2.0)
   - `fix:` - Bug fix (patch version bump, e.g., 0.1.0 → 0.1.1)
   - `perf:` - Performance improvement (patch)
   - `refactor:` - Code refactoring (patch)
   - `docs:`, `test:`, `chore:` - No version bump

2. **Automatic Release**:
   - When commits are pushed to `main`, the Release workflow runs
   - semantic-release analyzes commits and determines the next version
   - A new GitHub release is created with:
     - Version tag (e.g., v0.1.0)
     - Release notes (from commits)
     - Updated CHANGELOG.md

3. **Version Update**:
   - After a release, `dbt_project.yml` version is automatically updated

## Example Workflow

```bash
# Make changes
git add .
git commit -m "feat: add Snowflake support"
git push origin main
```

This will trigger a minor version bump (e.g., v0.1.0 → v0.2.0).

```bash
# Fix a bug
git add .
git commit -m "fix: correct column type detection"
git push origin main
```

This will trigger a patch version bump (e.g., v0.2.0 → v0.2.1).

## Skipping CI

Release commits include `[skip ci]` to avoid unnecessary workflow runs.

## First Release

The first release (v0.1.0) will be created manually. After that, all releases are automated.

# GitHub Pages Deployment

This repository automatically deploys dbt documentation to GitHub Pages on every push to `main` branch.

## Documentation URL

The documentation will be available at: `https://blueprintdata.github.io/dbt-warehouse-profiler/`

## How It Works

The CI workflow has two jobs:

### 1. **Lint and Test** (runs on PRs and pushes)
- Validates dbt project structure
- Generates documentation
- Uploads documentation as an artifact

### 2. **Deploy to GitHub Pages** (runs only on pushes to main)
- Regenerates documentation
- Deploys to GitHub Pages automatically

## Flags Used

- `--empty-catalog`: Skips database queries (no database connection needed, but profile.yml still required)
- `--static`: Generates a single `static_index.html` file for easy hosting

## Setting Up GitHub Pages (One-time setup)

1. Go to your repository **Settings** → **Pages**
2. Under **Source**, select **GitHub Actions** (not Deploy from a branch)
3. Save the changes

That's it! The workflow will handle everything else.

## Manual Documentation Generation

To generate documentation locally:

```bash
cd integration_tests
dbt docs generate
```

This will create `target/` directory with:
- `index.html` - Main documentation page
- `manifest.json` - Project manifest
- `catalog.json` - Database catalog (requires database connection)
- `graph/` - Lineage graphs

To view locally:
```bash
dbt docs serve
```

Or with static file (no server needed):
```bash
dbt docs generate --static
open target/static_index.html
```

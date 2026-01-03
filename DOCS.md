# GitHub Pages Deployment

This repository automatically deploys dbt documentation to GitHub Pages on every push to the `main` branch.

## Documentation URL

The documentation will be available at: `https://blueprintdata.github.io/dbt-warehouse-profiler/`

## How It Works

1. On every push to `main`, the `Deploy Documentation` workflow runs
2. It generates dbt docs using `dbt docs generate --empty-catalog --static`
3. The `--empty-catalog` flag skips database queries (no authentication needed)
4. The `--static` flag generates a single `static_index.html` file
5. The file is automatically deployed to GitHub Pages

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

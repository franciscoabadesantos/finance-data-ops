# Legacy Public Product Surface Migrations

This directory is an archive. These SQL files documented or migrated retired
public product tables, materialized views, and refresh functions from the old
runtime model.

Do not run these files for fresh installs or normal Data Ops refreshes. The
current fresh-install schema is `sql/000_definitive_runtime_schema.sql`.

For an existing database that still has the retired objects, use the explicit
manual cleanup script:

```bash
psql "$DATA_OPS_DATABASE_URL" -f sql/019_retire_legacy_public_product_surfaces.sql
```

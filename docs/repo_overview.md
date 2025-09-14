# UKGovComms – Repository Overview
_Generated: 2025-09-14 01:06_

This document was generated automatically by `tools/generate_repo_docs.py`.

## Top-level folders
- **tools/** — Command-line utilities: crawlers, renderers, exporters, health/check scripts.
- **tools/legacy/** — Older one-off scripts kept for reference.
- **templates/** — Flask/Jinja2 HTML templates used to render pages.
- **static/** — Static assets (CSS, JS, images).
- **assets/** — Generated visualisations and wordclouds (per-source and global).
- **exports/** — Exported CSV/ZIP bundles and generated documents.
- **logs/** — Runtime logs from crawlers/renderers.
- **charts/** — Legacy chart output (mostly superseded by assets/).
- **.github/workflows/** — GitHub Actions workflows (deploy, docs, etc.).

## Command-line tools (`tools/`)
| Script | Description | Notable flags |
|---|---|---|
| `build_source_exports.py` | Export data for all kinds in Source into a single ZIP. | `--include-disabled` `--keep` `--max-age-days` |
| `export_govuk_blogs.py` | Export the GOV.UK blogs directory to CSV (and optionally DB). | `--log-level` `--out` `--write-db` |
| `export_sources_csv.py` | Export Source table to CSV | `--enabled` `--kind` `--log-level` `--out` |
| `fetch_blogs_from_db.py` | Fetch blog posts into DB. | `--force` `--log-level` `--max-posts` `--only-host` `--sleep` `--start-url` |
| `fetch_youtube_videos.py` | Fetch YouTube videos for Source(kind='YouTube'). | `--dry-run` `--log-level` `--max` `--only-channel-id` `--only-name` `--only-source-id` `--only-url` `--playlists-limit` `--playlists-only` `--since` `--sleep` `--uploads-only` |
| `generate_repo_docs.py` | Generate a Markdown overview of the repository. | `--log-level` `--out` `--root` |
| `import_youtube_sources.py` | — | `--csv` |
| `render_assets_for_updates.py` | Render assets where new items exist and/or assets are missing. | `--catch-up-missing` `--kind` `--log-level` `--only-host` |
| `render_global_assets.py` | Render aggregate charts + wordcloud across all enabled sources. | `--log-level` `--only-wordcloud` `--rolling-days` |
| `render_source_assets.py` | Render charts + wordcloud for a single Source. | `--host` `--id` `--log-level` `--only-wordcloud` `--outdir` `--rolling-days` |
| `source_health_report.py` | Report data health for a Source (Blog). | `--host` `--id` `--url` |
| `yt_backfill_channel_ids.py` | — | — |

### Legacy (`tools/legacy/`)
- `crawl_blog.py` — Crawl all posts for a *.blog.gov.uk site by following the left-arrow (previous) link.
- `plot_blog_frequency.py` — Plot monthly counts and rolling average for a GOV.UK blog from BlogPost.
- `wordcloud_gds_blog.py` — Create a word cloud from GDS blog post titles.

## Templates (`templates/`)
| Template | Title / summary |
|---|---|
| `404.html` | 404 - Page Not Found |
| `add.html` | Add New Record to {{ table }} |
| `admin.html` | Admin Interface |
| `base.html` | {% block title %}UK Gov Comms{% endblock %} |
| `bestpractice.html` | Best practice |
| `datavis.html` | Datavis |
| `downloads.html` | Downloads |
| `edit.html` | Edit Record in {{ table }} |
| `gate.html` | Access Gate |
| `index.html` | — |
| `signatories.html` | Signatories |
| `thankyou.html` | Thank you! |
| `video.html` | Contact |

## Flask routes (`app.py`)
| Route | Methods | View | Docstring |
|---|---|---|---|
| `/` | GET | `home` | — |
| `/admin/add/<table>` | GET, POST | `add_record` | — |
| `/admin/delete/<table>/<int:record_id>` | GET | `delete_record` | — |
| `/admin/edit/<table>/<int:record_id>` | GET, POST | `edit_record` | — |
| `/assets/<path:filename>` | GET | `serve_assets` | — |
| `/bestpractice` | GET | `bestpractice` | — |
| `/datavis` | GET | `datavis` | — |
| `/download/<path:filename>` | GET | `download_file` | — |
| `/downloads` | GET | `downloads_index` | — |
| `/gate` | GET, POST | `gate` | — |
| `/signatories` | GET | `signatories` | — |
| `/silent-pebble-echo` | GET | `admin` | — |
| `/tbd` | GET | `tbd` | — |
| `/thank-you` | GET | `thankyou` | — |

## Notable files
- `wsgi.py`
- `requirements.txt`
- `.github/workflows/deploy.yml`

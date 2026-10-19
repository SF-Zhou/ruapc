# RuaPC landing page

A single, dependency-free static page for RuaPC. It is deployed from this
directory by [`.github/workflows/pages.yml`](../.github/workflows/pages.yml).

## Preview locally

```bash
python3 -m http.server 4173 --directory site
```

Open <http://127.0.0.1:4173/>. Relative asset paths also make the page work
under the project Pages prefix (`/ruapc/`).

## Validate

```bash
python3 site/scripts/check-site.py
```

The checker verifies local links and assets, in-page anchors, duplicate ids,
ARIA references, required social metadata and the canonical URL. CI runs it on
every pull request and before every deployment.

## Deploy

1. In the repository, open **Settings → Pages → Build and deployment** and set
   **Source** to **GitHub Actions**. This is a one-time repository setting; the
   workflow token cannot enable Pages on its own.
2. Push a change under `site/` to `main`, or run the **Landing page** workflow
   manually from the Actions tab.

The site publishes at <https://sf-zhou.github.io/ruapc/>. Pull requests run the
checker but do not deploy.

## Structure

| Path | Purpose |
|---|---|
| `index.html` | The complete page and its metadata. |
| `assets/styles.css` | Design tokens, layout, motion and responsive rules. |
| `assets/main.js` | Transport preview, code tabs, copy buttons, navigation and reveals. |
| `assets/fonts/` | Self-hosted Bricolage Grotesque, Instrument Serif and IBM Plex Mono, with their OFL licenses. |
| `assets/img/` | Optimized mascot and social preview artwork. |
| `scripts/check-site.py` | Dependency-free pre-deploy validation. |
| `robots.txt`, `sitemap.xml` | Crawler metadata for the project Pages URL. |

There is no build step. The workflow stages `site/` as-is, excluding this
README and the checker, then uploads it as the Pages artifact.

## Updating

- The release version appears in the hero eyebrow, both install commands,
  social metadata, structured data and the OG artwork. Update them together
  when the crate version changes.
- The canonical URL, Open Graph metadata, sitemap and `robots.txt` use
  `https://sf-zhou.github.io/ruapc/`. Update all of them (and the root README
  link) together if the site moves to a custom domain.
- Font files are self-hosted to avoid third-party requests. Keep the OFL
  license files next to them.
- To regenerate the mascot WebP assets from `docs/logo.png`, use Pillow or a
  comparable image tool; do not link the original 1.6 MB PNG from the page.

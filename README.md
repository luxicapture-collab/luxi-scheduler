# Luxi Scheduler

A self-hosted post scheduler for Instagram, LinkedIn and X, running on the `luxi-style` Supabase project.

## How it works
- `dashboard/index.html` is the dashboard. Open it in a browser, sign in, upload a video or image, write captions and pick a time.
- Media goes to the public `social-media` storage bucket under a random file name. Instagram needs a public URL to fetch it.
- Every 2 minutes `pg_cron` calls the `social-publish` edge function. It publishes due posts, waits for each platform to process videos, retries temporary errors up to 3 times, and records links and errors.
- Instagram tokens refresh automatically. LinkedIn tokens last 60 days: paste a new one with "Update token" before they expire. X keys don't expire.

## Database (all tables prefixed `social_`)
| table | purpose |
|---|---|
| `social_admins` | emails allowed to use the dashboard |
| `social_accounts` | connected accounts (no secrets) |
| `social_account_secrets` | tokens; the dashboard can write them but never read them back |
| `social_posts` / `social_post_targets` | the post, and one row per account it goes to |
| `social_logs` | publish history |
| `social_config` | cron secret (server only) |

## Limits
- Free Supabase plan: 50 MB per uploaded file. Edge functions have about 150 s per run, so very large videos to LinkedIn or X may time out.
- X captions max 280 characters; use the per-account "Custom caption".
- Add a teammate: `insert into social_admins (email) values ('name@example.com');`
- LinkedIn API version is set by the `LINKEDIN_VERSION` function secret (default `202608`). Bump it if LinkedIn reports the version as unsupported.

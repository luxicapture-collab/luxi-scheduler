// Social scheduler publisher. Called every 2 minutes by pg_cron.
// Picks due post targets and publishes them to Instagram, LinkedIn or X.
import { createClient } from "jsr:@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const db = createClient(SUPABASE_URL, Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!);

const IG_API = "https://graph.instagram.com/v23.0";
const LI_API = "https://api.linkedin.com/rest";
const LI_VERSION = Deno.env.get("LINKEDIN_VERSION") ?? "202608";
const X_API = "https://api.x.com/2";
const MAX_ATTEMPTS = 3;
const MAX_POLLS = 40;

type Account = { id: string; platform: string; name: string; external_id: string };
type Post = { id: string; caption: string; media_path: string | null; media_type: string | null; title: string | null };
type Target = {
  id: string; post_id: string; account_id: string; caption: string | null; status: string;
  attempts: number; state: Record<string, any>;
};
type Result =
  | { done: true; externalId: string; permalink?: string }
  | { done: false; state: Record<string, any>; retryInSeconds: number };

class PermanentError extends Error {}

Deno.serve(async (req) => {
  const { data: cfg } = await db.from("social_config").select("value").eq("key", "cron_secret").single();
  if (!cfg || req.headers.get("x-cron-secret") !== cfg.value) {
    return new Response("unauthorized", { status: 401 });
  }

  const report: string[] = [];
  const now = new Date().toISOString();
  const { data: due, error } = await db
    .from("social_post_targets")
    .select("*, post:social_posts!inner(*), account:social_accounts!inner(*)")
    .in("status", ["queued", "processing"])
    .eq("post.status", "scheduled")
    .lte("post.scheduled_at", now)
    .eq("account.active", true)
    .or(`next_attempt_at.is.null,next_attempt_at.lte.${now}`)
    .limit(5);
  if (error) return json({ error: error.message }, 500);

  const touchedPosts = new Set<string>();
  for (const row of due ?? []) {
    const { post, account, ...target } = row as Target & { post: Post; account: Account };
    // Claim the row so an overlapping run skips it.
    const lease = new Date(Date.now() + 10 * 60_000).toISOString();
    let claim = db.from("social_post_targets").update({ next_attempt_at: lease }).eq("id", target.id);
    claim = row.next_attempt_at ? claim.eq("next_attempt_at", row.next_attempt_at) : claim.is("next_attempt_at", null);
    const { data: claimed } = await claim.select("id");
    if (!claimed?.length) continue;

    touchedPosts.add(post.id);
    try {
      const creds = await loadCredentials(account.id);
      const caption = target.caption ?? post.caption ?? "";
      const result = await publish(account, creds, post, caption, target.state ?? {});
      if (result.done) {
        await db.from("social_post_targets").update({
          status: "posted", external_post_id: result.externalId, permalink: result.permalink ?? null,
          error: null, posted_at: new Date().toISOString(), next_attempt_at: null, updated_at: new Date().toISOString(),
        }).eq("id", target.id);
        await log(target.id, "info", `Posted to ${account.platform} (${account.name})`);
        report.push(`${target.id}: posted`);
      } else {
        const polls = (result.state.polls ?? 0) as number;
        if (polls > MAX_POLLS) throw new PermanentError("Platform took too long to process the media");
        await db.from("social_post_targets").update({
          status: "processing", state: result.state, updated_at: new Date().toISOString(),
          next_attempt_at: new Date(Date.now() + result.retryInSeconds * 1000).toISOString(),
        }).eq("id", target.id);
        report.push(`${target.id}: processing`);
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      const attempts = target.attempts + 1;
      const giveUp = e instanceof PermanentError || attempts >= MAX_ATTEMPTS;
      await db.from("social_post_targets").update({
        status: giveUp ? "failed" : "queued", attempts, error: msg, state: {},
        next_attempt_at: giveUp ? null : new Date(Date.now() + attempts * 5 * 60_000).toISOString(),
        updated_at: new Date().toISOString(),
      }).eq("id", target.id);
      await log(target.id, "error", msg);
      report.push(`${target.id}: error ${msg}`);
    }
  }

  for (const postId of touchedPosts) await settlePost(postId);
  await refreshInstagramTokens(report);
  return json({ processed: report });
});

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

async function log(targetId: string, level: string, message: string) {
  await db.from("social_logs").insert({ target_id: targetId, level, message: message.slice(0, 2000) });
}

async function loadCredentials(accountId: string): Promise<Record<string, string>> {
  const { data } = await db.from("social_account_secrets").select("credentials").eq("account_id", accountId).single();
  if (!data) throw new PermanentError("No credentials saved for this account");
  return data.credentials;
}

async function settlePost(postId: string) {
  const { data } = await db.from("social_post_targets").select("status").eq("post_id", postId);
  const statuses = (data ?? []).map((t) => t.status).filter((s) => s !== "cancelled");
  if (statuses.some((s) => s === "queued" || s === "processing")) return;
  const posted = statuses.filter((s) => s === "posted").length;
  const status = posted === statuses.length ? "done" : posted === 0 ? "failed" : "partial";
  await db.from("social_posts").update({ status }).eq("id", postId);
}

function mediaUrl(path: string) {
  return `${SUPABASE_URL}/storage/v1/object/public/social-media/${path.split("/").map(encodeURIComponent).join("/")}`;
}

function mimeOf(path: string, type: string | null) {
  const ext = path.split(".").pop()?.toLowerCase();
  const map: Record<string, string> = {
    mp4: "video/mp4", mov: "video/quicktime", jpg: "image/jpeg", jpeg: "image/jpeg", png: "image/png", webp: "image/webp",
  };
  return map[ext ?? ""] ?? (type === "video" ? "video/mp4" : "image/jpeg");
}

async function downloadMedia(path: string) {
  const { data, error } = await db.storage.from("social-media").download(path);
  if (error || !data) throw new Error(`Could not read media file: ${error?.message}`);
  return new Uint8Array(await data.arrayBuffer());
}

async function check(res: Response, what: string) {
  if (res.ok) return res;
  const body = await res.text();
  const msg = `${what} failed (${res.status}): ${body.slice(0, 800)}`;
  // 4xx other than rate limiting will not fix itself on retry.
  if (res.status >= 400 && res.status < 500 && res.status !== 429) throw new PermanentError(msg);
  throw new Error(msg);
}

function publish(account: Account, creds: Record<string, string>, post: Post, caption: string, state: Record<string, any>) {
  switch (account.platform) {
    case "instagram": return publishInstagram(account, creds, post, caption, state);
    case "linkedin": return publishLinkedIn(account, creds, post, caption, state);
    case "x": return publishX(account, creds, post, caption, state);
  }
  throw new PermanentError(`Unknown platform ${account.platform}`);
}

// ---------- Instagram (Instagram API with Instagram Login) ----------

async function publishInstagram(account: Account, creds: Record<string, string>, post: Post, caption: string, state: Record<string, any>): Promise<Result> {
  if (!post.media_path) throw new PermanentError("Instagram posts need a video or image");
  const token = creds.access_token;
  const igId = account.external_id;

  if (!state.container) {
    const params = new URLSearchParams({ caption, access_token: token });
    if (post.media_type === "video") {
      params.set("media_type", "REELS");
      params.set("video_url", mediaUrl(post.media_path));
      params.set("share_to_feed", "true");
    } else {
      params.set("image_url", mediaUrl(post.media_path));
    }
    const res = await check(await fetch(`${IG_API}/${igId}/media`, { method: "POST", body: params }), "Instagram create container");
    const { id } = await res.json();
    return { done: false, state: { container: id, polls: 0 }, retryInSeconds: post.media_type === "video" ? 60 : 5 };
  }

  const statusRes = await check(
    await fetch(`${IG_API}/${state.container}?fields=status_code,status&access_token=${encodeURIComponent(token)}`),
    "Instagram container status",
  );
  const { status_code, status } = await statusRes.json();
  if (status_code === "ERROR" || status_code === "EXPIRED") throw new PermanentError(`Instagram rejected the media: ${status ?? status_code}`);
  if (status_code !== "FINISHED") return { done: false, state: { ...state, polls: (state.polls ?? 0) + 1 }, retryInSeconds: 60 };

  const pubRes = await check(await fetch(`${IG_API}/${igId}/media_publish`, {
    method: "POST", body: new URLSearchParams({ creation_id: state.container, access_token: token }),
  }), "Instagram publish");
  const { id } = await pubRes.json();
  let permalink: string | undefined;
  try {
    const r = await fetch(`${IG_API}/${id}?fields=permalink&access_token=${encodeURIComponent(token)}`);
    permalink = (await r.json()).permalink;
  } catch { /* permalink is optional */ }
  return { done: true, externalId: id, permalink };
}

async function refreshInstagramTokens(report: string[]) {
  const soon = new Date(Date.now() + 15 * 86_400_000).toISOString();
  const dayAgo = new Date(Date.now() - 86_400_000).toISOString();
  const { data: accounts } = await db.from("social_accounts").select("id, name, token_expires_at")
    .eq("platform", "instagram").eq("active", true)
    .or(`token_expires_at.is.null,token_expires_at.lte.${soon}`);
  for (const acc of accounts ?? []) {
    const { data: sec } = await db.from("social_account_secrets").select("credentials, updated_at")
      .eq("account_id", acc.id).single();
    if (!sec || sec.updated_at > dayAgo) continue;
    try {
      const res = await check(await fetch(
        `https://graph.instagram.com/refresh_access_token?grant_type=ig_refresh_token&access_token=${encodeURIComponent(sec.credentials.access_token)}`,
      ), "Instagram token refresh");
      const { access_token, expires_in } = await res.json();
      await db.from("social_account_secrets").update({
        credentials: { ...sec.credentials, access_token }, updated_at: new Date().toISOString(),
      }).eq("account_id", acc.id);
      await db.from("social_accounts").update({
        token_expires_at: new Date(Date.now() + expires_in * 1000).toISOString(), last_error: null,
      }).eq("id", acc.id);
      report.push(`refreshed Instagram token for ${acc.name}`);
    } catch (e) {
      await db.from("social_account_secrets").update({ updated_at: new Date().toISOString() }).eq("account_id", acc.id);
      await db.from("social_accounts").update({ last_error: String(e).slice(0, 500) }).eq("id", acc.id);
    }
  }
}

// ---------- LinkedIn (Posts API) ----------

function liHeaders(token: string, json = true) {
  const h: Record<string, string> = {
    Authorization: `Bearer ${token}`, "LinkedIn-Version": LI_VERSION, "X-Restli-Protocol-Version": "2.0.0",
  };
  if (json) h["Content-Type"] = "application/json";
  return h;
}

// LinkedIn "little text" format: escape reserved characters, keep #hashtags clickable.
function liCommentary(text: string) {
  return text.split(/(#[\p{L}\p{N}_]+)/u)
    .map((part, i) => i % 2 ? `{hashtag|\\#|${part.slice(1)}}` : part.replace(/[\\|{}@\[\]()<>#*_~]/g, (c) => `\\${c}`))
    .join("");
}

async function publishLinkedIn(account: Account, creds: Record<string, string>, post: Post, caption: string, state: Record<string, any>): Promise<Result> {
  const token = creds.access_token;
  const owner = account.external_id;
  let mediaUrn: string | undefined = state.media;

  if (post.media_path && !mediaUrn) {
    const bytes = await downloadMedia(post.media_path);
    if (post.media_type === "video") {
      const init = await check(await fetch(`${LI_API}/videos?action=initializeUpload`, {
        method: "POST", headers: liHeaders(token),
        body: JSON.stringify({ initializeUploadRequest: { owner, fileSizeBytes: bytes.length, uploadCaptions: false, uploadThumbnail: false } }),
      }), "LinkedIn video init");
      const { value } = await init.json();
      const etags: string[] = [];
      for (const part of value.uploadInstructions) {
        const res = await check(await fetch(part.uploadUrl, {
          method: "PUT", headers: { "Content-Type": "application/octet-stream" },
          body: bytes.slice(part.firstByte, part.lastByte + 1),
        }), "LinkedIn video upload");
        etags.push(res.headers.get("etag") ?? "");
      }
      await check(await fetch(`${LI_API}/videos?action=finalizeUpload`, {
        method: "POST", headers: liHeaders(token),
        body: JSON.stringify({ finalizeUploadRequest: { video: value.video, uploadToken: value.uploadToken ?? "", uploadedPartIds: etags } }),
      }), "LinkedIn video finalize");
      return { done: false, state: { media: value.video, polls: 0 }, retryInSeconds: 30 };
    }
    const init = await check(await fetch(`${LI_API}/images?action=initializeUpload`, {
      method: "POST", headers: liHeaders(token), body: JSON.stringify({ initializeUploadRequest: { owner } }),
    }), "LinkedIn image init");
    const { value } = await init.json();
    await check(await fetch(value.uploadUrl, { method: "PUT", headers: { Authorization: `Bearer ${token}` }, body: bytes }), "LinkedIn image upload");
    mediaUrn = value.image;
  }

  if (mediaUrn && post.media_type === "video") {
    const res = await check(await fetch(`${LI_API}/videos/${encodeURIComponent(mediaUrn)}`, { headers: liHeaders(token, false) }), "LinkedIn video status");
    const { status } = await res.json();
    if (status === "PROCESSING_FAILED") throw new PermanentError("LinkedIn could not process the video");
    if (status !== "AVAILABLE") return { done: false, state: { ...state, polls: (state.polls ?? 0) + 1 }, retryInSeconds: 30 };
  }

  const body: Record<string, unknown> = {
    author: owner,
    commentary: liCommentary(caption),
    visibility: "PUBLIC",
    distribution: { feedDistribution: "MAIN_FEED", targetEntities: [], thirdPartyDistributionChannels: [] },
    lifecycleState: "PUBLISHED",
    isReshareDisabledByAuthor: false,
  };
  if (mediaUrn) body.content = { media: { id: mediaUrn, ...(post.title ? { title: post.title } : {}) } };
  const res = await check(await fetch(`${LI_API}/posts`, { method: "POST", headers: liHeaders(token), body: JSON.stringify(body) }), "LinkedIn create post");
  const urn = res.headers.get("x-restli-id") ?? "";
  return { done: true, externalId: urn, permalink: urn ? `https://www.linkedin.com/feed/update/${urn}` : undefined };
}

// ---------- X (API v2, OAuth 1.0a user context) ----------

const rfc3986 = (s: string) => encodeURIComponent(s).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);

async function oauthHeader(method: string, url: string, creds: Record<string, string>) {
  const u = new URL(url);
  const oauth: Record<string, string> = {
    oauth_consumer_key: creds.consumer_key,
    oauth_nonce: crypto.randomUUID().replace(/-/g, ""),
    oauth_signature_method: "HMAC-SHA1",
    oauth_timestamp: Math.floor(Date.now() / 1000).toString(),
    oauth_token: creds.access_token,
    oauth_version: "1.0",
  };
  const params: [string, string][] = [...Object.entries(oauth), ...u.searchParams.entries()];
  const paramStr = params.map(([k, v]) => [rfc3986(k), rfc3986(v)]).sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] < b[1] ? -1 : 1)
    .map(([k, v]) => `${k}=${v}`).join("&");
  const base = `${method.toUpperCase()}&${rfc3986(u.origin + u.pathname)}&${rfc3986(paramStr)}`;
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(`${rfc3986(creds.consumer_secret)}&${rfc3986(creds.access_token_secret)}`),
    { name: "HMAC", hash: "SHA-1" }, false, ["sign"]);
  const sig = new Uint8Array(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(base)));
  oauth.oauth_signature = btoa(String.fromCharCode(...sig));
  return "OAuth " + Object.entries(oauth).map(([k, v]) => `${rfc3986(k)}="${rfc3986(v)}"`).join(", ");
}

async function xFetch(method: string, url: string, creds: Record<string, string>, body?: BodyInit, contentType?: string) {
  const headers: Record<string, string> = { Authorization: await oauthHeader(method, url, creds) };
  if (contentType) headers["Content-Type"] = contentType;
  return fetch(url, { method, headers, body });
}

async function publishX(account: Account, creds: Record<string, string>, post: Post, caption: string, state: Record<string, any>): Promise<Result> {
  if ([...caption].length > 280) throw new PermanentError(`X caption is ${[...caption].length} characters; the limit is 280. Add a shorter X caption.`);
  let mediaId: string | undefined = state.media;

  if (post.media_path && !mediaId) {
    const bytes = await downloadMedia(post.media_path);
    const mime = mimeOf(post.media_path, post.media_type);
    const category = post.media_type === "video" ? "tweet_video" : "tweet_image";
    const init = await check(await xFetch("POST", `${X_API}/media/upload/initialize`, creds,
      JSON.stringify({ media_type: mime, total_bytes: bytes.length, media_category: category }), "application/json"), "X media init");
    mediaId = (await init.json()).data.id as string;
    const chunk = 4 * 1024 * 1024;
    for (let i = 0, seg = 0; i < bytes.length; i += chunk, seg++) {
      const form = new FormData();
      form.append("media", new Blob([bytes.slice(i, i + chunk)], { type: mime }));
      form.append("segment_index", String(seg));
      await check(await xFetch("POST", `${X_API}/media/upload/${mediaId}/append`, creds, form), "X media append");
    }
    const fin = await check(await xFetch("POST", `${X_API}/media/upload/${mediaId}/finalize`, creds), "X media finalize");
    const info = (await fin.json()).data?.processing_info;
    if (info && info.state !== "succeeded") {
      return { done: false, state: { media: mediaId, polls: 0 }, retryInSeconds: Math.max(info.check_after_secs ?? 10, 10) };
    }
  } else if (mediaId) {
    const res = await check(await xFetch("GET", `${X_API}/media/upload?command=STATUS&media_id=${mediaId}`, creds), "X media status");
    const info = (await res.json()).data?.processing_info;
    if (info?.state === "failed") throw new PermanentError(`X could not process the media: ${JSON.stringify(info.error ?? {})}`);
    if (info && info.state !== "succeeded") {
      return { done: false, state: { ...state, polls: (state.polls ?? 0) + 1 }, retryInSeconds: Math.max(info.check_after_secs ?? 10, 10) };
    }
  }

  const body: Record<string, unknown> = { text: caption };
  if (mediaId) body.media = { media_ids: [mediaId] };
  const res = await check(await xFetch("POST", `${X_API}/tweets`, creds, JSON.stringify(body), "application/json"), "X create post");
  const id = (await res.json()).data.id as string;
  return { done: true, externalId: id, permalink: `https://x.com/i/web/status/${id}` };
}

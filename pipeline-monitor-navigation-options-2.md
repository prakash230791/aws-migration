# Pipeline Monitor — Navigation Options & Recommendation

**Route:** `/app/pipeline-monitor` (statically rendered)
**Symptom:** After the tab idles, in-app navigation (tab clicks, breadcrumbs, in-panel back arrow) silently stops responding in production. Native browser Back always recovers. Not reproducible in dev.

---

## Option 0 — `KEEP_ALIVE_TIMEOUT` in `web.config` (infrastructure)

**This is the strongest candidate found so far and should be tried first.** It is one line, requires no code change, and is officially documented by Next.js.

### The mechanism

Node's HTTP server defaults to `keepAliveTimeout = 5000ms`. Next's own CLI documentation states that when deploying behind a downstream proxy, Next's underlying HTTP server must be configured with keep-alive timeouts *larger* than the proxy's — otherwise Node silently terminates the TCP connection without telling the proxy, and the proxy errors when it next tries to reuse it.

Here, `httpPlatformHandler` (IIS's reverse proxy to the Node process) pools connections to Node. `KEEP_ALIVE_TIMEOUT` is unset, so any pooled connection idle for more than 5 seconds has already been torn down on the Node side while IIS still believes it is usable.

### The fix

The standalone `server.js` already reads the value from the environment — no code change required:

```js
let keepAliveTimeout = parseInt(process.env.KEEP_ALIVE_TIMEOUT, 10)
startServer({ ..., keepAliveTimeout })
```

Add to `web.config`, alongside the existing `PORT` / `HOSTNAME` / `NODE_ENV` / `LOG_DIR` entries:

```xml
<environmentVariable name="KEEP_ALIVE_TIMEOUT" value="70000"/>
```

Then redeploy / restart the app pool.

**Effort:** Trivial · **Risk:** Very low · **Officially supported:** yes

### Verification notes

- Env-var support for standalone builds was added in vercel/next.js#46052 and corrected in #50221 (the value was initially not applied). Confirm your Next version includes the fix — check that the generated `.next/standalone/server.js` actually contains the `parseInt(process.env.KEEP_ALIVE_TIMEOUT, 10)` line, which it does per the pasted file.
- `next start --keepAliveTimeout 70000` is the equivalent for non-standalone deployments. Note `next start` cannot be used with `output: 'standalone'`, so pick whichever matches your build.
- `headersTimeout` still has no env-var equivalent. On Node 18+ this is generally not required; on older Node it may also need raising.

### Two caveats to keep in mind

1. **The documented symptom of this bug is 502 errors, not silent hangs.** Every report in the Next.js issue tracker describes "random 502s" from the load balancer. Your symptom is a fetch that never settles. A 502 returned to the browser would *resolve* the fetch, and the App Router would normally fall back to a hard navigation rather than freeze. It is plausible that `httpPlatformHandler` swallows the upstream reset instead of returning a 502, but that step is inferred, not observed.
2. **A 5-second window is very short relative to the reported behaviour.** If every pause longer than 5s risked a stale connection, failures would be frequent rather than occasional. The intermittency is explainable — IIS only fails when it happens to pick a stale pooled entry rather than opening a fresh one — but it is worth confirming rather than assuming.

**How to confirm:** check IIS / `httpPlatformHandler` logs and the Windows event log for upstream connection resets or 502s at the moments navigation wedged. Also check the response status of the pending `?_rsc=` request in the Network tab when stuck. If you see a 502 or a reset, this is confirmed. If the request simply hangs with no server-side error logged, the mechanism is still unproven.

Ship it regardless — it is nearly free, officially recommended, and correct practice for Node behind any proxy. But treat "confirmed root cause" as "strong hypothesis" until the logs back it up.

---

## Key finding that shapes every option

The URL that breaks:

```
/app/pipeline-monitor?tab=executions&execRunId=14703133&execView=app&execIcd=...
```

The breadcrumb calls `buildNavUrl({ execRunId: null })`, landing on:

```
/app/pipeline-monitor?tab=executions
```

**Same pathname, different query string.** So the breadcrumbs and the in-panel back arrow are *not* cross-route navigation — they are the same category as tab switching. All ~19 handlers in `execution-details.tsx` plus `handleTabChange` in `page.tsx` are firing a full RSC network round-trip in order to change a query parameter, on a route that is statically rendered and reads its params client-side.

That fetch is pure overhead. It is also the thing that wedges.

### Correction to the current plan

The plan states there is "no native shallow routing for query-param-only changes." This is not accurate for Next.js 14.1+. `window.history.pushState` / `replaceState` are the officially supported replacement for shallow routing: they update the URL **and** `useSearchParams` with no RSC fetch, no router transition, and no client router cache involvement.

This matters because the plan's entire premise is that every click must go over the network. For query-only navigation, it does not have to.

---

## Option 1 — Native `pushState` for query-only navigation

Replace `router.push(buildNavUrl(...))` with a direct history write. `buildNavUrl` is unchanged.

```ts
const navigate = useCallback((url: string) => {
  window.history.pushState(null, "", url);
}, []);
```

For the tab handler in `page.tsx`:

```ts
const handleTabChange = (value: string) => {
  const params = new URLSearchParams(searchParams.toString());
  params.set("tab", value);
  if (value !== "comparison") {
    params.delete("productA");
    params.delete("productB");
  }
  window.history.pushState(null, "", `?${params}`);
};
```

Also remove the `window.location.search` read and derive `activeTab` from `searchParams` only.

**Pros**
- Eliminates the failure mode rather than detecting and recovering from it
- No watchdog, timeout tuning, retry logic, idle-gating, or forced reload needed
- Creates real history entries, so native Back steps through states correctly
- Instant navigation — no network dependency at all
- Mechanical find-and-replace across ~20 call sites; smaller diff than the current plan

**Cons / prerequisite**
- Any component whose data depends on `execRunId` must fetch client-side (`useEffect` / SWR / React Query keyed on the param)
- **Verify before committing:** if a Server Component in that subtree reads `searchParams` to fetch data, `pushState` will not re-run it

**Effort:** Low · **Risk:** Low · **Behaviour change:** none visible to users

---

## Option 2 — `nuqs` library

Same underlying mechanism as Option 1, packaged behind typed hooks.

```ts
const [params, setParams] = useQueryStates({
  tab: parseAsString.withDefault("overview"),
  execRunId: parseAsString,
  execView: parseAsString,
}, { history: "push" });
```

**Pros**
- Typed params, batched updates, declarative drill-down clearing
- Replaces 19 hand-built URL strings with one schema
- Per-key control over push vs. replace

**Cons**
- New dependency (needs approval in a corporate environment)
- Same Server Component caveat as Option 1

**Effort:** Low–Medium · **Risk:** Low

---

## Option 3 — Promote drill-down to real routes

```
app/pipeline-monitor/executions/[execRunId]/[execView]/page.tsx
```

Breadcrumbs become plain `<Link href="/app/pipeline-monitor/executions">`.

**Pros**
- Architecturally what the App Router is designed for
- Real anchors: work without JS, prefetch correctly, right-click / open-in-new-tab work
- Per-level `loading.tsx` and `error.tsx`
- Proper segment-level caching

**Cons**
- Largest change by far — restructures the route tree and component boundaries
- Still uses the router, so if the wedge has a cause not yet identified, it may persist
- Server-rendered params change data-fetching assumptions throughout

**Effort:** High · **Risk:** Medium · Best filed as the correct long-term refactor, not this week's fix

---

## Option 4 — Client state with URL mirroring

Hold `execRunId` / `execView` in a reducer, render from state, mirror to the URL with `replaceState`, and listen for `popstate` to sync back.

```ts
useEffect(() => {
  const onPop = () => dispatch({
    type: "sync",
    params: new URLSearchParams(location.search),
  });
  window.addEventListener("popstate", onPop);
  return () => window.removeEventListener("popstate", onPop);
}, []);
```

**Pros**
- Fully decouples UI responsiveness from the network
- Instant navigation; URL remains shareable
- Works regardless of how data fetching is structured

**Cons**
- More code than Option 1
- You now own state ↔ URL synchronisation, including edge cases
- Easy to introduce subtle back/forward bugs

**Effort:** Medium · **Risk:** Medium

---

## Option 5 — Plain `<a href>` links

**Pros**
- Immune to router state by construction
- Zero framework surface area

**Cons**
- Full page load: loses table scroll position, expanded rows, in-memory state
- Refetches the 500-row `summary` payload on every breadcrumb click
- Effectively identical to what the watchdog eventually does, minus the 6-second wait

**Effort:** Very low · **Risk:** Low · Acceptable as an escape hatch, poor as the primary path

---

## Option 6 — Watchdog wrapper (`createResilientRouter`, current plan)

Wrap `push` / `replace` with a timeout that falls back to `window.location.assign` if the URL hasn't changed.

**Pros**
- Already written in `resilient-navigation.ts`; only needs wiring
- Two-line change per call site
- Genuinely useful for real cross-route navigation

**Cons and specific risks**
- Detects the failure instead of removing it; users still wait out the timeout
- **`router.refresh()` warm-up is counterproductive:** it refetches the current route *and* clears the client router cache. On a page pulling `page_size=500` that is a heavy refetch every time the tab regains focus. If the connection is genuinely dead, `refresh()` hangs too, potentially leaving a pending transition that the next real click queues behind — it may cause the wedge rather than prevent it.
- **Retry-before-hard-reload assumes a dead socket.** If the router's internal transition state is stuck, a second `push` queues behind the same unresolved promise and does nothing, adding 4–8s before the reload the user was going to get anyway.
- **`createResilientRouter(useRouter())` returns a new object every render.** Must be wrapped in `useMemo`, or it will cause re-render loops in any `useEffect` / `useCallback` dependency array.
- **Timer races:** a transition completing at 6.5s when the timer fired at 6.0s produces a completed navigation *plus* a hard reload.

**Effort:** Low · **Risk:** Medium (four interacting heuristics to tune and debug)

---

## Comparison

| Option | Effort | Risk | Removes root cause | Keeps SPA feel |
|---|---|---|---|---|
| 0 — `KEEP_ALIVE_TIMEOUT` | Trivial | Very low | Yes, if confirmed | Yes |
| 1 — `pushState` | Low | Low | Yes | Yes |
| 2 — `nuqs` | Low–Med | Low | Yes | Yes |
| 3 — Real routes | High | Med | No | Yes |
| 4 — Client state | Med | Med | Yes | Yes |
| 5 — `<a href>` | Very low | Low | Yes | No |
| 6 — Watchdog | Low | Med | No | Partly |

---

## Recommendation

Options 0 and 1 are complementary, not alternatives. Option 0 fixes the transport; Option 1 removes the dependency on the transport. Do both, in this order.

### Phase 1 — ship immediately

1. **Option 0: add `KEEP_ALIVE_TIMEOUT=70000` to `web.config`**, redeploy, restart the app pool. One line, no code change, officially documented. Highest value per unit of effort in this entire document.
2. **Collect evidence while doing it.** Capture IIS / `httpPlatformHandler` logs and the pending `?_rsc=` request status at the next wedge. This is what turns "strong hypothesis" into "confirmed root cause," and it determines whether Phases 2 and 3 are still necessary.
3. **Add `error.tsx` and `loading.tsx`** under `app/pipeline-monitor/`. Currently absent — this is why production failures are silent and indistinguishable from a dead click. Correct regardless of root cause, and it will make the next occurrence far easier to diagnose.

### Phase 2 — the durable fix

4. **Verify the prerequisite:** grep the `pipeline-monitor` subtree for Server Components reading `searchParams` for data fetching. If any exist, pair Option 1 with client-side fetching (Option 4's approach) for those components.
5. **Apply Option 1** to `page.tsx`, `execution-details.tsx`, `file-watcher.tsx`, `failed-accounts-detail-dialog.tsx`. Remove the `window.location.search` read and the unchanged-guard in `replaceCurrentUrlQuery`.

Rationale: every one of those ~20 navigations is query-only on a statically rendered route, so the RSC round-trip they currently perform is pure overhead. Even with a correct keep-alive setting, they remain exposed to the *external* corporate proxy in front of IIS, to backend latency, and to any future transport fault. `pushState` makes them synchronous and local — nothing to hang, nothing to time out, nothing to tune. It also makes the page measurably faster.

### Phase 3 — only if still needed

6. **Keep `createResilientRouter` wired only for genuine cross-route navigation** (leaving Pipeline Monitor for another top-level route). Use the retuned 6s timeout and idle-gating; drop the retry and the `router.refresh()` warm-up unless the simpler version proves insufficient.

After Phases 1 and 2, the watchdog is defending a surface that has mostly ceased to exist. Reassess before investing further in tuning it.

Steps 3 and 5 are correct regardless of what the true root cause turns out to be.

---

## Open items worth noting

- **"Root cause confirmed."** The `KEEP_ALIVE_TIMEOUT` finding is a substantially better hypothesis than anything preceding it — it explains the idle correlation, the prod-only behaviour, and the recovery on full reload, and it is grounded in documented Next.js/Node behaviour rather than inference. It is still not *confirmed* until server-side logs show the upstream reset, because the documented failure mode is a 502 rather than a silent hang (see Option 0 caveats).
- **"Native Back uses popstate + a real reload, bypassing the soft-navigation path."** In the App Router, back/forward navigations are served *from* the client router cache by design. The keep-alive theory offers a better account: native Back either needs no network at all, or opens a fresh connection. Worth restating correctly in any write-up, since the original phrasing describes the framework inaccurately.
- **Earlier evidence that still stands.** The captured RSC responses returned valid flight payloads (`$Sreact.fragment`, `I[339756,...]`), so the proxy is not corrupting RSC data. Whatever the failure is, it is a connection-level fault, not a content-level one — consistent with the keep-alive theory.

None of this changes the recommendation. Phase 1 is worth shipping on the strength of the hypothesis alone, and Phase 2 removes the dependency on the mechanism ever being fully pinned down.

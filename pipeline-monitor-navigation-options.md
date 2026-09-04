# Pipeline Monitor — Navigation Options & Recommendation

**Route:** `/app/pipeline-monitor` (statically rendered)
**Symptom:** After the tab idles, in-app navigation (tab clicks, breadcrumbs, in-panel back arrow) silently stops responding in production. Native browser Back always recovers. Not reproducible in dev.

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
| 1 — `pushState` | Low | Low | Yes | Yes |
| 2 — `nuqs` | Low–Med | Low | Yes | Yes |
| 3 — Real routes | High | Med | No | Yes |
| 4 — Client state | Med | Med | Yes | Yes |
| 5 — `<a href>` | Very low | Low | Yes | No |
| 6 — Watchdog | Low | Med | No | Partly |

---

## Recommendation

**Adopt Option 1 for all ~19 breadcrumb / back-button handlers and the tab switch.**

Rationale: every one of those navigations is query-only on a statically rendered route. `pushState` makes them synchronous and local, which means there is no fetch to hang, no transition to wedge, and no timeout to tune. It is a smaller diff than the current plan and removes the need for the watchdog, retry logic, idle-gating, and warm-up entirely.

**Supporting changes, in order:**

1. **Verify the prerequisite.** Grep the `pipeline-monitor` subtree for Server Components reading `searchParams` for data fetching. If any exist, pair Option 1 with client-side fetching (Option 4's approach) for those components.
2. **Apply Option 1** to `page.tsx`, `execution-details.tsx`, `file-watcher.tsx`, `failed-accounts-detail-dialog.tsx`. Remove the `window.location.search` read and the unchanged-guard in `replaceCurrentUrlQuery`.
3. **Add `error.tsx` and `loading.tsx`** under `app/pipeline-monitor/`. Currently absent — this is why production failures are silent and indistinguishable from a dead click.
4. **Keep `createResilientRouter` wired only for genuine cross-route navigation** (leaving Pipeline Monitor for another top-level route). Use the retuned 6s timeout and idle-gating; drop the retry and `router.refresh()` warm-up unless the simpler version proves insufficient.
5. **Check IIS timeouts directly.** `web.config` / `httpPlatformHandler requestTimeout` and ARR idle-timeout settings are configurable. Worth fixing at the actual layer rather than compensating for it client-side.

Steps 2 and 3 are correct regardless of what the true root cause turns out to be.

---

## Open items worth noting

Two claims in the current analysis remain unverified:

- **"Root cause confirmed via live diagnostics."** The diagnostics confirmed the *symptom* (both call sites fail together; the main thread stays alive), not the mechanism. The captured RSC responses returned valid flight payloads (`$Sreact.fragment`, `I[339756,...]`), which argues against proxy corruption of RSC data.
- **"Native Back uses popstate + a real reload, bypassing the soft-navigation path."** In the App Router, back/forward navigations are served *from* the client router cache by design. That native Back reliably recovers needs a better explanation than the current analysis provides.

Neither affects the recommendation — Option 1 removes the dependency on the mechanism being correctly identified — but both should be resolved before any further client-side mitigation is layered on.

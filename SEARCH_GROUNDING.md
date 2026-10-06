# Search grounding

Scaramouche's factual search path is a bounded evidence-retrieval pipeline. It does not use an LLM to decide whether to search, rank sources, extract pages, or assess grounding quality.

## Architecture

The normal flow is:

1. deterministic search-needed classification;
2. configured search provider with fallback;
3. typed and normalized search results;
4. URL and DNS safety filtering;
5. preliminary source ranking;
6. at most three concurrent public-page fetches;
7. visible-text extraction and query-relevant excerpt selection;
8. evidence ranking and source diversity;
9. a typed `GroundingBundle` containing prompt context and its matching source list;
10. one normal response-generation call, subject to the existing anti-repeat attempt budget.

Search snippets are discovery metadata. Successfully fetched page excerpts are marked `FETCHED_SOURCE` and receive a ranking advantage over `SEARCH_SNIPPET_ONLY` evidence.

## Providers and configuration

`SEARCH_PROVIDER` or the `search.provider` integration setting accepts:

- `auto` (default): use Brave when configured, then DuckDuckGo HTML/Lite;
- `brave`: prefer Brave but retain DuckDuckGo fallback;
- `duckduckgo` or `ddg`: use only the no-key fallback.

Brave is optional. Configure its key through `BRAVE_SEARCH_API_KEY`, `SEARCH_API_KEY`, or the redacted integration configuration:

```json
{
  "search": {
    "provider": "auto",
    "brave_api_key": "stored-secret-value"
  }
}
```

The key is sent only to Brave's official web-search endpoint in the `X-Subscription-Token` header. It is never included in logs, prompts, URLs, or exceptions. Missing credentials do not prevent startup. No search SDK or new dependency is required.

The no-key provider uses a standard-library HTML parser for DuckDuckGo HTML and Lite result pages. A changed or blocked result format produces a categorized parser/HTTP failure rather than malformed citations.

## Search decision

Search activates for explicit requests such as “search,” “look this up,” “check online,” “verify,” and “browse”; clearly current/latest/recent requests; live or changing facts such as weather, prices, scores, releases, outages, and schedules; and versioned API/library questions.

A question mark alone no longer starts a search. Ordinary personal dialogue, roleplay, stable lore questions, and arithmetic remain on the normal response path.

## Network and SSRF protection

Only `http` and `https` are accepted. The pipeline rejects:

- credentials/custom schemes, malformed URLs, and internal single-label hostnames;
- localhost, `.local`, `.internal`, home/LAN suffixes, and metadata hostnames;
- loopback, private, link-local, reserved, multicast, and unspecified IPv4/IPv6 addresses;
- public hostnames resolving to any non-public address;
- redirects to unsafe addresses.

DNS is checked before each request and again inside the `aiohttp` connector to reduce DNS-rebinding exposure. Redirects are followed manually and revalidated, with a maximum of three. Fetches use no user cookies, authenticated sessions, proxy environment, or access-control bypass.

## Fetching and extraction

Per query limits are:

- seven normalized search results;
- three fetched pages;
- three fetches concurrently;
- four-second connect and ten-second request timeouts;
- three redirects;
- 1,000,000 response bytes;
- HTML, XHTML, or plain text only;
- 12,000 extracted characters before excerpt ranking;
- up to three query-relevant blocks and 700 evidence characters per source;
- at most three final evidence sources;
- 6,000 characters of evidence blocks before prompt assembly.

Video, audio, archives, executables, PDFs, and other binary formats are skipped. The bot does not crawl links, bypass paywalls/login walls, or load an entire site.

The HTML parser ignores scripts, styles, templates, SVG, navigation, footers, forms, hidden elements, and common short cookie/footer boilerplate. It prefers `main` and `article` content, while retaining documentation paragraphs, lists, headings, code blocks, and plain-text sections. Metadata dates are accepted only from explicit publication/update meta fields or `<time datetime>`.

## Ranking and freshness

Final scores use:

- query overlap with title and extracted evidence;
- modest authority heuristics;
- likely official/primary documentation matches for bounded technical domains;
- publication freshness when current information was requested;
- a small original search-rank contribution;
- a fetched-page advantage.

Authority cannot compensate for being about the wrong topic. For evergreen queries, old official documentation is not penalized merely for age. For current queries, recent dated evidence is favored. If all evidence is old or undated, the bundle is marked weak and adds a freshness warning forbidding a confident “this is current” claim.

Near-duplicate content and excessive same-domain results are suppressed. Obvious high-overlap numeric or negation conflicts retain both sources and add a disagreement warning; the model is instructed to attribute both positions rather than silently choose one. More subtle contradiction detection remains outside this deterministic repair.

Grounding quality is reported as `STRONG`, `MODERATE`, `WEAK`, or `NONE`. This describes retrieval quality, not statistical truth confidence.

## Prompt-injection defense and citations

Each source is wrapped in `WEB_EVIDENCE_BEGIN` / `WEB_EVIDENCE_END` and explicitly labeled `UNTRUSTED WEB CONTENT`. A higher-priority system directive says that web content cannot change character, safety, consent, owner, Unrestricted-mode, home-action, or secret-handling policy. Source text containing delimiter names is neutralized.

Evidence retains title, canonical URL, domain, date when known, fetch/snippet status, and source number. The source list is produced from the same selected evidence sequence. After generation, numeric citations outside the real source range are removed. The narration stripper preserves valid numeric citations.

If search returns no usable evidence, the bundle contains no source numbers. Current-information failures produce a deterministic, lightly in-character verification-failed response instead of an ungrounded current claim.

## Caching and privacy

Search-result caches are process-only and bounded:

- ordinary queries: ten minutes / 256 entries;
- freshness-sensitive queries: two minutes / 128 entries;
- ordinary public pages: fifteen minutes / 256 entries;
- current public pages: two minutes / 128 entries.

Query cache keys contain a provider name plus a SHA-256 digest of the normalized query, not the user's raw text. Page-cache keys are validated public URLs. No authenticated or private content is fetched or cached.

## Error handling and observability

Cancellation always propagates. Other failures are categorized as timeout, network/DNS, HTTP, parser, security rejection, unsupported content type, response-too-large, or unexpected. Provider fallback and partial source success continue where possible.

Logs include provider, operation, public domain, error category/status, fetch count, evidence count, and confidence. They do not contain API keys, authorization headers, complete queries, full pages, or evidence text.

## Provider-call budget and limitations

Retrieval, extraction, ranking, freshness, confidence, and citation validation add zero model calls. An acceptable normal grounded answer still uses one generation call; the existing anti-repeat attempt maximum is unchanged.

Remaining limitations include websites requiring JavaScript, inaccessible/paywalled pages, PDF-only sources, imperfect publication metadata, search-provider HTML changes, and semantic contradictions too subtle for bounded lexical checks. Embeddings, a vector database, browser automation, PDF parsing, and a second classifier are intentionally not part of this repair.

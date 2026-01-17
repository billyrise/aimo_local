# Domain Parsing (eTLD+1) and Public Suffix List Policy

## Why we use the Public Suffix List
Many proxy/SWG logs provide only a hostname. For rollups such as "user × domain" we need a stable, privacy-safe notion of a registrable domain.

We compute **eTLD+1** (also called "registrable domain") using the **Public Suffix List (PSL)**, which enumerates public suffixes like `com`, `co.uk`, and `tokyo.jp`. The PSL is maintained as a community resource and is intended to be pulled from `https://publicsuffix.org/list/public_suffix_list.dat`.

## Implementation requirements
1. Use the PSL snapshot shipped in `data/psl/public_suffix_list.dat` for computations during a run.
2. Do not fetch PSL dynamically during a run.
3. Record the PSL file SHA-256 in `runs` metadata so results are reproducible.
4. If you use a library (e.g., `publicsuffix2`), configure it to read from the local PSL snapshot.

## Update policy (reproducibility first)
- PSL updates are allowed only as a deliberate change:
  1. Download the latest PSL to `data/psl/public_suffix_list.dat`.
  2. Record its SHA-256 and update `docs/domain_parsing.md` changelog note.
  3. Run regression tests on domain parsing and aggregation.
  4. If aggregation outputs change materially, document the impact.

## Changelog
- v1.4 pack: PSL snapshot is shipped as a pinned input to avoid drift across runs.

- 2026-01-17: PSL snapshot sha256=4bd5bd3d1fd15a8ac7cbfff17a64ba16f423a9f96b307721a46d355b40de3663 (source: https://publicsuffix.org/list/public_suffix_list.dat)

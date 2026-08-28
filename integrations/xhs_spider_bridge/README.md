# Spider_XHS read-only bridge

This optional loopback service pins Spider_XHS to commit
`e1888d712519040f5fcc294baeac4b9505b25c98` and exposes only paginated search,
note detail, and health endpoints in the schema RetailTide already uses.

RetailTide treats this bridge and `xiaohongshu-mcp` as two transports for one
logical source. The bridge is the paginated search/detail primary;
`xiaohongshu-mcp` remains the account-login authority and a bounded recent-page
or detail fallback. They must not be counted as independent data sources.

Every Spider_XHS operation runs in one replaceable worker process. Search has a
45-second hard deadline and detail has a 30-second hard deadline by default;
the parent terminates and recreates a stuck worker so one timeout cannot hold
the serial source lock indefinitely. Override these only in Compose with
`BRIDGE_SEARCH_TIMEOUT_SECONDS` and `BRIDGE_DETAIL_TIMEOUT_SECONDS`.

It never writes cookies into the RetailTide database or logs them. Mount a
project-owned cookie JSON file read-only and bind the service to loopback.

Spider_XHS uses private Xiaohongshu web interfaces and its README limits use to
learning/non-commercial scenarios. Operators must verify their authorization,
the upstream terms, and license compatibility before enabling this integration.

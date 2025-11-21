# URL Functions

Functions for parsing and manipulating URLs.

## URL Parsing

### `url_parse(url)`
Parses a URL into its components.

```bash
dsq '"https://example.com:8080/path?key=value#fragment" | url_parse'
# Output: {
#   protocol: "https",
#   domain: "example.com",
#   port: 8080,
#   path: "/path",
#   query: "key=value",
#   fragment: "fragment"
# }
```

### `url_extract_protocol(url)`
Extracts the protocol/scheme from a URL.

```bash
dsq '"https://example.com" | url_extract_protocol'
# Output: "https"

dsq '.website | url_extract_protocol' sites.csv
```

### `url_extract_domain(url)`
Extracts the domain from a URL.

```bash
dsq '"https://www.example.com/path" | url_extract_domain'
# Output: "www.example.com"

dsq '.url | url_extract_domain' links.csv
```

### `url_extract_path(url)`
Extracts the path from a URL.

```bash
dsq '"https://example.com/api/users" | url_extract_path'
# Output: "/api/users"

dsq '.request_url | url_extract_path' logs.csv
```

### `url_extract_query_string(url)`
Extracts the query string from a URL.

```bash
dsq '"https://example.com?foo=bar&baz=qux" | url_extract_query_string'
# Output: "foo=bar&baz=qux"

dsq '.url | url_extract_query_string' data.csv
```

### `url_extract_port(url)`
Extracts the port number from a URL.

```bash
dsq '"https://example.com:8080" | url_extract_port'
# Output: 8080

dsq '"https://example.com" | url_extract_port'
# Output: null (default port)
```

## URL Modification

### `url_set_protocol(url, protocol)`
Changes the protocol of a URL.

```bash
dsq '"http://example.com" | url_set_protocol("https")'
# Output: "https://example.com"

dsq 'map({url: .url | url_set_protocol("https")})' links.csv
# Convert all to HTTPS
```

### `url_set_domain(url, domain)`
Changes the domain of a URL.

```bash
dsq '"https://old.com/path" | url_set_domain("new.com")'
# Output: "https://new.com/path"

dsq '.url | url_set_domain("api.example.com")' endpoints.csv
```

### `url_set_domain_without_www(url)`
Removes "www." from the domain.

```bash
dsq '"https://www.example.com" | url_set_domain_without_www'
# Output: "https://example.com"

dsq 'map({clean_url: .url | url_set_domain_without_www})' sites.csv
```

### `url_set_path(url, path)`
Changes the path of a URL.

```bash
dsq '"https://example.com/old" | url_set_path("/new")'
# Output: "https://example.com/new"

dsq '.url | url_set_path("/api/v2")' data.csv
```

### `url_set_query_string(url, key, value)`
Sets or updates a query parameter.

```bash
dsq '"https://example.com" | url_set_query_string("api_key", "123")'
# Output: "https://example.com?api_key=123"

dsq '.url | url_set_query_string("version", "2")' requests.csv
```

### `url_set_port(url, port)`
Changes the port of a URL.

```bash
dsq '"https://example.com" | url_set_port(8080)'
# Output: "https://example.com:8080"

dsq '.url | url_set_port(443)' data.csv
```

## URL Cleanup

### `url_strip_protocol(url)`
Removes the protocol from a URL.

```bash
dsq '"https://example.com" | url_strip_protocol'
# Output: "example.com"

dsq '.url | url_strip_protocol' data.csv
```

### `url_strip_fragment(url)`
Removes the fragment/anchor from a URL.

```bash
dsq '"https://example.com/page#section" | url_strip_fragment'
# Output: "https://example.com/page"

dsq '.url | url_strip_fragment' links.csv
```

### `url_strip_query_string(url)`
Removes all query parameters from a URL.

```bash
dsq '"https://example.com?foo=bar&baz=qux" | url_strip_query_string'
# Output: "https://example.com"

dsq '.url | url_strip_query_string' clean_urls.csv
```

### `url_strip_port(url)`
Removes the port from a URL.

```bash
dsq '"https://example.com:8080" | url_strip_port'
# Output: "https://example.com"
```

### `url_strip_port_if_default(url)`
Removes port only if it's the default for the protocol.

```bash
dsq '"https://example.com:443" | url_strip_port_if_default'
# Output: "https://example.com"

dsq '"https://example.com:8080" | url_strip_port_if_default'
# Output: "https://example.com:8080" (non-default port kept)
```

## Examples

### URL Analysis
```bash
# Extract domains from URLs
dsq 'map(.url | url_extract_domain) | unique' links.csv

# Group by protocol
dsq 'group_by(.url | url_extract_protocol)' websites.csv

# Find non-HTTPS URLs
dsq 'map(select(.url | url_extract_protocol != "https"))' links.csv
```

### URL Normalization
```bash
# Normalize to HTTPS
dsq 'map({url: .url | url_set_protocol("https")})' sites.csv

# Remove www prefix
dsq 'map({url: .url | url_set_domain_without_www})' links.csv

# Clean URLs (remove query and fragment)
dsq 'map({clean: .url | url_strip_query_string | url_strip_fragment})' data.csv

# Standardize ports
dsq 'map({url: .url | url_strip_port_if_default})' endpoints.csv
```

### URL Transformation
```bash
# Change API version in paths
dsq 'map({
  old: .url,
  new: .url | url_set_path((.url | url_extract_path | replace("/v1/", "/v2/")))
})' api_calls.csv

# Add tracking parameters
dsq 'map({url: .url | url_set_query_string("utm_source", "newsletter")})' links.csv

# Convert to API endpoints
dsq 'map({
  api_url: .url | url_set_domain("api.example.com") | url_set_protocol("https")
})' data.csv
```

### URL Validation
```bash
# Find URLs with non-standard ports
dsq 'map(select(.url | url_extract_port != null))' urls.csv

# Find URLs without HTTPS
dsq 'map(select((.url | url_extract_protocol) != "https")) | {
  url: .url,
  protocol: .url | url_extract_protocol
}' links.csv

# Check for query parameters
dsq 'map(select((.url | url_extract_query_string) != null))' urls.csv
```

### Domain Analysis
```bash
# Count by domain
dsq 'group_by(.url | url_extract_domain) | map({
  domain: .[0].url | url_extract_domain,
  count: length
}) | sort_by(.count) | reverse' access_logs.csv

# Find subdomains
dsq 'map(.url | url_extract_domain | split(".") | {
  subdomain: .[0],
  domain: (.[1:] | join("."))
})' urls.csv

# Group by TLD
dsq 'map(.url | url_extract_domain | split(".") | last) | unique' domains.csv
```

### Path Analysis
```bash
# Extract API endpoints
dsq 'map(.url | url_extract_path) | unique' api_logs.csv

# Find specific paths
dsq 'map(select((.url | url_extract_path | startswith("/api/"))))' requests.csv

# Group by path prefix
dsq 'group_by(.url | url_extract_path | split("/") | .[1])' logs.csv
```

### URL Building
```bash
# Build complete URLs from parts
dsq 'map({
  url: ("https://" + .domain + .path)
})' url_parts.csv

# Add base URL
dsq 'map({
  full_url: ("https://api.example.com" + .endpoint)
})' endpoints.csv
```

## Common Patterns

### Clean and standardize URLs
```bash
dsq 'map({
  url: .url
    | url_set_protocol("https")
    | url_set_domain_without_www
    | url_strip_fragment
    | url_strip_port_if_default
})' urls.csv
```

### Extract all URL components
```bash
dsq 'map({
  original: .url,
  protocol: (.url | url_extract_protocol),
  domain: (.url | url_extract_domain),
  path: (.url | url_extract_path),
  query: (.url | url_extract_query_string),
  port: (.url | url_extract_port)
})' urls.csv
```

### Convert relative to absolute URLs
```bash
dsq 'map({
  url: if (.path | startswith("/")) then
    "https://example.com" + .path
  else
    .path
  end
})' paths.csv
```

## Type Support

URL functions work with:
- String values containing URLs
- DataFrame columns with URL data
- Any valid URL format

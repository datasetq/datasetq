# Date/Time Functions

Functions for working with dates, times, and timestamps.

## Date Component Extraction

### `year(date)`, `month(date)`, `day(date)`
Extracts date components.

```bash
dsq '.created_at | year' events.csv
# Extract year

dsq '.timestamp | month' logs.csv
# Extract month (1-12)

dsq '.date | day' records.csv
# Extract day of month (1-31)
```

### `hour(date)`, `minute(date)`, `second(date)`
Extracts time components.

```bash
dsq '.timestamp | hour' events.csv
# Extract hour (0-23)

dsq '.time | minute' data.csv
# Extract minute (0-59)

dsq '.time | second' data.csv
# Extract second (0-59)
```

## Current Date/Time

### `today()`
Returns the current date.

```bash
dsq 'today()'
# Output: current date

dsq 'map({date: .date, is_today: (.date == today())})' events.csv
```

### `now()`
Returns the current timestamp.

```bash
dsq 'now()'
# Output: current timestamp

dsq 'map({timestamp: now()})' data.csv
# Add current timestamp
```

### `systime()`, `systime_ns()`, `systime_int()`
Returns system time in various formats.

```bash
dsq 'systime()'
# Unix timestamp (seconds)

dsq 'systime_ns()'
# Nanoseconds

dsq 'systime_int()'
# Integer timestamp
```

## Date Construction

### `mktime(year, month, day, hour?, min?, sec?)`
Creates a timestamp from components.

```bash
dsq 'mktime(2024, 1, 15)'
# Date only: 2024-01-15

dsq 'mktime(2024, 1, 15, 14, 30, 0)'
# With time: 2024-01-15 14:30:00

dsq 'map({start: mktime(.year, .month, .day)})' data.csv
```

## Time Formatting

### `strftime(timestamp, format)`
Formats a timestamp as a string.

```bash
dsq '.timestamp | strftime("%Y-%m-%d")' events.csv
# Output: "2024-01-15"

dsq '.timestamp | strftime("%Y-%m-%d %H:%M:%S")' logs.csv
# Output: "2024-01-15 14:30:00"

dsq '.date | strftime("%B %d, %Y")' data.csv
# Output: "January 15, 2024"
```

Common format codes:
- `%Y` - Year (4 digits)
- `%m` - Month (01-12)
- `%d` - Day (01-31)
- `%H` - Hour (00-23)
- `%M` - Minute (00-59)
- `%S` - Second (00-59)
- `%B` - Full month name
- `%A` - Full weekday name

### `strflocaltime(timestamp, format)`
Formats timestamp in local timezone.

```bash
dsq '.timestamp | strflocaltime("%Y-%m-%d %H:%M:%S %Z")' events.csv
```

### `strptime(string, format)`
Parses a timestamp string.

```bash
dsq '"2024-01-15" | strptime("%Y-%m-%d")'
# Parse date string

dsq '.date_string | strptime("%m/%d/%Y")' data.csv
# Parse MM/DD/YYYY format
```

## Time Conversion

### `localtime(timestamp)`
Converts timestamp to local time.

```bash
dsq '.utc_timestamp | localtime' events.csv
```

### `gmtime(timestamp)`
Converts timestamp to GMT/UTC.

```bash
dsq '.local_timestamp | gmtime' data.csv
```

## Date Arithmetic

### `date_diff(date1, date2, unit?)`
Calculates difference between dates.

```bash
dsq 'date_diff(.end_date, .start_date)' events.csv
# Difference in days (default)

dsq 'date_diff(.end, .start, "hours")' sessions.csv
# Difference in hours

dsq 'date_diff(.checkout, .created, "minutes")' orders.csv
# Difference in minutes
```

Units: `"seconds"`, `"minutes"`, `"hours"`, `"days"`, `"weeks"`

### `truncate_date(date, unit)`
Truncates date to specified unit.

```bash
dsq '.timestamp | truncate_date("day")' events.csv
# Truncate to start of day

dsq '.timestamp | truncate_date("month")' data.csv
# Truncate to start of month

dsq '.timestamp | truncate_date("year")' logs.csv
# Truncate to start of year
```

### `truncate_time(time, unit)`
Truncates time to specified unit.

```bash
dsq '.timestamp | truncate_time("hour")' events.csv
# Truncate to start of hour

dsq '.timestamp | truncate_time("minute")' logs.csv
# Truncate to start of minute
```

## Date Ranges

### `start_of_month(date)`, `end_of_month(date)`
Gets month boundaries.

```bash
dsq '.date | start_of_month' data.csv
# First day of month

dsq '.date | end_of_month' data.csv
# Last day of month
```

### `start_of_week(date)`, `end_of_week(date)`
Gets week boundaries.

```bash
dsq '.date | start_of_week' events.csv
# Start of week (Monday)

dsq '.date | end_of_week' events.csv
# End of week (Sunday)
```

## Examples

### Date Formatting
```bash
# Format dates for display
dsq 'map({date: .timestamp | strftime("%B %d, %Y")})' events.csv

# Create ISO 8601 timestamps
dsq 'map({iso: .timestamp | strftime("%Y-%m-%dT%H:%M:%SZ")})' data.csv

# Year-month format for grouping
dsq 'map({month: .date | strftime("%Y-%m")})' logs.csv
```

### Date Filtering
```bash
# Events from 2024
dsq 'map(select(.date | year == 2024))' events.csv

# Events in January
dsq 'map(select(.timestamp | month == 1))' data.csv

# Recent events (last 7 days)
dsq 'map(select(date_diff(today(), .date, "days") <= 7))' events.csv

# Business hours only
dsq 'map(select(.timestamp | hour >= 9 and hour < 17))' logs.csv
```

### Date Grouping
```bash
# Group by year
dsq 'group_by(.date | year) | map({
  year: .[0].date | year,
  count: length
})' events.csv

# Group by month
dsq 'group_by(.date | strftime("%Y-%m")) | map({
  month: .[0].date | strftime("%Y-%m"),
  total: (map(.amount) | sum)
})' transactions.csv

# Group by day of week
dsq 'group_by(.date | strftime("%A")) | map({
  day: .[0].date | strftime("%A"),
  avg: (map(.value) | mean)
})' daily_data.csv
```

### Duration Calculations
```bash
# Session duration in minutes
dsq 'map({
  user: .user_id,
  duration: date_diff(.logout, .login, "minutes")
})' sessions.csv

# Age in years
dsq 'map({
  name: .name,
  age: date_diff(today(), .birthdate, "days") / 365 | floor
})' people.csv

# Processing time
dsq 'map({
  id: .id,
  processing_seconds: date_diff(.completed_at, .started_at, "seconds")
})' jobs.csv
```

### Time Series Analysis
```bash
# Daily aggregates
dsq 'group_by(.timestamp | truncate_date("day")) | map({
  date: .[0].timestamp | truncate_date("day") | strftime("%Y-%m-%d"),
  count: length,
  total: (map(.amount) | sum)
})' transactions.csv

# Hourly patterns
dsq 'group_by(.timestamp | hour) | map({
  hour: .[0].timestamp | hour,
  avg_requests: length
})' requests.csv

# Monthly trends
dsq 'group_by(.date | truncate_date("month")) | map({
  month: .[0].date | strftime("%Y-%m"),
  revenue: (map(.revenue) | sum),
  orders: length
})' orders.csv
```

### Date Range Queries
```bash
# Month-to-date
dsq 'map(select(.date >= start_of_month(today())))' data.csv

# Current week
dsq 'map(select(
  .date >= start_of_week(today()) and
  .date <= end_of_week(today())
))' events.csv

# Last 30 days
dsq 'map(select(date_diff(today(), .date, "days") <= 30))' logs.csv
```

### Date Transformation
```bash
# Add timestamps
dsq 'map(. + {processed_at: now()})' data.csv

# Parse string dates
dsq 'map({
  original: .date_str,
  parsed: .date_str | strptime("%m/%d/%Y")
})' data.csv

# Normalize to UTC
dsq 'map({timestamp: .local_time | gmtime})' events.csv
```

## Performance Tips

- Truncate dates before grouping for better performance
- Use date components for filtering instead of string comparisons
- Parse date strings once and reuse
- Consider using `--lazy` for large time series datasets

## Type Support

Date/time functions work with:
- ISO 8601 timestamp strings
- Unix timestamps (seconds since epoch)
- Date objects in DataFrames
- Parsed date strings via `strptime`

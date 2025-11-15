# Data Download Pipeline Review

## Current Implementation

### Scripts
1. **estimate_download_cost.py** - Cost estimation, only handles `node_fills_by_block/`
2. **check_data_availability.py** - Checks all 3 paths, provides overview
3. **download_data.py** - Downloads from all 3 paths with progress tracking

### Current Strengths
- ✅ Handles requester-pays bucket correctly
- ✅ Skips already downloaded files (resume capability)
- ✅ Progress tracking and cost estimation
- ✅ Good error handling
- ✅ Maintains S3 directory structure locally

### Issues & Limitations

#### 1. **Inconsistent Path Handling**
- `estimate_download_cost.py` only checks `node_fills_by_block/`
- `check_data_availability.py` and `download_data.py` check all 3 paths
- Users might estimate cost for only 1 path but download all 3 → surprise costs

#### 2. **No Date Range Filtering**
- Downloads ALL available data (~104 GB)
- No option to download specific date ranges
- `estimate_download_cost.py` has date filtering UI but `download_data.py` doesn't use it
- Users might only need recent data (e.g., last 3 months)

#### 3. **No Selective Path Downloading**
- Always downloads from all 3 paths
- User might only want current format (`node_fills_by_block/`)
- Wastes bandwidth/cost on overlapping legacy data

#### 4. **Inefficient Progress Reporting**
- Prints every file download (can be 100k+ files)
- "Skipped" message every 100 files still clutters output
- No overall progress bar or time estimate

#### 5. **No Parallel Downloads**
- Sequential downloads from S3
- Could be 5-10x faster with concurrent downloads
- S3 handles parallel requests well

#### 6. **No Integrity Verification**
- Downloads check size match only
- No checksum/hash verification
- Corrupted partial downloads could go unnoticed

#### 7. **Memory Usage for Large Listings**
- Loads all object metadata into memory
- For 100k+ files, this could be 50-100 MB
- Could stream instead

#### 8. **Redundant Code**
- `list_s3_objects()` function duplicated across 3 scripts
- `format_size()` function duplicated across 3 scripts
- Should have shared utilities module

#### 9. **AWS Credentials**
- No guidance on AWS credential configuration
- Users might hit auth errors without clear messaging

#### 10. **No Dry-Run Mode**
- Can't test download process without incurring costs
- Would be useful for validation

## Improvement Proposals

### High Priority

#### 1. **Unify Path Handling** ⭐⭐⭐
Make all scripts consistent about which paths they handle:
```python
# Add to download_data.py
PATHS = {
    'current': ('node_fills_by_block/', 'Current format'),
    'legacy_fills': ('node_fills/', 'Legacy API format'),
    'legacy_trades': ('node_trades/', 'Legacy alternative format'),
}

# Let user choose
--paths current  # Only current format
--paths current,legacy_fills  # Current + one legacy
--paths all  # Everything (default)
```

#### 2. **Add Date Range Filtering** ⭐⭐⭐
```bash
# Download only recent data
uv run scripts/download_data.py --start-date 2025-10-01 --end-date 2025-11-07

# Download last N days
uv run scripts/download_data.py --last-days 90

# Download everything (default)
uv run scripts/download_data.py
```

#### 3. **Parallel Downloads** ⭐⭐⭐
```python
import concurrent.futures

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(download_file, ...) for obj in objects]
    for future in concurrent.futures.as_completed(futures):
        # Update progress
```
Expected speedup: 5-10x for small files

#### 4. **Better Progress Display** ⭐⭐
```python
from tqdm import tqdm

with tqdm(total=total_bytes, unit='B', unit_scale=True) as pbar:
    # Update as files complete
    pbar.update(file_size)
```
Shows: progress bar, ETA, download speed

### Medium Priority

#### 5. **Shared Utilities Module** ⭐⭐
Create `src/s3_utils.py`:
```python
def list_s3_objects(bucket, prefix, **kwargs):
    """Shared S3 listing logic"""

def format_size(bytes_size):
    """Shared size formatting"""

def calculate_cost(total_gb, num_files):
    """Shared cost calculation"""
```

#### 6. **Integrity Verification** ⭐⭐
```python
# Use S3 ETag for verification
def download_file_with_verification(s3_client, bucket, key, output_path):
    # Get ETag from S3
    # Download file
    # Verify local file ETag matches
```

#### 7. **Dry-Run Mode** ⭐
```bash
uv run scripts/download_data.py --dry-run
# Shows what would be downloaded without actually downloading
```

#### 8. **Better Error Messages** ⭐
```python
# Check AWS credentials upfront
try:
    s3_client.list_buckets()
except NoCredentialsError:
    print("Error: AWS credentials not found!")
    print("Configure with: aws configure")
    sys.exit(1)
```

### Low Priority

#### 9. **Resume from Interruption**
Currently resumes by checking file existence, but could be smarter:
```python
# Track download state in JSON file
# Resume from exact position if interrupted
```

#### 10. **Compression-Aware Downloads**
Files are already LZ4 compressed, but could:
- Download directly to compressed format
- Stream decompression during download

## Recommended Implementation Order

### Phase 1: Quick Wins (1-2 hours)
1. Create shared `src/s3_utils.py` module
2. Add `--dry-run` flag
3. Improve error messages for AWS credentials
4. Update `estimate_download_cost.py` to handle all paths

### Phase 2: Major Improvements (3-4 hours)
1. Add date range filtering (`--start-date`, `--end-date`, `--last-days`)
2. Add path selection (`--paths`)
3. Better progress display with tqdm
4. Make all 3 scripts consistent

### Phase 3: Performance (2-3 hours)
1. Implement parallel downloads
2. Add integrity verification
3. Optimize memory usage for large listings

## Questions to Consider

1. **Do we need all 3 data paths?**
   - `node_fills_by_block/` is current format
   - Other two are legacy with overlapping dates
   - Could default to only current format?

2. **What's the common use case?**
   - Full historical download (rare)
   - Recent data only (common for updates)
   - Specific date range (research)

3. **Performance vs. Simplicity?**
   - Parallel downloads add complexity
   - But 5-10x speedup is significant for 104 GB

4. **Cost optimization priority?**
   - Date filtering could save 50-80% of costs
   - Path selection could save 40% by skipping legacy

## Estimated Impact

| Improvement | Dev Time | User Time Saved | Cost Saved | Complexity |
|-------------|----------|-----------------|------------|------------|
| Date filtering | 2h | N/A | 50-80% | Low |
| Path selection | 1h | N/A | 0-40% | Low |
| Parallel downloads | 3h | 80% (25min → 5min) | 0% | Medium |
| Better progress | 1h | Better UX | 0% | Low |
| Shared utils | 2h | Better maintainability | 0% | Low |

## Conclusion

**Top 3 improvements:**
1. **Date range filtering** - biggest cost savings
2. **Path selection** - flexibility + potential cost savings  
3. **Parallel downloads** - massive time savings

These three changes would transform the download experience from "download everything, wait 30 minutes" to "download what you need in 2-5 minutes."

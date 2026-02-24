# Hawaii Fisheries Data Cleaning Backend

Backend validation and optional filtering pipeline for Hawaii commercial and non-commercial fisheries exchange value data.

## Overview

This repository contains the backend data processing pipeline that:
- **Validates** pre-tidied commercial fisheries data (1997-2021)
- **Validates** pre-tidied non-commercial fisheries data (2005-2022)
- **Optionally removes** aggregate rows ('All Species', 'All Ecosystems')
- **Optionally removes** display-only columns (Hawaiian language names, formatted values)
- **Exports** analysis-ready CSV files
- **Generates** summary statistics in JSON format

**Note:** Input data is already "tidied" from `process_fisheries_ev_data.qmd`, so this pipeline focuses on validation and optional transformations rather than heavy cleaning.

## Data Sources

### Commercial Fisheries Data
- **Time period:** 1997-2021
- **Reporting units:** DAR catch areas (82 areas) and county
- **Source:** HDAR Commercial Marine Landings
- **Species groups:** Deep 7 Bottomfish, Shallow Bottomfish, Pelagics, Reef-Associated, All Species
- **Ecosystems:** Inshore — Reef, Coastal — Open Ocean, All Ecosystems
- **EV definition:** Gross Revenue - Marginal Costs per UN SEEA EA framework

### Non-Commercial Fisheries Data
- **Time period:** 2005-2022
- **Reporting units:** Islands (Hawaii, Kauai, Lanai, Maui, Molokai, Oahu)
- **Source:** MRIP Hawaii Marine Recreational Fishing Survey
- **Species groups:** Herbivores only
- **Ecosystems:** Inshore — Reef (all herbivores map to 100% reef habitat)
- **EV definition:** Gross Revenue - Marginal Costs per UN SEEA EA framework
- **Note:** Niihau and Kahoolawe not included (MRIP survey does not cover)

## Project Structure

```
fisheries-data-cleaning/
├── clean_commercial.py          # Commercial data validation module
├── clean_noncommercial.py       # Non-commercial data validation module
├── pipeline.py                  # Main pipeline orchestrator
├── config.py                    # Configuration settings
├── requirements.txt             # Python dependencies
├── README.md                    # This file
├── .gitignore                   # Git ignore rules
├── data/
│   ├── raw/                     # Input: Pre-tidied CSV files
│   └── cleaned/                 # Output: Analysis-ready CSV files
└── logs/                        # Processing logs
```

## Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd fisheries-data-cleaning
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create required directories:
```bash
mkdir -p data/raw data/cleaned logs
```

4. Place your tidied data files in `data/raw/`:
```bash
data/raw/
├── 20260216_tidied_comm_ev.csv
└── 20260216_tidied_noncomm_ev.csv
```

## Usage

### Run Complete Pipeline

To process both commercial and non-commercial data:

```bash
python3 pipeline.py
```

**Default behavior:**
- ✅ Removes aggregate rows ('All Species', 'All Ecosystems')
- ✅ Keeps display columns (Hawaiian names, formatted values)

### Configure Pipeline Options

Edit `pipeline.py` to customize:

```python
success = pipeline.run_full_pipeline(
    remove_aggregates=True,   # Remove 'All Species' and 'All Ecosystems' rows
    remove_display=False      # Keep display-only columns
)
```

**Options:**
- `remove_aggregates=True` - Removes aggregate summary rows to prevent double-counting in analyses
- `remove_display=False` - Keeps columns like `county_olelo`, `island_olelo`, `exchange_value_formatted`

### Run Individual Cleaners

**Commercial data only:**
```python
from clean_commercial import CommercialDataCleaner

cleaner = CommercialDataCleaner(
    input_dir='data/raw',
    output_dir='data/cleaned'
)

success, output_file, summary = cleaner.run_cleaning_pipeline(
    remove_aggregates=True,
    remove_display=False
)
```

**Non-commercial data only:**
```python
from clean_noncommercial import NonCommercialDataCleaner

cleaner = NonCommercialDataCleaner(
    input_dir='data/raw',
    output_dir='data/cleaned'
)

success, output_file, summary = cleaner.run_cleaning_pipeline(
    remove_aggregates=True,
    remove_display=False
)
```

## Data Schema

### Commercial Data Columns

| Column | Type | Description |
|--------|------|-------------|
| `year` | integer | Calendar year (1997-2021) |
| `area_id` | integer | DAR catch area identifier (1-82) |
| `county` | character | County name (Hawaii, Maui, Honolulu, Kauai, Kalawao) |
| `county_olelo` | character | County name in ʻŌlelo Hawaiʻi (display only) |
| `species_group` | character | Species group or 'All Species' |
| `ecosystem_type` | character | 'Inshore — Reef', 'Coastal — Open Ocean', or 'All Ecosystems' |
| `exchange_value` | numeric | Exchange value in USD |
| `exchange_value_formatted` | character | Currency-formatted value (display only) |

### Non-Commercial Data Columns

| Column | Type | Description |
|--------|------|-------------|
| `year` | integer | Calendar year (2005-2022) |
| `island` | character | Island name (Hawaii, Kauai, Lanai, Maui, Molokai, Oahu) |
| `island_olelo` | character | Island name in ʻŌlelo Hawaiʻi (display only) |
| `county` | character | County name |
| `county_olelo` | character | County name in ʻŌlelo Hawaiʻi (display only) |
| `species_group` | character | 'Herbivores' only |
| `ecosystem_type` | character | 'Inshore — Reef' or 'All Ecosystems' |
| `exchange_value` | numeric | Exchange value in USD |
| `exchange_value_formatted` | character | Currency-formatted value (display only) |

## Processing Steps

### Commercial Data Pipeline

1. **Load Data** - Read tidied CSV from input directory
2. **Validate Schema** - Check required columns exist
3. **Validate Data Types** - Ensure year/area_id are integers, exchange_value is numeric
4. **Validate Ranges** - Check for invalid years (outside 1997-2021) or negative values
5. **Validate Ecosystem Types** - Ensure values match expected categories
6. **Validate Species Groups** - Check against known species groups
7. **Validate Counties** - Verify county names are valid
8. **Remove Aggregates** - (Optional) Exclude 'All Species' and 'All Ecosystems' rows
9. **Remove Display Columns** - (Optional) Drop *_olelo and *_formatted columns
10. **Export Clean Data** - Save to CSV with timestamp

### Non-Commercial Data Pipeline

1. **Load Data** - Read tidied CSV from input directory
2. **Validate Schema** - Check required columns exist
3. **Validate Data Types** - Ensure year is integer, exchange_value is numeric
4. **Validate Ranges** - Check for invalid years (outside 2005-2022) or negative values
5. **Validate Ecosystem Types** - Ensure values match expected categories
6. **Validate Species Groups** - Ensure only 'Herbivores' present
7. **Validate Islands** - Check island names against surveyed islands
8. **Validate Counties** - Verify county names are valid
9. **Remove Aggregates** - (Optional) Exclude 'All Ecosystems' rows
10. **Remove Display Columns** - (Optional) Drop *_olelo and *_formatted columns
11. **Export Clean Data** - Save to CSV with timestamp

## Output Files

### Cleaned Data CSVs

**Commercial:**
```
data/cleaned/cleaned_commercial_YYYYMMDD.csv
```

**Non-Commercial:**
```
data/cleaned/cleaned_noncommercial_YYYYMMDD.csv
```

### Summary Statistics JSON

```
data/cleaned/cleaning_summary_YYYYMMDD.json
```

Example structure:
```json
{
  "pipeline_timestamp": "2026-02-17T23:48:44",
  "commercial": {
    "data_type": "commercial",
    "raw_row_count": 31125,
    "cleaned_row_count": 24800,
    "rows_removed": 6325,
    "date_range": {
      "min_year": 1997,
      "max_year": 2021
    },
    "total_exchange_value": 2450000000.50,
    "unique_counties": ["Hawaii", "Honolulu", "Kauai", "Maui"],
    "unique_species_groups": ["Deep 7 Bottomfish", "Pelagics", "Reef-Associated", "Shallow Bottomfish"],
    "unique_area_ids": [100, 101, 102, "..."]
  },
  "non_commercial": {
    "data_type": "non_commercial",
    "raw_row_count": 324,
    "cleaned_row_count": 216,
    "rows_removed": 108,
    "date_range": {
      "min_year": 2005,
      "max_year": 2022
    },
    "total_exchange_value": 125000000.75,
    "unique_islands": ["Hawaii", "Kauai", "Lanai", "Maui", "Molokai", "Oahu"],
    "unique_species_groups": ["Herbivores"]
  }
}
```

## Configuration

Edit `config.py` to customize:
- Input/output directories
- File naming patterns
- Valid county and island names
- Year range validation limits
- Expected species groups and ecosystem types
- Logging settings

## Logging

Logs are saved to `logs/cleaning_pipeline_YYYYMMDD_HHMMSS.log`

Log levels:
- **INFO:** Normal processing steps
- **WARNING:** Data quality issues (non-fatal)
- **ERROR:** Critical failures

## Data Quality Checks

The pipeline validates:
- ✅ Required columns present
- ✅ Exchange values are numeric and non-negative
- ✅ Years within valid ranges
- ✅ County names match known Hawaii counties
- ✅ Island names match surveyed Hawaiian islands
- ✅ Ecosystem types match expected values
- ✅ Species groups match expected values
- ✅ Non-commercial data contains only Herbivore species

## Why Remove Aggregates?

Aggregate rows ('All Species', 'All Ecosystems') are useful for reporting but can cause double-counting in analyses:

**Example - Area 100, 2021:**
- Deep 7 Bottomfish + Inshore — Reef: $10,289
- Deep 7 Bottomfish + Coastal — Open Ocean: $10,289
- **Deep 7 Bottomfish + All Ecosystems: $10,289** ← This is a sum of above two

If you sum all rows without filtering, you'd count the same value multiple times. Setting `remove_aggregates=True` removes these rollup rows.

## Integration with Frontend

The cleaned CSV files and summary JSON are designed to be consumed by:
- Interactive web dashboards
- Data visualization tools
- Statistical analysis software
- API endpoints

**Example frontend integration:**
```javascript
// Load summary stats for quick metadata
fetch('data/cleaned/cleaning_summary_20260217.json')
  .then(response => response.json())
  .then(summary => {
    displayStats(summary);
  });

// Load full cleaned data for charts
fetch('data/cleaned/cleaned_commercial_20260217.csv')
  .then(response => response.text())
  .then(csv => {
    const data = parseCSV(csv);
    createInteractiveCharts(data);
  });
```

## Source Data Credits

**Prepared by:** Alemarie Ceria  
**Processing date:** 02/16/2026  
**Data source:** Hawaii Department of Land and Natural Resources (DLNR)
- Commercial: HDAR Commercial Marine Landings
- Non-commercial: MRIP Hawaii Marine Recreational Fishing Survey

**Methodology:** SEEA EA (System of Environmental-Economic Accounting Experimental Ecosystem Accounting) exchange value framework

**Ecosystem attribution:** Fish Species over Ecosystem Type (FSOET) dataset

## License

[Add your license information here]

## Contact

[Add contact information here]

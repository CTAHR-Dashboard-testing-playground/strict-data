"""
Commercial Fisheries Data Cleaning Module

Processes commercial fisheries exchange value data from Hawaii.
Data covers 1997-2021, organized by DAR catch areas (82 areas) and county.
Source: HDAR Commercial Marine Landings with SEEA EA exchange value methodology
Note: Input data is pre-tidied from process_fisheries_ev_data.qmd
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime


class CommercialDataCleaner:
    """
    Validates and optionally filters commercial fisheries tidied data.
    Since input is pre-tidied, focuses on validation and optional transformations.
    """

    def __init__(self, input_dir='data/raw', output_dir='data/cleaned'):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.data = None
        self.raw_row_count = 0
        self.cleaned_row_count = 0

    def load_data(self):
        """
        Load commercial fisheries CSV from input directory.
        Looks for files matching pattern '*tidied_comm_ev*.csv'.
        
        Returns:
            bool: True if data loaded successfully, False otherwise
        """
        logging.info('Loading commercial fisheries data...')
        
        try:
            comm_files = list(self.input_dir.glob('*tidied_comm_ev*.csv'))
            
            if not comm_files:
                comm_files = [
                    f for f in self.input_dir.glob('*comm_ev*.csv')
                    if 'noncomm' not in f.name
                ]
            
            if not comm_files:
                logging.error(f'No commercial data file found in {self.input_dir}')
                return False
            
            self.data = pd.read_csv(comm_files[0])
            self.raw_row_count = len(self.data)
            
            logging.info(f'Loaded {self.raw_row_count:,} rows from {comm_files[0].name}')
            return True
            
        except Exception as e:
            logging.error(f'Error loading commercial data: {e}')
            return False

    def validate_schema(self):
        """
        Validate that required columns exist in the dataset.
        
        Returns:
            bool: True if all required columns present, False otherwise
        """
        logging.info('Validating data schema...')
        
        required_columns = [
            'year',
            'area_id',
            'county',
            'species_group',
            'ecosystem_type',
            'exchange_value'
        ]
        
        optional_columns = [
            'county_olelo',
            'exchange_value_formatted'
        ]
        
        missing_required = [col for col in required_columns if col not in self.data.columns]
        
        if missing_required:
            logging.error(f'Missing required columns: {missing_required}')
            return False
        
        present_optional = [col for col in optional_columns if col in self.data.columns]
        if present_optional:
            logging.info(f'Optional columns present: {present_optional}')
        
        logging.info('Schema validation passed')
        return True

    def validate_data_types(self):
        """
        Validate and convert data types if needed.
        Ensures year is integer and exchange_value is numeric.
        """
        logging.info('Validating data types...')
        
        self.data['year'] = pd.to_numeric(self.data['year'], errors='coerce').astype('Int64')
        
        self.data['area_id'] = pd.to_numeric(self.data['area_id'], errors='coerce').astype('Int64')
        
        self.data['exchange_value'] = pd.to_numeric(
            self.data['exchange_value'],
            errors='coerce'
        )
        
        null_years = self.data['year'].isnull().sum()
        null_values = self.data['exchange_value'].isnull().sum()
        
        if null_years > 0:
            logging.warning(f'Found {null_years} null years after conversion')
        if null_values > 0:
            logging.warning(f'Found {null_values} null exchange values after conversion')

    def validate_data_ranges(self):
        """
        Validate that data values are within expected ranges.
        Checks for negative values, invalid years, and extreme outliers.
        
        Returns:
            bool: True if validation passes with warnings only
        """
        logging.info('Validating data ranges...')
        
        issues = []
        
        if (self.data['exchange_value'] < 0).any():
            negative_count = (self.data['exchange_value'] < 0).sum()
            issues.append(f'{negative_count} negative exchange values')
        
        if (self.data['year'] < 1997).any() or (self.data['year'] > 2021).any():
            invalid_years = self.data[
                (self.data['year'] < 1997) | (self.data['year'] > 2021)
            ]['year'].unique()
            issues.append(f'Years outside expected range (1997-2021): {invalid_years}')
        
        if issues:
            for issue in issues:
                logging.warning(f'Data quality issue: {issue}')
        else:
            logging.info('Data range validation passed')
        
        return True

    def validate_ecosystem_types(self):
        """
        Validate ecosystem type values match expected categories.
        Expected values: 'Inshore — Reef', 'Coastal — Open Ocean', 'All Ecosystems'
        """
        logging.info('Validating ecosystem types...')
        
        expected_ecosystems = [
            'Inshore — Reef',
            'Coastal — Open Ocean',
            'All Ecosystems'
        ]
        
        unique_ecosystems = self.data['ecosystem_type'].unique()
        unexpected = [e for e in unique_ecosystems if e not in expected_ecosystems]
        
        if unexpected:
            logging.warning(f'Unexpected ecosystem types: {unexpected}')
        else:
            logging.info(f'All ecosystem types valid: {sorted(unique_ecosystems)}')

    def validate_species_groups(self):
        """
        Validate species group values match expected categories.
        Expected: Deep 7 Bottomfish, Shallow Bottomfish, Pelagics, Reef-Associated, All Species
        """
        logging.info('Validating species groups...')
        
        expected_species = [
            'Deep 7 Bottomfish',
            'Shallow Bottomfish',
            'Pelagics',
            'Reef-Associated',
            'All Species'
        ]
        
        unique_species = self.data['species_group'].unique()
        unexpected = [s for s in unique_species if s not in expected_species]
        
        if unexpected:
            logging.warning(f'Unexpected species groups: {unexpected}')
        else:
            logging.info(f'All species groups valid: {sorted(unique_species)}')

    def validate_counties(self):
        """
        Validate county names match known Hawaii counties.
        """
        logging.info('Validating county names...')
        
        expected_counties = ['Hawaii', 'Maui', 'Honolulu', 'Kauai', 'Kalawao']
        
        unique_counties = self.data['county'].unique()
        unexpected = [c for c in unique_counties if c not in expected_counties]
        
        if unexpected:
            logging.warning(f'Unexpected county names: {unexpected}')
        else:
            logging.info(f'All counties valid: {sorted(unique_counties)}')

    def remove_null_values(self):
        """
        Remove records with null or NA exchange values.
        These cannot be used in analysis or visualization.
        """
        logging.info('Removing null/NA exchange values...')
        
        before_count = len(self.data)
        self.data = self.data[self.data['exchange_value'].notna()].copy()
        after_count = len(self.data)
        
        removed = before_count - after_count
        if removed > 0:
            logging.info(f'Removed {removed:,} records with null/NA exchange values')
        else:
            logging.info('No null/NA values to remove')

    def remove_aggregate_rows(self, remove_aggregates=True):
        """
        Optionally remove aggregate summary rows to prevent double-counting.
        Removes records where species_group is 'All Species' or ecosystem_type is 'All Ecosystems'.
        
        Args:
            remove_aggregates (bool): If True, remove aggregate rows. Default True.
        """
        if not remove_aggregates:
            logging.info('Skipping aggregate row removal (remove_aggregates=False)')
            return
        
        logging.info('Removing aggregate rows...')
        
        before_count = len(self.data)
        
        self.data = self.data[
            ~self.data['species_group'].isin(['All Species'])
        ].copy()
        
        self.data = self.data[
            ~self.data['ecosystem_type'].isin(['All Ecosystems'])
        ].copy()
        
        after_count = len(self.data)
        removed = before_count - after_count
        
        if removed > 0:
            logging.info(f'Removed {removed:,} aggregate rows')
        else:
            logging.info('No aggregate rows found to remove')

    def remove_display_columns(self, remove_display=False):
        """
        Optionally remove display-only columns to create analysis-ready dataset.
        Display columns: county_olelo, exchange_value_formatted
        
        Args:
            remove_display (bool): If True, remove display columns. Default False.
        """
        if not remove_display:
            logging.info('Keeping display columns (remove_display=False)')
            return
        
        logging.info('Removing display-only columns...')
        
        display_columns = ['county_olelo', 'exchange_value_formatted']
        columns_to_drop = [col for col in display_columns if col in self.data.columns]
        
        if columns_to_drop:
            self.data = self.data.drop(columns=columns_to_drop)
            logging.info(f'Removed columns: {columns_to_drop}')
        else:
            logging.info('No display columns to remove')

    def generate_summary_statistics(self):
        """
        Generate summary statistics for the cleaned dataset.
        
        Returns:
            dict: Summary statistics including row counts, date ranges, and unique values
        """
        logging.info('Generating summary statistics...')
        
        summary = {
            'data_type': 'commercial',
            'processing_timestamp': datetime.now().isoformat(),
            'raw_row_count': self.raw_row_count,
            'cleaned_row_count': len(self.data),
            'rows_removed': self.raw_row_count - len(self.data),
            'date_range': {
                'min_year': int(self.data['year'].min()),
                'max_year': int(self.data['year'].max())
            },
            'total_exchange_value': float(self.data['exchange_value'].sum()),
            'unique_counties': sorted(self.data['county'].unique().tolist()),
            'unique_species_groups': sorted(self.data['species_group'].unique().tolist()),
            'unique_ecosystem_types': sorted(self.data['ecosystem_type'].unique().tolist()),
            'unique_area_ids': sorted([int(x) for x in self.data['area_id'].unique().tolist() if pd.notna(x)]),
            'records_by_year': self.data.groupby('year').size().to_dict(),
            'total_value_by_year': self.data.groupby('year')['exchange_value'].sum().to_dict()
        }
        
        return summary

    def export_cleaned_data(self):
        """
        Export cleaned data to CSV file.
        Saves to output directory with timestamp.
        
        Returns:
            Path: Path to exported file
        """
        logging.info('Exporting cleaned commercial data...')
        
        timestamp = datetime.now().strftime('%Y%m%d')
        output_file = self.output_dir / f'cleaned_commercial_{timestamp}.csv'
        
        self.data.to_csv(output_file, index=False)
        self.cleaned_row_count = len(self.data)
        
        logging.info(f'Exported {self.cleaned_row_count:,} rows to {output_file}')
        return output_file

    def run_cleaning_pipeline(self, remove_aggregates=True, remove_display=False):
        """
        Execute the complete data cleaning pipeline.
        Runs all cleaning steps in sequence.
        
        Args:
            remove_aggregates (bool): Remove 'All Species' and 'All Ecosystems' rows. Default True.
            remove_display (bool): Remove display-only columns. Default False.
        
        Returns:
            tuple: (success: bool, output_file: Path, summary: dict)
        """
        logging.info('=' * 60)
        logging.info('COMMERCIAL FISHERIES DATA CLEANING PIPELINE')
        logging.info('=' * 60)
        
        if not self.load_data():
            return False, None, None
        
        if not self.validate_schema():
            return False, None, None
        
        self.validate_data_types()
        self.validate_data_ranges()
        self.validate_ecosystem_types()
        self.validate_species_groups()
        self.validate_counties()
        
        self.remove_null_values()
        
        self.remove_aggregate_rows(remove_aggregates=remove_aggregates)
        self.remove_display_columns(remove_display=remove_display)
        
        output_file = self.export_cleaned_data()
        summary = self.generate_summary_statistics()
        
        logging.info('=' * 60)
        logging.info('COMMERCIAL DATA CLEANING COMPLETE')
        logging.info(f'Input:  {self.raw_row_count:,} rows')
        logging.info(f'Output: {self.cleaned_row_count:,} rows')
        logging.info(f'Removed: {self.raw_row_count - self.cleaned_row_count:,} rows')
        logging.info('=' * 60)
        
        return True, output_file, summary

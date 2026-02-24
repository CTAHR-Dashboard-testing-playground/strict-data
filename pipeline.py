"""
Fisheries Data Cleaning Pipeline

Main orchestrator that coordinates cleaning of both commercial and non-commercial
fisheries tidied data. Generates combined summary statistics and exports metadata.
"""

import json
import logging
from pathlib import Path
from datetime import datetime

from clean_commercial import CommercialDataCleaner
from clean_noncommercial import NonCommercialDataCleaner


class FisheriesCleaningPipeline:
    """
    Orchestrates the complete data cleaning pipeline for both commercial
    and non-commercial fisheries datasets.
    """

    def __init__(self, input_dir='data/raw', output_dir='data/cleaned'):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.commercial_cleaner = CommercialDataCleaner(input_dir, output_dir)
        self.noncommercial_cleaner = NonCommercialDataCleaner(input_dir, output_dir)
        
        self.results = {
            'commercial': None,
            'non_commercial': None
        }

    def setup_logging(self):
        """
        Configure logging for the entire pipeline.
        Creates log directory and sets up file and console handlers.
        """
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f'cleaning_pipeline_{timestamp}.log'
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        logging.info(f'Logging initialized: {log_file}')

    def run_commercial_cleaning(self, remove_aggregates=True, remove_display=False):
        """
        Execute commercial data cleaning pipeline.
        
        Args:
            remove_aggregates (bool): Remove 'All Species' and 'All Ecosystems' rows
            remove_display (bool): Remove display-only columns
        
        Returns:
            bool: True if cleaning successful, False otherwise
        """
        logging.info('')
        logging.info('STARTING COMMERCIAL DATA CLEANING')
        logging.info('')
        
        success, output_file, summary = self.commercial_cleaner.run_cleaning_pipeline(
            remove_aggregates=remove_aggregates,
            remove_display=remove_display
        )
        
        if success:
            self.results['commercial'] = {
                'success': True,
                'output_file': str(output_file),
                'summary': summary
            }
            return True
        else:
            self.results['commercial'] = {
                'success': False,
                'error': 'Commercial data cleaning failed'
            }
            return False

    def run_noncommercial_cleaning(self, remove_aggregates=True, remove_display=False):
        """
        Execute non-commercial data cleaning pipeline.
        
        Args:
            remove_aggregates (bool): Remove 'All Ecosystems' rows
            remove_display (bool): Remove display-only columns
        
        Returns:
            bool: True if cleaning successful, False otherwise
        """
        logging.info('')
        logging.info('STARTING NON-COMMERCIAL DATA CLEANING')
        logging.info('')
        
        success, output_file, summary = self.noncommercial_cleaner.run_cleaning_pipeline(
            remove_aggregates=remove_aggregates,
            remove_display=remove_display
        )
        
        if success:
            self.results['non_commercial'] = {
                'success': True,
                'output_file': str(output_file),
                'summary': summary
            }
            return True
        else:
            self.results['non_commercial'] = {
                'success': False,
                'error': 'Non-commercial data cleaning failed'
            }
            return False

    def generate_combined_summary(self):
        """
        Generate combined summary statistics from both datasets.
        Creates a comprehensive metadata file for downstream applications.
        
        Returns:
            dict: Combined summary statistics
        """
        logging.info('Generating combined summary statistics...')
        
        combined_summary = {
            'pipeline_timestamp': datetime.now().isoformat(),
            'commercial': self.results['commercial']['summary'] if self.results['commercial'] else None,
            'non_commercial': self.results['non_commercial']['summary'] if self.results['non_commercial'] else None
        }
        
        if self.results['commercial'] and self.results['non_commercial']:
            combined_summary['overall'] = {
                'total_records': (
                    self.results['commercial']['summary']['cleaned_row_count'] +
                    self.results['non_commercial']['summary']['cleaned_row_count']
                ),
                'total_exchange_value': (
                    self.results['commercial']['summary']['total_exchange_value'] +
                    self.results['non_commercial']['summary']['total_exchange_value']
                ),
                'combined_date_range': {
                    'min_year': min(
                        self.results['commercial']['summary']['date_range']['min_year'],
                        self.results['non_commercial']['summary']['date_range']['min_year']
                    ),
                    'max_year': max(
                        self.results['commercial']['summary']['date_range']['max_year'],
                        self.results['non_commercial']['summary']['date_range']['max_year']
                    )
                }
            }
        
        return combined_summary

    def export_summary_json(self, summary):
        """
        Export combined summary statistics to JSON file.
        
        Args:
            summary (dict): Combined summary statistics
            
        Returns:
            Path: Path to exported JSON file
        """
        logging.info('Exporting summary statistics to JSON...')
        
        timestamp = datetime.now().strftime('%Y%m%d')
        output_file = self.output_dir / f'cleaning_summary_{timestamp}.json'
        
        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logging.info(f'Summary exported to {output_file}')
        return output_file

    def generate_pipeline_report(self):
        """
        Generate a human-readable report of the cleaning pipeline results.
        Prints summary to console and log.
        """
        logging.info('')
        logging.info('=' * 70)
        logging.info('FISHERIES DATA CLEANING PIPELINE - FINAL REPORT')
        logging.info('=' * 70)
        
        if self.results['commercial'] and self.results['commercial']['success']:
            comm_summary = self.results['commercial']['summary']
            logging.info('')
            logging.info('COMMERCIAL FISHERIES:')
            logging.info(f"  Status: SUCCESS")
            logging.info(f"  Input Rows:  {comm_summary['raw_row_count']:,}")
            logging.info(f"  Output Rows: {comm_summary['cleaned_row_count']:,}")
            logging.info(f"  Removed:     {comm_summary['rows_removed']:,}")
            logging.info(f"  Date Range:  {comm_summary['date_range']['min_year']}-{comm_summary['date_range']['max_year']}")
            logging.info(f"  Total Value: ${comm_summary['total_exchange_value']:,.2f}")
            logging.info(f"  Counties:    {len(comm_summary['unique_counties'])}")
            logging.info(f"  Species:     {len(comm_summary['unique_species_groups'])}")
            logging.info(f"  DAR Areas:   {len(comm_summary['unique_area_ids'])}")
        else:
            logging.info('')
            logging.info('COMMERCIAL FISHERIES: FAILED')
        
        if self.results['non_commercial'] and self.results['non_commercial']['success']:
            noncomm_summary = self.results['non_commercial']['summary']
            logging.info('')
            logging.info('NON-COMMERCIAL FISHERIES:')
            logging.info(f"  Status: SUCCESS")
            logging.info(f"  Input Rows:  {noncomm_summary['raw_row_count']:,}")
            logging.info(f"  Output Rows: {noncomm_summary['cleaned_row_count']:,}")
            logging.info(f"  Removed:     {noncomm_summary['rows_removed']:,}")
            logging.info(f"  Date Range:  {noncomm_summary['date_range']['min_year']}-{noncomm_summary['date_range']['max_year']}")
            logging.info(f"  Total Value: ${noncomm_summary['total_exchange_value']:,.2f}")
            logging.info(f"  Islands:     {len(noncomm_summary['unique_islands'])}")
        else:
            logging.info('')
            logging.info('NON-COMMERCIAL FISHERIES: FAILED')
        
        logging.info('')
        logging.info('=' * 70)

    def run_full_pipeline(self, remove_aggregates=True, remove_display=False):
        """
        Execute the complete data cleaning pipeline for both datasets.
        Runs commercial and non-commercial cleaning, generates summaries,
        and exports all results.
        
        Args:
            remove_aggregates (bool): Remove aggregate rows from both datasets. Default True.
            remove_display (bool): Remove display-only columns. Default False.
        
        Returns:
            bool: True if pipeline completed successfully, False otherwise
        """
        self.setup_logging()
        
        logging.info('=' * 70)
        logging.info('FISHERIES DATA CLEANING PIPELINE - START')
        logging.info('=' * 70)
        logging.info(f'Input Directory:  {self.input_dir.absolute()}')
        logging.info(f'Output Directory: {self.output_dir.absolute()}')
        logging.info(f'Remove Aggregates: {remove_aggregates}')
        logging.info(f'Remove Display Columns: {remove_display}')
        logging.info('=' * 70)
        
        comm_success = self.run_commercial_cleaning(
            remove_aggregates=remove_aggregates,
            remove_display=remove_display
        )
        
        noncomm_success = self.run_noncommercial_cleaning(
            remove_aggregates=remove_aggregates,
            remove_display=remove_display
        )
        
        self.generate_pipeline_report()
        
        overall_success = comm_success and noncomm_success
        
        if overall_success:
            logging.info('PIPELINE STATUS: SUCCESS')
        else:
            logging.info('PIPELINE STATUS: PARTIAL SUCCESS OR FAILURE')
        
        return overall_success


def main():
    """
    Main entry point for the data cleaning pipeline.
    Creates pipeline instance and executes full cleaning process.
    
    Configuration:
        remove_aggregates: Set to True to remove 'All Species' and 'All Ecosystems' rows
        remove_display: Set to True to remove display-only columns (*_olelo, *_formatted)
    """
    pipeline = FisheriesCleaningPipeline(
        input_dir='data/raw',
        output_dir='data/cleaned'
    )
    
    success = pipeline.run_full_pipeline(
        remove_aggregates=False,
        remove_display=False
    )
    
    if success:
        print("\n✓ Data cleaning completed successfully!")
        print(f"  Cleaned files saved to: data/cleaned/")
        print(f"  Summary JSON saved to: data/cleaned/")
    else:
        print("\n✗ Data cleaning encountered errors. Check logs for details.")
    
    return success


if __name__ == '__main__':
    main()

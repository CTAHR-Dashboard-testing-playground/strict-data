"""
Configuration settings for fisheries data cleaning pipeline.

This file contains all configurable parameters for data cleaning,
validation rules, and file paths.

Note: Input data is pre-tidied from process_fisheries_ev_data.qmd
"""

from pathlib import Path


class Config:
    """
    Central configuration for fisheries data cleaning pipeline.
    Adapted for pre-tidied data format.
    """
    
    BASE_DIR = Path(__file__).parent
    
    DATA_RAW_DIR = BASE_DIR / 'data' / 'raw'
    DATA_CLEANED_DIR = BASE_DIR / 'data' / 'cleaned'
    LOGS_DIR = BASE_DIR / 'logs'
    
    COMMERCIAL_FILE_PATTERN = '*tidied_comm_ev*.csv'
    NONCOMMERCIAL_FILE_PATTERN = '*tidied_noncomm_ev*.csv'
    
    VALID_COUNTIES = [
        'Hawaii',
        'Maui',
        'Honolulu',
        'Kauai',
        'Kalawao'
    ]
    
    VALID_ISLANDS = [
        'Hawaii',
        'Kauai',
        'Lanai',
        'Maui',
        'Molokai',
        'Oahu'
    ]
    
    COMMERCIAL_MIN_YEAR = 1997
    COMMERCIAL_MAX_YEAR = 2021
    
    NONCOMMERCIAL_MIN_YEAR = 2005
    NONCOMMERCIAL_MAX_YEAR = 2022
    
    COMMERCIAL_SPECIES_GROUPS = [
        'Deep 7 Bottomfish',
        'Shallow Bottomfish',
        'Pelagics',
        'Reef-Associated',
        'All Species'
    ]
    
    NONCOMMERCIAL_SPECIES_GROUPS = [
        'Herbivores'
    ]
    
    ECOSYSTEM_TYPES = [
        'Inshore — Reef',
        'Coastal — Open Ocean',
        'All Ecosystems'
    ]
    
    AGGREGATE_SPECIES_VALUES = ['All Species']
    
    AGGREGATE_ECOSYSTEM_VALUES = ['All Ecosystems']
    
    DISPLAY_ONLY_COLUMNS = [
        'county_olelo',
        'island_olelo',
        'exchange_value_formatted'
    ]
    
    LOG_LEVEL = 'INFO'
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    
    EXPORT_TIMESTAMP_FORMAT = '%Y%m%d'
    
    REQUIRED_COMMERCIAL_COLUMNS = [
        'year',
        'area_id',
        'county',
        'species_group',
        'ecosystem_type',
        'exchange_value'
    ]
    
    REQUIRED_NONCOMMERCIAL_COLUMNS = [
        'year',
        'island',
        'county',
        'species_group',
        'ecosystem_type',
        'exchange_value'
    ]


def get_config():
    """
    Get configuration instance.
    
    Returns:
        Config: Configuration object
    """
    return Config()

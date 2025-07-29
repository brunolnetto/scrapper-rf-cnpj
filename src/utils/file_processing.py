"""
Unified file processing utilities for the CNPJ ETL project.

This module consolidates file processing functions that were previously
duplicated across the codebase, providing a clean, configurable interface.
"""

from typing import List, Dict, Optional
from pathlib import Path
import re
from unicodedata import normalize
from datetime import timedelta

from setup.logging import logger


class FileProcessor:
    """Unified file processing utilities."""
    
    @staticmethod
    def normalize_filename(
        filename: str, 
        remove_numbers: bool = True, 
        normalize_accents: bool = True,
        remove_extension: bool = True
    ) -> str:
        """
        Unified filename normalization with configurable options.
        
        Args:
            filename: The filename to normalize
            remove_numbers: Whether to remove numbers from the filename
            normalize_accents: Whether to normalize accents and convert to lowercase
            remove_extension: Whether to remove the file extension
            
        Returns:
            The normalized filename
        """
        if remove_extension:
            base_name = Path(filename).stem
        else:
            base_name = filename
        
        if remove_numbers:
            # Remove numbers from the end of the filename
            base_name = re.sub(r'\d+$', '', base_name)
        
        if normalize_accents:
            # Normalize accentuation and convert to lowercase
            base_name = normalize('NFD', base_name).casefold()
        else:
            # Just convert to lowercase
            base_name = base_name.lower()
            
        return base_name
    
    @staticmethod
    def normalize_filenames(
        filenames: List[str], 
        **kwargs
    ) -> Dict[str, List[str]]:
        """
        Group filenames by normalized names.
        
        Args:
            filenames: List of filenames to normalize
            **kwargs: Additional arguments passed to normalize_filename
            
        Returns:
            Dictionary with normalized filenames as keys and original filenames as values
        """
        normalized_dict = {}
        for filename in filenames:
            base_name = FileProcessor.normalize_filename(filename, **kwargs)
            if base_name not in normalized_dict:
                normalized_dict[base_name] = [filename]
            else:
                normalized_dict[base_name].append(filename)
        return normalized_dict
    
    @staticmethod
    def process_filename(filename: str) -> str:
        """
        Legacy function for backward compatibility.
        Processes a filename by removing the extension and numbers, and converting it to lowercase.
        
        Args:
            filename: The filename to process
            
        Returns:
            The processed filename
        """
        return FileProcessor.normalize_filename(
            filename, 
            remove_numbers=True, 
            normalize_accents=False,  # Legacy behavior
            remove_extension=True
        )
    
    @staticmethod
    def process_filenames(filenames: List[str]) -> List[str]:
        """
        Legacy function for backward compatibility.
        Processes a list of filenames and returns unique processed names.
        
        Args:
            filenames: List of filenames to process
            
        Returns:
            List of unique processed filenames
        """
        processed_names = []
        for filename in filenames:
            processed_names.append(FileProcessor.process_filename(filename))
        return list(set(processed_names))
    
    @staticmethod
    def get_date_range(timestamps: List) -> Optional[tuple]:
        """
        Find the minimum and maximum date in a list of datetime timestamps.
        
        Args:
            timestamps: List of datetime timestamps
            
        Returns:
            Tuple containing (min_date, max_date) or None if empty list
        """
        if not timestamps:
            return None  # Handle empty list case

        if len(timestamps) == 1:
            return timestamps[0], timestamps[0] + timedelta(days=0)
        else:
            return min(timestamps), max(timestamps)
    
    @staticmethod
    def convert_to_bytes(size_str: str) -> Optional[int]:
        """
        Convert a size string (e.g., "22K", "321M") into bytes.
        
        Args:
            size_str: The size string to convert
            
        Returns:
            Size in bytes or None if format is invalid
        """
        if not size_str or len(size_str) < 2:
            return None
            
        try:
            size_value = float(size_str[:-1])  # Extract numerical value
            size_unit = size_str[-1].upper()  # Get the unit (K, M, G)
        except (ValueError, IndexError):
            return None

        unit_multiplier = {
            'K': 1024,
            'M': 1024 * 1024,
            'G': 1024 * 1024 * 1024,
            'T': 1024 * 1024 * 1024 * 1024,
            'P': 1024 * 1024 * 1024 * 1024 * 1024
        }

        if size_unit in unit_multiplier:
            return int(size_value * unit_multiplier[size_unit])
        else:
            return None  # Handle invalid units


# Backward compatibility aliases
def normalize_filename(filename: str) -> str:
    """Backward compatibility alias for FileProcessor.normalize_filename."""
    return FileProcessor.normalize_filename(filename)

def normalize_filenames(filenames: List[str]) -> Dict[str, List[str]]:
    """Backward compatibility alias for FileProcessor.normalize_filenames."""
    return FileProcessor.normalize_filenames(filenames)

def process_filename(filename: str) -> str:
    """Backward compatibility alias for FileProcessor.process_filename."""
    return FileProcessor.process_filename(filename)

def process_filenames(filenames: List[str]) -> List[str]:
    """Backward compatibility alias for FileProcessor.process_filenames."""
    return FileProcessor.process_filenames(filenames)

def get_date_range(timestamps: List) -> Optional[tuple]:
    """Backward compatibility alias for FileProcessor.get_date_range."""
    return FileProcessor.get_date_range(timestamps)

def convert_to_bytes(size_str: str) -> Optional[int]:
    """Backward compatibility alias for FileProcessor.convert_to_bytes."""
    return FileProcessor.convert_to_bytes(size_str)
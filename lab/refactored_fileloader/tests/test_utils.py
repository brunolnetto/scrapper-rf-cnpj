#!/usr/bin/env python3
"""
Test utils module
"""
import pytest
from src.utils import normalize_batch

def test_normalize_batch_basic():
    """Test basic batch normalization."""
    batch = [
        ['1', 'Alice', '100'],
        ['2', 'Bob', '200']
    ]
    types_map = {'id': 'INTEGER', 'name': 'TEXT', 'value': 'INTEGER'}
    
    result = normalize_batch(batch, types_map)
    
    assert len(result) == 2
    assert result[0] == ('1', 'Alice', '100')
    assert result[1] == ('2', 'Bob', '200')

def test_normalize_batch_with_none():
    """Test batch normalization with None values."""
    batch = [
        ['1', None, '100'],
        [None, 'Bob', None]
    ]
    types_map = {'id': 'INTEGER', 'name': 'TEXT', 'value': 'INTEGER'}
    
    result = normalize_batch(batch, types_map)
    
    assert result[0] == ('1', None, '100')
    assert result[1] == (None, 'Bob', None)

def test_normalize_batch_mixed_types():
    """Test batch normalization with mixed types."""
    batch = [
        [1, 'Alice', 100.5],  # Numbers
        ['2', 'Bob', '200']   # Strings
    ]
    types_map = {'id': 'INTEGER', 'name': 'TEXT', 'value': 'REAL'}
    
    result = normalize_batch(batch, types_map)
    
    assert result[0] == (1, 'Alice', 100.5)
    assert result[1] == ('2', 'Bob', '200')

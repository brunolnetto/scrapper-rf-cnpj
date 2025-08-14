"""
Robust file type detection utilities for data loading.

This module provides comprehensive file type detection that goes beyond simple
file extension checking to include magic number detection, content analysis,
and encoding detection for reliable data processing.
"""

from pathlib import Path
from typing import Dict, Union
import struct


def detect_file_type_with_confidence(
    file_path: Union[str, Path],
) -> Dict[str, Union[str, float, list]]:
    """
    Enhanced file type detection with confidence scoring and detailed analysis.

    Returns comprehensive analysis including:
    - Detected file type
    - Confidence score (0.0 to 1.0)
    - Evidence collected from each strategy
    - Verification results

    Args:
        file_path: Path to file to analyze

    Returns:
        dict: {
            'type': str,           # 'csv', 'parquet', or 'unknown'
            'confidence': float,   # 0.0 to 1.0
            'evidence': list,      # List of detection evidence
            'verification': dict,  # Detailed verification results
            'certainty': str       # 'high', 'medium', 'low', 'unknown'
        }
    """
    file_path = Path(file_path)

    result = {
        "type": "unknown",
        "confidence": 0.0,
        "evidence": [],
        "verification": {},
        "certainty": "unknown",
    }

    if not file_path.exists():
        result["evidence"].append("File does not exist")
        return result

    # File size check
    file_size = file_path.stat().st_size
    result["verification"]["file_size"] = file_size

    if file_size == 0:
        result["evidence"].append("File is empty")
        return result

    # Strategy 1: Magic number verification (highest confidence)
    magic_result = _verify_magic_numbers(file_path)
    result["verification"]["magic_numbers"] = magic_result

    if magic_result["parquet_signature"]:
        result["type"] = "parquet"
        result["confidence"] = 0.95
        result["evidence"].append(
            f"Parquet magic signature found: {magic_result['signature_details']}"
        )
        result["certainty"] = "high"
        return result

    # Strategy 2: File extension analysis with verification
    extension_result = _verify_file_extension(file_path)
    result["verification"]["extension"] = extension_result

    # Strategy 3: Content structure analysis (for CSV)
    content_result = _verify_content_structure(file_path)
    result["verification"]["content"] = content_result

    # Strategy 4: Binary pattern analysis
    binary_result = _verify_binary_patterns(file_path)
    result["verification"]["binary"] = binary_result

    # Strategy 5: Schema validation (for Parquet)
    if extension_result["suggests_parquet"] or binary_result["parquet_indicators"]:
        schema_result = _verify_parquet_schema(file_path)
        result["verification"]["parquet_schema"] = schema_result

        if schema_result["valid_parquet"]:
            result["type"] = "parquet"
            result["confidence"] = 0.90
            result["evidence"].extend(schema_result["evidence"])
            result["certainty"] = "high"
            return result

    # Determine CSV with confidence scoring
    csv_score = _calculate_csv_confidence(
        extension_result, content_result, binary_result
    )
    result["verification"]["csv_score"] = csv_score

    if csv_score["total_score"] >= 0.7:
        result["type"] = "csv"
        result["confidence"] = csv_score["total_score"]
        result["evidence"] = csv_score["evidence"]

        if csv_score["total_score"] >= 0.9:
            result["certainty"] = "high"
        elif csv_score["total_score"] >= 0.7:
            result["certainty"] = "medium"
        else:
            result["certainty"] = "low"

    return result


def _verify_magic_numbers(file_path: Path) -> Dict:
    """Verify binary magic numbers/signatures with detailed analysis."""
    result = {
        "parquet_signature": False,
        "signature_details": "",
        "file_header": "",
        "file_footer": "",
    }

    try:
        with open(file_path, "rb") as f:
            # Read first 16 bytes
            header = f.read(16)
            result["file_header"] = header.hex()

            # Check for Parquet magic number at start
            if header.startswith(b"PAR1"):
                result["parquet_signature"] = True
                result["signature_details"] = "PAR1 at file start"
                return result

            # Check for Parquet magic at offset 4
            if len(header) >= 8 and header[4:8] == b"PAR1":
                result["parquet_signature"] = True
                result["signature_details"] = "PAR1 at offset 4"
                return result

            # Check footer for Parquet signature
            if file_path.stat().st_size > 8:
                f.seek(-8, 2)
                footer = f.read(8)
                result["file_footer"] = footer.hex()

                if footer.endswith(b"PAR1"):
                    result["parquet_signature"] = True
                    result["signature_details"] = "PAR1 at file end"
                    return result

                # Check for Parquet metadata length + PAR1
                if len(footer) >= 8:
                    # Last 4 bytes should be metadata length, followed by PAR1
                    metadata_len = struct.unpack("<I", footer[:4])[0]
                    if footer[4:8] == b"PAR1" and metadata_len > 0:
                        result["parquet_signature"] = True
                        result["signature_details"] = (
                            f"PAR1 with metadata length {metadata_len}"
                        )
                        return result

    except Exception as e:
        result["error"] = str(e)

    return result


def _verify_file_extension(file_path: Path) -> Dict:
    """Verify file extension with detailed analysis."""
    extension = file_path.suffix.lower()
    name = file_path.name.lower()

    result = {
        "extension": extension,
        "suggests_csv": False,
        "suggests_parquet": False,
        "cnpj_pattern": False,
        "confidence": 0.0,
    }

    # Definitive Parquet extension
    if extension == ".parquet":
        result["suggests_parquet"] = True
        result["confidence"] = 0.95
        return result

    # Standard CSV extensions
    if extension in [".csv", ".txt"]:
        result["suggests_csv"] = True
        result["confidence"] = 0.8

    # CNPJ-specific patterns
    cnpj_patterns = ["cnae", "moti", "munic", "pais", "quals", "natju", "simples"]
    if any(pattern in name for pattern in cnpj_patterns):
        result["cnpj_pattern"] = True
        result["suggests_csv"] = True
        result["confidence"] = max(result["confidence"], 0.7)

    # Special CNPJ extensions
    if extension.endswith("csv") or "csv" in extension:
        result["suggests_csv"] = True
        result["confidence"] = max(result["confidence"], 0.8)

    return result


def _verify_content_structure(file_path: Path) -> Dict:
    """Verify content structure for CSV files with detailed analysis."""
    result = {
        "is_text": False,
        "has_separators": False,
        "consistent_fields": False,
        "field_count": 0,
        "separator": None,
        "sample_lines": [],
        "encoding": "unknown",
        "confidence": 0.0,
    }

    # Try different encodings
    encodings = ["utf-8", "latin1", "cp1252", "iso-8859-1"]

    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding, errors="ignore") as f:
                lines = []
                for i, line in enumerate(f):
                    if i >= 5:  # Read max 5 lines for analysis
                        break
                    lines.append(line.strip())

                if not lines:
                    continue

                result["encoding"] = encoding
                result["is_text"] = True
                result["sample_lines"] = lines[:3]  # Store first 3 lines

                # Analyze separator patterns
                separators = [";", ",", "\t", "|"]
                best_separator = None
                best_consistency = 0

                for sep in separators:
                    field_counts = [len(line.split(sep)) for line in lines if line]
                    if not field_counts:
                        continue

                    # Check consistency
                    if len(set(field_counts)) <= 2:  # Allow 1 line variation
                        avg_fields = sum(field_counts) / len(field_counts)
                        if avg_fields >= 2:  # Must have at least 2 fields
                            consistency = 1.0 - (len(set(field_counts)) - 1) / len(
                                field_counts
                            )
                            if consistency > best_consistency:
                                best_consistency = consistency
                                best_separator = sep
                                result["field_count"] = int(avg_fields)

                if best_separator:
                    result["has_separators"] = True
                    result["separator"] = best_separator
                    result["consistent_fields"] = best_consistency > 0.8

                    # Calculate confidence based on structure quality
                    confidence = 0.0
                    if result["consistent_fields"]:
                        confidence += 0.4
                    if result["field_count"] >= 2:
                        confidence += 0.3
                    if best_consistency > 0.9:
                        confidence += 0.2
                    if len(lines) >= 2:
                        confidence += 0.1

                    result["confidence"] = confidence

                break  # Successfully processed with this encoding

        except Exception:
            continue

    return result


def _verify_binary_patterns(file_path: Path) -> Dict:
    """Verify binary patterns and content characteristics."""
    result = {
        "mostly_binary": False,
        "mostly_text": False,
        "parquet_indicators": False,
        "csv_indicators": False,
        "printable_ratio": 0.0,
        "sample_bytes": "",
    }

    try:
        with open(file_path, "rb") as f:
            # Read first 2KB for analysis
            content = f.read(2048)

            if not content:
                return result

            result["sample_bytes"] = content[:100].hex()  # First 100 bytes as hex

            # Calculate printable character ratio
            printable_count = sum(
                1 for b in content if 32 <= b <= 126 or b in [9, 10, 13]
            )
            result["printable_ratio"] = printable_count / len(content)

            # Determine if mostly text or binary
            if result["printable_ratio"] > 0.8:
                result["mostly_text"] = True
            elif result["printable_ratio"] < 0.3:
                result["mostly_binary"] = True

            # Look for Parquet-specific patterns
            parquet_keywords = [b"schema", b"thrift", b"apache", b"parquet", b"PARQUET"]
            if any(keyword in content.lower() for keyword in parquet_keywords):
                result["parquet_indicators"] = True

            # Look for CSV-specific patterns
            if result["mostly_text"] and (b";" in content or b"," in content):
                # Count line breaks and separators
                line_breaks = content.count(b"\n") + content.count(b"\r\n")
                separators = content.count(b";") + content.count(b",")

                if line_breaks > 0 and separators > line_breaks:
                    result["csv_indicators"] = True

    except Exception as e:
        result["error"] = str(e)

    return result


def _verify_parquet_schema(file_path: Path) -> Dict:
    """Verify Parquet file by attempting to read schema."""
    result = {
        "valid_parquet": False,
        "schema_readable": False,
        "column_count": 0,
        "columns": [],
        "evidence": [],
        "error": None,
    }

    try:
        import polars as pl

        # Try to read schema without loading data
        lazy_df = pl.scan_parquet(file_path)
        schema = lazy_df.collect_schema()

        result["valid_parquet"] = True
        result["schema_readable"] = True
        result["column_count"] = len(schema)
        result["columns"] = list(schema.keys())
        result["evidence"].append(f"Valid Parquet schema with {len(schema)} columns")

        # Try to get row count (optional)
        try:
            row_count = lazy_df.select(pl.len()).collect().item()
            result["evidence"].append(f"File contains {row_count:,} rows")
        except:
            pass

    except ImportError:
        result["error"] = "Polars not available for Parquet verification"
    except Exception as e:
        result["error"] = str(e)

    return result


def _calculate_csv_confidence(
    extension_result: Dict, content_result: Dict, binary_result: Dict
) -> Dict:
    """Calculate overall CSV confidence score from multiple factors."""
    evidence = []
    scores = {}

    # Extension score
    if extension_result["suggests_csv"]:
        scores["extension"] = extension_result["confidence"]
        evidence.append(
            f"Extension suggests CSV (confidence: {extension_result['confidence']:.2f})"
        )
    else:
        scores["extension"] = 0.0

    # Content structure score
    if content_result["is_text"]:
        scores["content"] = content_result["confidence"]
        if content_result["has_separators"]:
            evidence.append(
                f"Valid CSV structure: {content_result['field_count']} fields, separator '{content_result['separator']}'"
            )
        if content_result["consistent_fields"]:
            evidence.append("Consistent field count across lines")
    else:
        scores["content"] = 0.0

    # Binary analysis score
    if binary_result["mostly_text"]:
        scores["binary"] = 0.3
        evidence.append(
            f"Text file (printable ratio: {binary_result['printable_ratio']:.2f})"
        )
    elif binary_result["csv_indicators"]:
        scores["binary"] = 0.2
        evidence.append("Contains CSV-like patterns")
    else:
        scores["binary"] = 0.0

    # CNPJ pattern bonus
    if extension_result["cnpj_pattern"]:
        scores["cnpj_bonus"] = 0.1
        evidence.append("Matches CNPJ file naming pattern")
    else:
        scores["cnpj_bonus"] = 0.0

    # Calculate weighted total
    total_score = (
        scores["extension"] * 0.4  # Extension is important
        + scores["content"] * 0.5  # Content structure is most important
        + scores["binary"] * 0.1  # Binary analysis is supporting
        + scores["cnpj_bonus"]  # CNPJ pattern is bonus
    )

    return {
        "total_score": min(total_score, 1.0),  # Cap at 1.0
        "component_scores": scores,
        "evidence": evidence,
    }


def detect_file_type(file_path: Union[str, Path]) -> str:
    """
    Main file type detection function with enhanced accuracy.

    This function now uses the enhanced detection system internally
    but maintains the simple string return interface for compatibility.

    Args:
        file_path: Path to file to analyze

    Returns:
        str: 'csv', 'parquet', or 'unknown'
    """
    result = detect_file_type_with_confidence(file_path)

    # Only return definitive results for high confidence
    if result["certainty"] in ["high", "medium"] and result["confidence"] >= 0.7:
        return result["type"]

    return "unknown"


def get_file_info(file_path: Union[str, Path]) -> Dict[str, Union[str, int, bool]]:
    """
    Get comprehensive file information including robust type detection.

    Args:
        file_path: Path to file to analyze

    Returns:
        dict: File information with enhanced detection results
    """
    file_path = Path(file_path)
    detection_result = detect_file_type_with_confidence(file_path)

    return {
        "path": str(file_path),
        "name": file_path.name,
        "extension": file_path.suffix,
        "size": file_path.stat().st_size if file_path.exists() else 0,
        "exists": file_path.exists(),
        "detected_type": detection_result["type"],
        "confidence": detection_result["confidence"],
        "certainty": detection_result["certainty"],
        "is_certain": detection_result["confidence"] >= 0.9
        and detection_result["type"] != "unknown",
    }


def is_valid_data_file(
    file_path: Union[str, Path], min_confidence: float = 0.7
) -> bool:
    """
    Check if file is a valid data file (CSV or Parquet) with sufficient confidence.

    Args:
        file_path: Path to file to check
        min_confidence: Minimum confidence threshold

    Returns:
        bool: True if file is valid data file with sufficient confidence
    """
    result = detect_file_type_with_confidence(file_path)
    return (
        result["type"] in ["csv", "parquet"] and result["confidence"] >= min_confidence
    )

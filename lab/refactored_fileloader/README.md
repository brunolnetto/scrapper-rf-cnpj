# Refactored Fileloader Package

## Usage Instructions

All modules in this package use relative imports. To run tests or scripts, always use the package context from the workspace root:

```bash
python -m pytest lab/refactored_fileloader/tests/ --cov=lab/refactored_fileloader --cov-report=term-missing
```

To run CLI or other scripts:

```bash
python -m lab.refactored_fileloader.cli --help
```

This ensures all imports resolve correctly and coverage is measured for operational modules only.

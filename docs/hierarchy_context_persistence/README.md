# Lab Directory Index
## CNPJ ETL Pipeline - Development & Analysis Files

*Last Updated: September 9, 2025*

---

## 📚 **Documentation**

### **Batch System** _(Consolidated)_
- **`README_batch_system.md`** - Complete implementation guide, problem resolution, and deployment reference
- **`batch_subbatch_manifest_analysis.md`** - Technical reference for database models and implementation details
- **`test_corrected_batch_hierarchy.py`** - Validation script for batch calculations and hierarchy

### **Analysis & Research**
- **`main.ipynb`** - Primary data analysis and exploration notebook
- **`pk_candidate_evaluator.py`** - Primary key analysis for CNPJ data
- **`test_files_row_integrity.py`** - Data validation across file formats

---

## 🔬 **Development Tools**

### **Performance Monitoring**
- **`memory_monitor.py`** - Memory usage tracking for ETL processes
- **`parquet_to_postgres.py`** - Database loading analysis
- **`sink_into_duckdb.py`** - Alternative database sink analysis

### **Data Processing Examples**
- **`upsert_csv_example.py`** - CSV upsert patterns
- **`upsert_parquet_example.py`** - Parquet database loading examples

---

## 🏗️ **Refactored Components**

### **File Loader Refactoring**
- **`refactored_fileloader/`** - Complete file loader redesign project
  - Independent Docker environment
  - Enhanced file format detection
  - Performance improvements

---

## 📋 **Development Workflow**

### **When Working on Batch System**
1. **Reference**: `README_batch_system.md` for problem context and implementation
2. **Technical Details**: `batch_subbatch_manifest_analysis.md` for database schemas
3. **Validation**: Run `test_corrected_batch_hierarchy.py` to verify calculations

### **When Analyzing Data**
1. **Primary Tool**: `main.ipynb` for interactive analysis
2. **Validation**: `test_files_row_integrity.py` for data quality checks
3. **Performance**: `memory_monitor.py` for resource monitoring

### **When Testing File Processing**
1. **Examples**: `upsert_*_example.py` files for loading patterns
2. **Alternative**: `refactored_fileloader/` for enhanced file handling
3. **Research**: `pk_candidate_evaluator.py` for key analysis

---

## 🧹 **Cleanup History**

### **Files Removed (2025-09-09)**
- ❌ `duplicate_batch_investigation_report.md` - Consolidated into README_batch_system.md
- ❌ `corrected_batch_hierarchy_implementation.md` - Merged with main guide
- ❌ `implementation_complete_summary.md` - Information integrated
- ❌ `manifest_hierarchy_correction.md` - Corrections applied
- ❌ `final_status_manifest_correction.md` - Status moved to main docs

**Reason**: Eliminated redundant documentation and over-documentation issues

---

## 🎯 **Quick Reference**

### **Need to...**
- **Fix batch issues**: See `README_batch_system.md`
- **Understand database models**: See `batch_subbatch_manifest_analysis.md`
- **Test calculations**: Run `test_corrected_batch_hierarchy.py`
- **Analyze data**: Open `main.ipynb`
- **Check performance**: Use `memory_monitor.py`
- **Load data examples**: Check `upsert_*_example.py`

### **File Organization Principle**
- **Single source of truth** for each topic
- **No redundant documentation**
- **Clear separation** between guides and technical references
- **Consolidated information** rather than scattered files

---

*This index maintains organization and prevents future over-documentation.*

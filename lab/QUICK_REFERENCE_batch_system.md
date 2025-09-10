# Quick Reference - Batch System
## CNPJ ETL Pipeline - Developer Cheat Sheet

*Quick access guide for daily development work*

---

## 🎯 **At a Glance**

### **Correct Hierarchy**
```
Table (1) → File (1 per CSV) → Batch (ceil(rows/10K)) → Subbatch (ceil(batch/1K))
```

### **Expected Batch Names**
- ✅ `Batch_simples_1of10_rows0-1000_20250909_*`
- ❌ `File_Simples.zip_*` (should not exist anymore)

---

## 🔧 **Configuration**

### **Key Environment Variables**
```bash
ETL_ROW_BATCH_SIZE=10000          # Rows per batch (default)
ETL_ROW_SUBBATCH_SIZE=1000        # Rows per subbatch (default)
ETL_DEV_MODE=true                 # Enable development filtering
```

### **Batch Size Impact**
- **10K rows/batch**: Normal file → 1 batch, 10 subbatches
- **1M rows**: Large file → 100 batches, 1000 subbatches
- **42.78M rows**: Simples.zip → 4278 batches, 42780 subbatches

---

## 🔍 **Quick Validation**

### **Check for Duplicate Batches (Should be 0)**
```sql
SELECT COUNT(*) FROM batch_ingestion_manifest 
WHERE batch_name LIKE '%File_%' 
  AND started_at >= NOW() - INTERVAL '1 hour';
```

### **Verify Correct Hierarchy**
```sql
SELECT 
    table_name,
    COUNT(DISTINCT batch_id) as batches,
    COUNT(DISTINCT subbatch_manifest_id) as subbatches
FROM v_batch_hierarchy 
WHERE created_at >= NOW() - INTERVAL '1 hour'
GROUP BY table_name;
```

### **Check Batch Completion**
```sql
SELECT status, COUNT(*) FROM batch_ingestion_manifest
WHERE started_at >= NOW() - INTERVAL '1 hour'
GROUP BY status;
```

---

## 🚨 **Troubleshooting**

### **Problem: Still seeing File_* batches**
- **Cause**: Old code still running
- **Solution**: Verify `strategies.py` updated, restart process

### **Problem: Batches never complete**
- **Cause**: Subbatch failures
- **Check**: `SELECT * FROM batch_ingestion_manifest WHERE status = 'RUNNING'`
- **Solution**: Review subbatch error messages

### **Problem: Too many manifest records**
- **Expected**: ~47K records for 42.78M row file
- **If seeing**: ~85K records, file batches still being created
- **Solution**: Verify fix deployed

---

## 📊 **Performance Expectations**

### **For Large Files (42.78M rows)**
- **Before**: ~85,839 manifest records (with duplicates)
- **After**: ~47,059 manifest records (45% reduction)
- **Batches**: 4,278 (row-driven segmentation)
- **Subbatches**: ~42,780 (granular processing)

### **For Normal Files (10K rows)**
- **Table**: 1 record
- **File**: 1 record
- **Batches**: 1 record (single batch)
- **Subbatches**: 10 records
- **Total**: 13 records

---

## 🎯 **Testing Commands**

### **Run Validation Script**
```bash
cd lab && python3 test_corrected_batch_hierarchy.py
```

### **Check Implementation**
```bash
grep -n "_process_file_batch\|File_.*zip" src/core/services/loading/strategies.py
```

### **Monitor Processing**
```bash
# Watch for new batches
psql -c "SELECT batch_name, status FROM batch_ingestion_manifest ORDER BY started_at DESC LIMIT 5;"
```

---

## 📚 **Documentation Links**

- **Full Guide**: `lab/README_batch_system.md`
- **Technical Reference**: `lab/batch_subbatch_manifest_analysis.md`
- **Validation Script**: `lab/test_corrected_batch_hierarchy.py`

---

*Keep this handy for quick lookups during development!*

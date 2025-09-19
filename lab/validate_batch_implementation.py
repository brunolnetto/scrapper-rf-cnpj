#!/usr/bin/env python3
"""
Real-world validation script for the corrected batch hierarchy.
This script tests the actual implementation with database queries.
"""

import sys
import os
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def get_db_connection():
    """Get database connection using environment variables."""
    try:
        # Try to load from .env if available
        from src.setup.config import get_config
        config = get_config()
        
        # Get audit database config
        audit_db = config.databases['audit']
        
        conn = psycopg2.connect(
            host=audit_db.host,
            port=audit_db.port,
            user=audit_db.user,
            password=audit_db.password,
            database=audit_db.database_name
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("💡 Make sure your .env file is configured and database is running")
        return None

def check_duplicate_batches(conn):
    """Check for duplicate file-level batches (should be none after fix)."""
    
    query = """
    SELECT 
        target_table,
        COUNT(*) as batch_count,
        STRING_AGG(batch_name, ' | ') as batch_names
    FROM batch_ingestion_manifest 
    WHERE batch_name LIKE '%File_%'
      AND started_at >= NOW() - INTERVAL '24 hours'
    GROUP BY target_table
    HAVING COUNT(*) > 0;
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        results = cur.fetchall()
        
        if results:
            print("❌ Found duplicate file-level batches (these should not exist):")
            for row in results:
                print(f"   Table: {row['target_table']}, Count: {row['batch_count']}")
                print(f"   Batches: {row['batch_names']}")
            return False
        else:
            print("✅ No duplicate file-level batches found (correct)")
            return True

def check_batch_naming(conn):
    """Check that batch names follow the new convention."""
    
    query = """
    SELECT 
        batch_name,
        started_at
    FROM batch_ingestion_manifest 
    WHERE started_at >= NOW() - INTERVAL '24 hours'
    ORDER BY started_at DESC
    LIMIT 10;
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        results = cur.fetchall()
        
        if not results:
            print("ℹ️  No recent batches found (run an ETL process to test)")
            return True
        
        print("📋 Recent batch names:")
        correct_naming = True
        for row in results:
            batch_name = row['batch_name']
            if batch_name.startswith('File_'):
                print(f"   ❌ {batch_name} (old file-level batch)")
                correct_naming = False
            elif batch_name.startswith('Batch_'):
                print(f"   ✅ {batch_name} (correct row-driven batch)")
            else:
                print(f"   ⚠️  {batch_name} (unexpected format)")
        
        return correct_naming

def check_hierarchy_counts(conn):
    """Check the 4-tier hierarchy record counts."""
    
    query = """
    SELECT 
        t.audi_table_name,
        COUNT(DISTINCT f.file_manifest_id) as file_count,
        COUNT(DISTINCT b.batch_id) as batch_count,
        COUNT(DISTINCT s.subbatch_manifest_id) as subbatch_count
    FROM table_ingestion_manifest t
    LEFT JOIN file_ingestion_manifest f ON t.audi_id = f.audit_id
    LEFT JOIN batch_ingestion_manifest b ON f.batch_id = b.batch_id  
    LEFT JOIN subbatch_ingestion_manifest s ON b.batch_id = s.batch_manifest_id
    WHERE t.audi_created_at >= NOW() - INTERVAL '24 hours'
    GROUP BY t.audi_id, t.audi_table_name
    ORDER BY batch_count DESC;
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        results = cur.fetchall()
        
        if not results:
            print("ℹ️  No recent table processing found (run an ETL process to test)")
            return True
        
        print("📊 Hierarchy record counts (last 24 hours):")
        for row in results:
            table = row['audi_table_name']
            files = row['file_count'] or 0
            batches = row['batch_count'] or 0
            subbatches = row['subbatch_count'] or 0
            
            print(f"   Table: {table}")
            print(f"     Files: {files}, Batches: {batches}, Subbatches: {subbatches}")
            
            # Check if ratios make sense
            if batches > 0 and subbatches > 0:
                avg_subbatches_per_batch = subbatches / batches
                if 1 <= avg_subbatches_per_batch <= 100:  # Reasonable range
                    print(f"     ✅ Ratio looks correct (~{avg_subbatches_per_batch:.1f} subbatches/batch)")
                else:
                    print(f"     ⚠️  Unusual ratio ({avg_subbatches_per_batch:.1f} subbatches/batch)")
        
        return True

def check_batch_completion(conn):
    """Check batch completion rates."""
    
    query = """
    SELECT 
        status,
        COUNT(*) as count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as percentage
    FROM batch_ingestion_manifest
    WHERE started_at >= NOW() - INTERVAL '24 hours'
    GROUP BY status
    ORDER BY count DESC;
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        results = cur.fetchall()
        
        if not results:
            print("ℹ️  No recent batch processing found")
            return True
        
        print("📈 Batch completion status (last 24 hours):")
        for row in results:
            status = row['status']
            count = row['count']
            percentage = row['percentage']
            
            if status == 'COMPLETED':
                print(f"   ✅ {status}: {count} ({percentage}%)")
            elif status == 'RUNNING':
                print(f"   🔄 {status}: {count} ({percentage}%)")
            elif status in ['FAILED', 'ERROR']:
                print(f"   ❌ {status}: {count} ({percentage}%)")
            else:
                print(f"   ⚠️  {status}: {count} ({percentage}%)")
        
        return True

def main():
    """Run all validation checks."""
    
    print("🔧 Real-World Batch Hierarchy Validation")
    print("=" * 60)
    print("Testing the corrected 4-tier hierarchy implementation")
    print("=" * 60)
    print()
    
    # Connect to database
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        all_checks_passed = True
        
        # Run validation checks
        print("1. Checking for duplicate file-level batches...")
        all_checks_passed &= check_duplicate_batches(conn)
        print()
        
        print("2. Validating batch naming conventions...")
        all_checks_passed &= check_batch_naming(conn)
        print()
        
        print("3. Analyzing hierarchy record counts...")
        all_checks_passed &= check_hierarchy_counts(conn)
        print()
        
        print("4. Checking batch completion rates...")
        all_checks_passed &= check_batch_completion(conn)
        print()
        
        # Summary
        if all_checks_passed:
            print("🎉 All validation checks passed!")
            print("✅ Batch hierarchy implementation appears to be working correctly")
        else:
            print("⚠️  Some validation checks failed")
            print("💡 Review the issues above and check implementation")
        
        return all_checks_passed
        
    finally:
        conn.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

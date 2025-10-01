"""
Audit Repository - Database operations layer for audit manifests.

This module contains the pure database operations for audit management,
separated from business logic for better testability and maintainability.
"""

import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import UUID
from pathlib import Path

from sqlalchemy import text
import sqlalchemy.exc

from ....setup.logging import logger
from ....database.engine import Database
from ....database.models.audit import AuditStatus


class AuditRepository:
    """
    Repository layer for audit database operations.
    
    Handles all direct database interactions for audit manifests,
    providing a clean interface for the audit service layer.
    """
    
    def __init__(self, database: Database):
        self.database = database
    
    def insert_file_manifest(
        self, 
        file_manifest_id: str, 
        table_name: str, 
        file_path: str, 
        status: AuditStatus,
        checksum: Optional[str], 
        filesize: Optional[int],
        rows_processed: Optional[int], 
        table_manifest_id: str, 
        notes: Optional[str] = None
    ) -> None:
        """Insert file manifest entry into database."""
        try:
            insert_query = '''
            INSERT INTO file_audit_manifest
            (
                file_audit_id, 
                parent_table_audit_id, 
                entity_name, 
                status, 
                created_at,
                started_at,
                file_path,
                checksum,
                filesize,
                rows_processed,
                notes
            )
            VALUES (
                :file_audit_id, 
                :table_audit_id, 
                :entity_name, 
                :status,  
                :created_at,
                :started_at,
                :file_path,
                :checksum,
                :filesize,
                :rows_processed,
                :notes
            )
            '''

            # Coerce status to AuditStatus enum for consistent DB values
            norm_status = self._coerce_status(status)

            # Ensure notes are serialized consistently
            safe_notes = self._serialize_notes(notes)

            with self.database.engine.begin() as conn:
                conn.execute(text(insert_query), {
                    'file_audit_id': file_manifest_id,
                    'table_audit_id': table_manifest_id,
                    'entity_name': table_name,
                    'status': norm_status.value,
                    'created_at': datetime.now(),
                    'started_at': datetime.now(),
                    'file_path': file_path,
                    'checksum': checksum,
                    'filesize': filesize,
                    'rows_processed': rows_processed,
                    'notes': safe_notes
                })

        except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DatabaseError) as e:
            logger.error(f"Database error inserting file manifest entry: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error inserting file manifest entry: {e}")
            raise

    def update_file_manifest(
        self,
        file_manifest_id: str,
        status: Optional[AuditStatus] = None,
        rows_processed: Optional[int] = None,
        error_msg: Optional[str] = None,
        notes: Optional[dict] = None,
    ) -> None:
        """Update an existing file manifest entry with completion details."""
        try:
            # Build update fields dynamically
            update_fields = ['completed_at = :completed_at']
            params = {
                'manifest_id': file_manifest_id,
                'completed_at': datetime.now()
            }
            
            if status is not None:
                norm_status = self._coerce_status(status)
                update_fields.append('status = :status')
                params['status'] = norm_status.value
                
            if error_msg is not None:
                update_fields.append('error_message = :error_msg')
                params['error_msg'] = error_msg
                
            if rows_processed is not None:
                update_fields.append('rows_processed = :rows_processed')
                params['rows_processed'] = rows_processed
                
            if notes is not None:
                update_fields.append('notes = :notes')
                params['notes'] = self._serialize_notes(notes)
            
            # Execute update
            with self.database.engine.begin() as conn:
                conn.execute(text(f'''
                    UPDATE file_audit_manifest 
                    SET {', '.join(update_fields)}
                    WHERE file_audit_id = :manifest_id
                '''), params)
                        
        except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DatabaseError) as e:
            logger.error(f"Database error updating file manifest {file_manifest_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating file manifest {file_manifest_id}: {e}")
            raise

    def find_table_audit(self, table_name: str) -> Optional[str]:
        """Find the most recent table audit ID for a given table name."""
        try:
            query = '''
            SELECT table_audit_id FROM table_audit_manifest 
            WHERE entity_name = :table_name 
            ORDER BY created_at DESC 
            LIMIT 1
            '''
            
            with self.database.engine.connect() as conn:
                result = conn.execute(text(query), {'table_name': table_name})
                row = result.fetchone()
                return str(row[0]) if row else None
                
        except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DatabaseError) as e:
            logger.error(f"Database error finding table audit for {table_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error finding table audit for {table_name}: {e}")
            return None

    def get_file_manifest_history(self, filename: str) -> List[dict]:
        """Get manifest history for a specific file."""
        try:
            query = '''
            SELECT 
                file_path, 
                entity_name as filename, 
                status, 
                checksum, 
                filesize, 
                rows_processed, 
                completed_at
            FROM file_audit_manifest
            WHERE entity_name = :filename
            ORDER BY completed_at DESC
            '''

            with self.database.engine.connect() as conn:
                result = conn.execute(text(query), {'filename': filename})
                return [dict(row._mapping) for row in result]

        except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DatabaseError) as e:
            logger.error(f"Database error getting manifest history for {filename}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting manifest history for {filename}: {e}")
            return []

    def find_subbatch_for_file_manifest(self, file_manifest_id: str) -> Optional[str]:
        """Find the subbatch ID associated with a file manifest."""
        try:
            query = '''
            SELECT s.subbatch_audit_id 
            FROM file_audit_manifest f
            JOIN batch_audit_manifest b ON f.file_audit_id = b.parent_file_audit_id
            JOIN subbatch_audit_manifest s ON b.batch_audit_id = s.parent_batch_audit_id
            WHERE f.file_audit_id = :manifest_id
            ORDER BY s.started_at DESC
            LIMIT 1
            '''
            
            with self.database.engine.connect() as conn:
                result = conn.execute(text(query), {'manifest_id': file_manifest_id})
                row = result.fetchone()
                return str(row[0]) if row else None
                
        except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DatabaseError) as e:
            logger.debug(f"Database error finding subbatch for manifest {file_manifest_id}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Unexpected error finding subbatch for manifest {file_manifest_id}: {e}")
            return None

    def mark_table_audit_started(self, table_name: str) -> Optional[str]:
        """Mark a table audit as started and return its ID."""
        try:
            query = '''
            UPDATE table_audit_manifest 
            SET status = :status, started_at = :started_at 
            WHERE entity_name = :table_name 
            AND (status = :pending_status OR started_at IS NULL)
            RETURNING table_audit_id
            '''
            
            with self.database.engine.begin() as conn:
                result = conn.execute(text(query), {
                    'table_name': table_name,
                    'status': AuditStatus.RUNNING.value,
                    'started_at': datetime.now(),
                    'pending_status': AuditStatus.PENDING.value
                })
                row = result.fetchone()
                return str(row[0]) if row else None
                
        except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DatabaseError) as e:
            logger.debug(f"Database error marking table audit started for {table_name}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Unexpected error marking table audit started for {table_name}: {e}")
            return None

    def update_table_audit_completion(
        self, 
        table_audit_id: str, 
        status: AuditStatus, 
        error_message: Optional[str] = None
    ) -> None:
        """Update table audit completion status."""
        try:
            update_fields = ['status = :status', 'completed_at = :completed_at']
            params = {
                'table_audit_id': table_audit_id,
                'status': status.value,
                'completed_at': datetime.now()
            }
            
            if error_message:
                update_fields.append('error_message = :error_message')
                params['error_message'] = error_message
            
            with self.database.engine.begin() as conn:
                conn.execute(text(f'''
                    UPDATE table_audit_manifest 
                    SET {', '.join(update_fields)}
                    WHERE table_audit_id = :table_audit_id
                '''), params)
                
        except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DatabaseError) as e:
            logger.error(f"Database error updating table audit completion: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating table audit completion: {e}")
            raise

    def get_batch_status(self, batch_id: UUID) -> Optional[Dict[str, Any]]:
        """Get the current status of a batch."""
        try:
            query = '''
            SELECT 
                target_table,
                status,
                created_at,
                started_at,
                completed_at,
                description,
                error_message,
                notes
            FROM batch_audit_manifest
            WHERE batch_audit_id = :batch_id
            '''
            
            with self.database.engine.connect() as conn:
                result = conn.execute(text(query), {'batch_id': str(batch_id)})
                row = result.fetchone()
                return dict(row._mapping) if row else None
                
        except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DatabaseError) as e:
            logger.error(f"Database error getting batch status for {batch_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting batch status for {batch_id}: {e}")
            return None

    def _coerce_status(self, status: AuditStatus | str) -> AuditStatus:
        """Coerce a status value into an AuditStatus enum."""
        try:
            if isinstance(status, AuditStatus):
                return status
            if isinstance(status, str):
                try:
                    return AuditStatus[status.strip().upper()]
                except KeyError:
                    try:
                        return AuditStatus(status)
                    except ValueError:
                        return AuditStatus.PENDING
        except (TypeError, AttributeError):
            return AuditStatus.PENDING
        return AuditStatus.PENDING

    def _serialize_notes(self, notes: Any) -> str:
        """Safely serialize notes to JSON string."""
        try:
            if notes is None:
                return '{}'  # store empty object
            elif isinstance(notes, dict):
                return json.dumps(notes)
            else:
                # wrap string notes into a dict for consistency
                return json.dumps({'notes': str(notes)})
        except (TypeError, ValueError) as json_error:
            logger.debug(f"Failed to serialize notes as JSON, falling back to string: {json_error}")
            return json.dumps({'notes': str(notes)})
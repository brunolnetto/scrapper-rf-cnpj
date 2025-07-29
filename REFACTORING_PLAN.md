# ETL Service-Oriented Refactoring Plan

## Rationale
The ETL project has evolved towards a service-oriented architecture, with dedicated services for audit management, data loading, and (soon) file download/upload. This refactoring will finalize the transition, improve maintainability, and ensure all file operations are robust, testable, and extensible.

## Goals
- Encapsulate all file download and extraction logic in a FileDownloadService class
- (Optionally) Encapsulate file upload logic in an UploadService class
- Integrate these services into the ETL pipeline (CNPJ_ETL, ETLOrchestrator)
- Refactor the Database object to encapsulate its SQLAlchemy Base and initialization logic
- Document the service-oriented pattern for onboarding and future development
- Ensure robust error handling, parallelization, and progress reporting are preserved

## Phase 1: Service Pattern Documentation

### Service-Oriented Pattern Overview

The ETL pipeline is organized around dedicated service classes, each responsible for a single aspect of the workflow:

- **AuditService**: Handles audit record creation, metadata, and insertion into a dedicated audit database.
- **FileDownloadService**: (To be implemented) Handles all file download and extraction logic, including retries, progress bars, and parallelization.
- **DataLoadingService**: Handles loading data into the database, using the strategy pattern to support CSV, Parquet, and auto-detection.

#### Interaction Diagram

```
ETLOrchestrator/CLI
    |
    v
CNPJ_ETL
    |---> FileDownloadService (download/extract files)
    |---> AuditService (manage audit records)
    |---> DataLoadingService (load data into DB)
```

#### Responsibilities
- **Orchestrator**: Coordinates the workflow, passing configuration and control to each service.
- **CNPJ_ETL**: Main ETL logic, composes and uses the services.
- **Services**: Each service is the single source of truth for its domain, with clear, testable interfaces.

#### Benefits
- Separation of concerns
- Testability and extensibility
- Consistent, maintainable codebase
- Easy onboarding for new developers

---

## Phased Refactoring Steps

### Phase 2: FileDownloadService Implementation
- Refactor all file download and extraction logic into FileDownloadService
- Preserve retry, progress bar, and parallelization features
- Add configuration options as needed

### Phase 3: Integration
- Update CNPJ_ETL and ETLOrchestrator to use FileDownloadService
- Remove direct calls to legacy download/extract functions
- (If needed) Implement and integrate UploadService

### Phase 4: Testing & Validation
- Test the full ETL pipeline with the new services
- Validate correctness, performance, and error handling

### Phase 5: Documentation & Rollout
- Update all documentation to reflect the new architecture
- Communicate changes to the team
- Monitor production for issues post-rollout

## Phase 6: Database Object Refactor

### Rationale
Currently, the Database object is a simple container for the engine and session maker, and table creation is handled externally. By refactoring Database to encapsulate its SQLAlchemy Base and initialization logic, we:
- Ensure each Database instance knows which tables it owns
- Encapsulate table creation and migration logic
- Reduce the risk of mixing up tables between databases
- Make the codebase more object-oriented and maintainable

### Steps
1. Update the Database class to accept and store a Base (e.g., MainBase or AuditBase)
2. Add a create_tables() method to Database that calls base.metadata.create_all(engine)
3. Refactor init_database to return a Database object with the correct base
4. Update all code to use Database.create_tables() for table creation
5. Test initialization and migrations for both main and audit databases

### Success Criteria
- Each Database instance only creates and manages its own tables
- No risk of cross-contamination between audit and main tables
- Initialization and migrations are clean and robust
- Code is more maintainable and object-oriented

---

## Risks & Mitigation
- **Risk:** Missed edge cases in file download/extract logic
  - *Mitigation:* Preserve and test all existing features, add new tests
- **Risk:** Disruption to ETL pipeline during integration
  - *Mitigation:* Integrate incrementally, validate at each step
- **Risk:** Onboarding confusion
  - *Mitigation:* Provide clear, updated documentation and migration notes

## Success Criteria
- All file download/extract operations are handled by FileDownloadService
- (If needed) All file upload operations are handled by UploadService
- ETL pipeline is clean, maintainable, and testable
- Documentation is up to date and onboarding is clear
- No loss of robustness or performance in file operations

---

**Owner:** Development Team  
**Version:** 1.1  
**Date:** 2024-12-19
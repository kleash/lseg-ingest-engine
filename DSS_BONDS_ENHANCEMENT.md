# DSS Bonds Ingestion Enhancement

This document tracks the progress of the DSS Bonds (`SG_HK_Bonds`) ingestion feature implementation.

## Tasks

- [x] **Task 1: Project Setup & Dependencies**
    - [x] Create `DSS_BONDS_ENHANCEMENT.md` [DONE]
    - [x] Add `opencsv` dependency to `pom.xml` [DONE]

- [x] **Task 2: Database Schema Implementation**
    - [x] Create `src/main/resources/db/changelog/sql/006-lseg_dss_bonds.sql` [DONE]
    - [x] Register SQL script in `src/main/resources/db/changelog/db.changelog-master.xml` [DONE]

- [x] **Task 3: Core Model & Classification**
    - [x] Add `DSS_BONDS` to `Target.java` [DONE]
    - [x] Define column mappings in `TargetSchema.java` [DONE]
    - [x] Update `FileScanner.java` to support `SG_HK_Bonds*.csv` classification [DONE]

- [x] **Task 4: Parser Implementation**
    - [x] Implement `CsvFileParser.java` using OpenCSV [DONE]
    - [x] Synthesize `Action` column as 'I' for compatibility [DONE]

- [x] **Task 5: Ingestor & Sanity Check Adaptation**
    - [x] Update `FileIngestor.java` to support CSV files [DONE]
    - [x] Update `FileSanityCheck.java` to support CSV files [DONE]

- [x] **Task 6: Testing & Verification**
    - [x] Enhance unit tests (`mvn test`) [DONE]
    - [x] Add integration tests for DSS Bonds [DONE]
    - [x] Verification 1: Cleanup + Smoke Test [DONE]
    - [x] Verification 2: Cleanup + Full End-to-End Test (All files) [DONE]

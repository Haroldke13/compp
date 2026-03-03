-- Auto-create indexes for waiver uploaded_data_* tables in PostgreSQL.
-- Targets sheets from file: "Waiver summary, 2025.xlsx"
-- Safe to run multiple times.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Helpful metadata indexes
CREATE INDEX IF NOT EXISTS idx_uploaded_files_original_filename
    ON uploaded_files(original_filename);
CREATE INDEX IF NOT EXISTS idx_uploaded_sheets_file_id_sheet_name
    ON uploaded_sheets(file_id, sheet_name);

DO $$
DECLARE
    rec RECORD;
    v_table text;
BEGIN
    FOR rec IN
        SELECT s.table_name
        FROM uploaded_sheets s
        JOIN uploaded_files f ON f.id = s.file_id
        WHERE f.original_filename = 'Waiver summary, 2025.xlsx'
          AND s.table_name LIKE 'uploaded_data\_%' ESCAPE '\'
        ORDER BY s.table_name
    LOOP
        v_table := rec.table_name;

        -- Always useful for paging/sorting
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = v_table
              AND column_name = 'excel_row_number'
        ) THEN
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I (%I)',
                'idx_' || v_table || '_excel_row_number',
                v_table,
                'excel_row_number'
            );
        END IF;

        -- B-tree indexes for exact/range filters
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = v_table AND column_name = 'status'
        ) THEN
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I (%I)',
                'idx_' || v_table || '_status',
                v_table,
                'status'
            );
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = v_table AND column_name = 'balance'
        ) THEN
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I (%I)',
                'idx_' || v_table || '_balance',
                v_table,
                'balance'
            );
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = v_table AND column_name = 'total_penalty'
        ) THEN
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I (%I)',
                'idx_' || v_table || '_total_penalty',
                v_table,
                'total_penalty'
            );
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = v_table AND column_name = 'penalty_paid'
        ) THEN
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I (%I)',
                'idx_' || v_table || '_penalty_paid',
                v_table,
                'penalty_paid'
            );
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = v_table AND column_name = 'granted_waiver_amount'
        ) THEN
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I (%I)',
                'idx_' || v_table || '_granted_waiver_amount',
                v_table,
                'granted_waiver_amount'
            );
        END IF;

        -- Trigram indexes for frequent text search fields
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = v_table AND column_name = 'name_of_the_ngo'
        ) THEN
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I USING GIN (LOWER(%I) gin_trgm_ops)',
                'idx_' || v_table || '_name_of_the_ngo_trgm',
                v_table,
                'name_of_the_ngo'
            );
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = v_table AND column_name = 'committee_s_comments'
        ) THEN
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I USING GIN (LOWER(%I) gin_trgm_ops)',
                'idx_' || v_table || '_committee_s_comments_trgm',
                v_table,
                'committee_s_comments'
            );
        END IF;
    END LOOP;
END $$;

COMMIT;


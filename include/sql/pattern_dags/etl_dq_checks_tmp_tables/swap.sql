DO
$$
BEGIN
    IF EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_name = '{{ params.perm_table }}' 
        AND table_schema = '{{ params.schema }}'
    ) 
    THEN
        IF EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_name = '{{ params.perm_table }}_backup' 
            AND table_schema = '{{ params.schema }}'
        ) 
        THEN
            EXECUTE 'DROP TABLE ' || quote_ident('{{ params.schema }}') || '.' || quote_ident('{{ params.perm_table }}') || '_backup';
        END IF;

        EXECUTE 'ALTER TABLE ' || quote_ident('{{ params.schema }}') || '.' || quote_ident('{{ params.perm_table }}') || 
        ' RENAME TO ' || quote_ident('{{ params.perm_table }}') || '_backup';
    END IF;
    
    EXECUTE 'ALTER TABLE ' || quote_ident('{{ params.schema }}') || '.' || quote_ident('{{ params.tmp_table }}') || 
    ' RENAME TO ' || quote_ident('{{ params.perm_table }}');
END
$$;

SELECT
    COUNT(*) - COUNT(DISTINCT id) AS duplicate_row_count
FROM
    synthdata;

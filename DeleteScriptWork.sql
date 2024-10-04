DO $$
DECLARE
	cur_year smallint := (EXTRACT (YEAR FROM CURRENT_DATE));
	partition_tName text;
	partition_t text;
BEGIN
	FOR year in 2019..cur_year LOOP
		partition_t := format('"EquipmentInfo_%s"', year);
		partition_tName := substring(partition_t from 2 for char_length(partition_t) - 2);

		-- Создаём партицию
		EXECUTE format('	WITH duplicates AS (
	    SELECT ctid
	    FROM (
	        SELECT ctid,
	               "svidetelstvoNumber",
	               "vri_id",
	               ROW_NUMBER() OVER (PARTITION BY "svidetelstvoNumber" ORDER BY "vri_id" IS NOT NULL DESC) AS rnum
	        FROM %s
	    ) subquery
	    WHERE rnum > 1 AND "vri_id" IS NULL
	)
	DELETE FROM %s
	WHERE ctid IN (SELECT ctid FROM duplicates);', partition_t, partition_t);
	END LOOP;
END $$;
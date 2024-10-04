

-- INSERT INTO "TypeStatistics" ("type", "count_", "year")
-- SELECT "UniqueTypes"."type", count(*), "EquipmentInfoPartitioned"."year" FROM "UniqueTypes"
-- JOIN "EquipmentInfoPartitioned" ON "UniqueTypes"."id" = "EquipmentInfoPartitioned"."typeId"
-- GROUP BY "UniqueTypes"."type", "EquipmentInfoPartitioned"."year";

DO $$
DECLARE
	partition_t text;
    partition_tName text;
	cur_year smallint := (EXTRACT (YEAR FROM CURRENT_DATE));
	tables_ text[] := ARRAY['"TypeStatistics"', '"TypeNameStatistics"', '"RegisterNumberStatistics"', '"ModificationStatistics"'];
    uniqueTables text[] := ARRAY['"UniqueTypes"', '"UniqueTypeNames"', '"UniqueRegisterNumbers"', '"UniqueModifications"'];
	valCols_ text[] := ARRAY['"type"', '"typeName"', '"registerNumber"', '"modification"'];
    idCols_ text[] := ARRAY['"typeId"', '"typeNameId"', '"registerNumberId"', '"modificationId"'];
    tb text;
    tbName text;
    vc text;
    ut text;
    id_ text;
BEGIN
FOR i IN 1..4 LOOP
	tb = tables_[i];
    tbName = substring(tb from 2 for char_length(tb) - 2);
	vc = valCols_[i];
    ut = uniqueTables[i];
    id_ = idCols_[i];
	EXECUTE format('CREATE TABLE IF NOT EXISTS %s
	(
	    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
	    %s text,
		"count_" integer,
	    year smallint NOT NULL,
	    CONSTRAINT "%s_pkey_id_year" PRIMARY KEY (id, year)
	) PARTITION BY RANGE (year)', tb, vc, tbName);

	-- Создаем партиции с индексами 
	FOR year in 2019..cur_year LOOP
		partition_t := format('"%s_%s"', tbName, year);
		-- Создаём партицию
		EXECUTE format('CREATE TABLE IF NOT EXISTS %s PARTITION OF %s
			FOR VALUES FROM (%s) TO (%s)', partition_t, tb, year, year + 1);
	END LOOP;

    -- EXECUTE format('INSERT INTO %s (%s, "count_", "year")
    -- SELECT %s.%s, count(*), "EquipmentInfoPartitioned"."year" FROM %s
    -- JOIN "EquipmentInfoPartitioned" ON %s."id" = "EquipmentInfoPartitioned".%s
    -- GROUP BY %s.%s, "EquipmentInfoPartitioned"."year"', tb, vc, ut, vc, ut, ut, id_, ut, vc);
END LOOP;
END $$;
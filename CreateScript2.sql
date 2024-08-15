-- WITH duplicates AS (
--     SELECT ctid
--     FROM (
--         SELECT ctid,
--                "svidetelstvoNumber",
--                "vri_id",
--                ROW_NUMBER() OVER (PARTITION BY "svidetelstvoNumber" ORDER BY "vri_id" IS NOT NULL DESC) AS rnum
--         FROM "EquipmentInfoPartitioned"
--     ) subquery
--     WHERE rnum > 1 AND "vri_id" IS NULL
-- )
-- DELETE FROM "EquipmentInfoPartitioned"
-- WHERE ctid IN (SELECT ctid FROM duplicates);

-- WITH duplicates AS (
--     SELECT ctid
--     FROM (
--         SELECT ctid,
--                ROW_NUMBER() OVER (PARTITION BY "svidetelstvoNumber" ORDER BY ctid) AS rnum
--         FROM "EquipmentInfoPartitioned"
--     ) subquery
--     WHERE rnum > 1
-- )
-- DELETE FROM "EquipmentInfoPartitioned"
-- WHERE ctid IN (SELECT ctid FROM duplicates);


DO $$
DECLARE
	tables_ text[] := ARRAY['"UniquePoveritelOrgs"', '"UniqueTypeNames"', '"UniqueRegisterNumbers"', '"UniqueTypes"', '"UniqueModifications"'];
	valCols_ text[] := ARRAY['"poveritelOrg"', '"typeName"', '"registerNumber"', '"type"', '"modification"'];
	idCols_ text[] := ARRAY['"poveritelOrgId"', '"typeNameId"', '"registerNumberId"', '"typeId"', '"modificationId"'];
    tsvectorsCols_ text[] := ARRAY['"registerNumber"', '"typeName"', '"type"', '"modification"'];
	tb text;
	vc text;
	ic text;
	tName text;
	vcName text;
    icName text;
	partition_tName text;
	partition_t text;
	cur_year smallint := (EXTRACT (YEAR FROM CURRENT_DATE));
BEGIN
FOR i IN 1..5 LOOP
	tb = tables_[i];
	vc = valCols_[i];
	ic = idCols_[i];
	-- Убираем двойные кавычки из имени таблицы
	tName = substring(tb from 2 for char_length(tb) - 2);
	vcName = substring(vc from 2 for char_length(vc) - 2);
    icName = substring(ic from 2 for char_length(ic) - 2);

    IF vc = ANY(tsvectorsCols_) THEN
        -- Заполяем столбец _tsvector !!!!!!!!!!!!!!!!!!!!!!!
        --EXECUTE format('UPDATE %s SET "%s_tsvector" = to_tsvector(''russian'', %s)', tb, vcName, vc);
        -- Создаём для них GIN индексы
        -- EXECUTE format('CREATE INDEX IF NOT EXISTS "%s_%s_gin_idx"
        --     ON %s USING gin ("%s_tsvector")', tName, vcName, tb, vcName);
        -- Создаем gin индексы для поиска по триграммам
        EXECUTE format('CREATE INDEX IF NOT EXISTS "%s_%s_gin_trgm_idx"
        ON %s USING gin ("%s" gin_trgm_ops)', tName, vcName, tb, vcName);
    END IF;

    -- Создаём для них HASH индексы !!!
	-- EXECUTE format('CREATE INDEX IF NOT EXISTS "%s_%s_hash_idx"
	-- 	ON %s USING hash (%s)', tName, vcName, tb, vc);

    -- Добавляем для уникальной таблицы ограничение уникальности
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT "uniqueConstraint%s" UNIQUE (%s)', tb, tName, vc);

    IF i = 1 THEN
        -- Создаем партиции с индексами 
        FOR year in 2019..cur_year LOOP
            partition_t := format('"EquipmentInfo_%s"', year);
            partition_tName := substring(partition_t from 2 for char_length(partition_t) - 2);

            -- Создаём для партиции хэш индекс для свидетельства о поверке !!!
            --EXECUTE format('CREATE INDEX IF NOT EXISTS "%s_svidetelstvoNumber_hash_idx" ON %s USING hash ("svidetelstvoNumber")', partition_tName, partition_t);
        END LOOP;
    END IF;

    -- Создаём внешний ключ
    EXECUTE format('ALTER TABLE "EquipmentInfoPartitioned" ADD CONSTRAINT "EquipmentInfoPartitioned_%s_fkey" FOREIGN KEY ("%s")
            REFERENCES %s (id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE CASCADE', icName, icName, tb);

    -- Создаём индекс на внешний ключ
    EXECUTE format('CREATE INDEX IF NOT EXISTS "EquipmentInfoPartitioned_%s_fkey_idx"
        ON "EquipmentInfoPartitioned" USING hash (%s)', icName, ic);

END LOOP;
END $$;
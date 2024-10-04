create extension pg_trgm;

-- Создаем партицированную таблицу
CREATE TABLE IF NOT EXISTS public."EquipmentInfoPartitioned"
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    "serialNumber" TEXT,
    "poverkaDate" date,
    "konecDate" date,
    "svidetelstvoNumber" TEXT,
    "isPrigodno" boolean,
    vri_id bigint,
    "typeNameId" integer,
    "poveritelOrgId" integer,
    "registerNumberId" integer,
    "typeId" integer,
    "modificationId" integer,
    year smallint NOT NULL,
    CONSTRAINT "EquipmentInfoPartitioned_pkey_id_year" PRIMARY KEY (id, year)
) PARTITION BY RANGE (year);

-- Создаём таблицу для хранения названий скачанных файлов
CREATE TABLE IF NOT EXISTS public."DownloadedFiles"
(
    "fileId" bigint,
    "fileName" character varying(256) COLLATE pg_catalog."default",
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    CONSTRAINT "DownloadedFiles_pkey" PRIMARY KEY (id)
);


DO $$
DECLARE
	tables_ text[] := ARRAY['"UniquePoveritelOrgs"', '"UniqueTypeNames"', '"UniqueRegisterNumbers"', '"UniqueTypes"', '"UniqueModifications"'];
	valCols_ text[] := ARRAY['"poveritelOrg"', '"typeName"', '"registerNumber"', '"type"', '"modification"'];
	idCols_ text[] := ARRAY['"poveritelOrgId"', '"typeNameId"', '"registerNumberId"', '"typeId"', '"modificationId"'];
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

    EXECUTE format('CREATE TABLE IF NOT EXISTS %s (
        id integer NOT NULL GENERATED ALWAYS AS IDENTITY, PRIMARY KEY (id),
        %s TEXT NOT NULL)', tb, vc);
    IF i = 1 THEN
        -- Создаем партиции с индексами 
        FOR year in 2019..cur_year LOOP
            partition_t := format('"EquipmentInfo_%s"', year);
            partition_tName := substring(partition_t from 2 for char_length(partition_t) - 2);

            -- Создаём партицию
            EXECUTE format('CREATE TABLE IF NOT EXISTS %s PARTITION OF "EquipmentInfoPartitioned"
                FOR VALUES FROM (%s) TO (%s)', partition_t, year, year + 1);
        END LOOP;
    END IF;
END LOOP;
END $$;
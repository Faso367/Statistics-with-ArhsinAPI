WITH duplicates AS (
    SELECT ctid
    FROM (
        SELECT ctid,
               "svidetelstvoNumber",
               "vri_id",
               ROW_NUMBER() OVER (PARTITION BY "svidetelstvoNumber" ORDER BY "vri_id" IS NOT NULL DESC) AS rnum
        FROM "EquipmentInfoPartitioned"
    ) subquery
    WHERE rnum > 1 AND "vri_id" IS NULL
)
DELETE FROM "EquipmentInfoPartitioned"
WHERE ctid IN (SELECT ctid FROM duplicates);

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



-- Этот код на PostgreSQL выполняет удаление дубликатов из таблицы "EquipmentInfoPartitioned", основываясь на колонках "svidetelstvoNumber" и "vri_id", используя вспомогательную конструкцию CTE (Common Table Expression). Давайте разберем его по шагам:

-- 1. WITH duplicates AS (...)
-- CTE duplicates создается для того, чтобы временно сохранить список записей, которые считаются дубликатами. Эти записи будут удалены в последующей части запроса.
-- 2. Подзапрос с выборкой дубликатов
-- sql
-- Копировать код
-- SELECT ctid,
--        "svidetelstvoNumber",
--        "vri_id",
--        ROW_NUMBER() OVER (PARTITION BY "svidetelstvoNumber" ORDER BY "vri_id" IS NOT NULL DESC) AS rnum
-- FROM "EquipmentInfoPartitioned"
-- ctid — это специальная внутренняя системная колонка в PostgreSQL, которая содержит идентификатор физического места записи в таблице. Он используется здесь для удаления конкретных строк.
-- "svidetelstvoNumber" и "vri_id" — это колонки, по которым выполняется выборка.
-- ROW_NUMBER() OVER (PARTITION BY ...):
-- PARTITION BY "svidetelstvoNumber": Для каждой уникальной комбинации "svidetelstvoNumber" будет создана группа записей.
-- ORDER BY "vri_id" IS NOT NULL DESC: Внутри каждой группы строки сортируются так, чтобы записи с ненулевым (непустым) значением "vri_id" шли выше пустых (NULL), используя сортировку в порядке убывания.
-- ROW_NUMBER() присваивает каждой строке в группе порядковый номер. Строка с первым номером (rnum = 1) будет считаться "основной" записью, а строки с rnum > 1 — потенциальными дубликатами.
-- 3. Фильтрация строк-дубликатов
-- sql
-- Копировать код
-- WHERE rnum > 1 AND "vri_id" IS NULL
-- Эта часть запроса оставляет только те строки, которые:
-- Имеют порядковый номер больше 1 (rnum > 1), то есть все кроме первой строки в каждой группе.
-- При этом значение "vri_id" должно быть NULL, что, вероятно, указывает на менее значимые записи в группе.
-- 4. Удаление дубликатов
-- sql
-- Копировать код
-- DELETE FROM "EquipmentInfoPartitioned"
-- WHERE ctid IN (SELECT ctid FROM duplicates);
-- Удаляются строки из таблицы "EquipmentInfoPartitioned", идентификаторы которых (ctid) находятся в результатах подзапроса duplicates.
-- Итог
-- Этот запрос ищет дубликаты записей по колонке "svidetelstvoNumber", оставляя первую строку в каждой группе, а остальные строки с NULL в "vri_id" и с порядковым номером больше 1 — удаляются.

-- Ключевые моменты:

-- Сначала строки группируются по "svidetelstvoNumber".
-- Строки сортируются так, что записи с ненулевым "vri_id" идут выше.
-- Удаляются строки-дубликаты с NULL в "vri_id", оставляя только одну уникальную строку с ненулевым "vri_id", если она есть.
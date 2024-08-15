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

WITH duplicates AS (
    SELECT ctid
    FROM (
        SELECT ctid,
               ROW_NUMBER() OVER (PARTITION BY "svidetelstvoNumber" ORDER BY ctid) AS rnum
        FROM "EquipmentInfoPartitioned"
    ) subquery
    WHERE rnum > 1
)
DELETE FROM "EquipmentInfoPartitioned"
WHERE ctid IN (SELECT ctid FROM duplicates);

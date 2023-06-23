CREATE OR REPLACE PROCEDURE dimadev.iter_dbkey_drop(
	IN _schema_name text,
	IN _table_name text,
	IN _dbkey_substr text,
  IN _date_loaded text)
LANGUAGE 'plpgsql'
AS $BODY$
-- DECLARE
BEGIN
	  	EXECUTE FORMAT('
		SELECT * FROM %1$s."%2$s"
		WHERE "DBKey"
    LIKE $1
    AND
    "DateLoadedInDB" >= $2;
		', _schema_name,
       _table_name,
       _dbkey_substr,
       _date_loaded
     );
END
$BODY$;

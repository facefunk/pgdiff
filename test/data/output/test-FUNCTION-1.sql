CREATE OR REPLACE FUNCTION s2.add(integer, integer)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$select $1 + $2;$function$
;
CREATE OR REPLACE FUNCTION s2.increment(i integer)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
    BEGIN
        RETURN i + 1;
    END;
$function$
;
DROP FUNCTION s2.minus CASCADE;

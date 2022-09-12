CREATE OR REPLACE FUNCTION s1.add(integer, integer)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$select $1 + $2;$function$
;
DROP FUNCTION s1.addition CASCADE;
CREATE OR REPLACE FUNCTION s2.add(bigint, bigint)
 RETURNS bigint
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$select $1 + $2;$function$
;

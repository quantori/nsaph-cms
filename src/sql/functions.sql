--  Copyright (c) 2022. Harvard University
--
--  Developed by Research Software Engineering,
--  Faculty of Arts and Sciences, Research Computing (FAS RC)
--  Author: Michael A Bouzinier
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--         http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
--


CREATE OR REPLACE FUNCTION public.parse_date (
    dstr character varying
)  RETURNS DATE
  VOLATILE
AS $body$
DECLARE ystr varchar; mstr varchar; daystr varchar; s varchar;
BEGIN
    IF LENGTH(dstr) = 8 THEN
        RETURN to_date(dstr, 'YYYYMMDD');
    ELSEIF LENGTH(dstr) > 8 THEN
        raise EXCEPTION 'Invalid date, len > 8 %', dstr;
    ELSEIF LENGTH(dstr) < 6 THEN
        raise EXCEPTION 'Invalid date, len < 6 %', dstr;
    ELSE
        ystr := SUBSTRING(dstr, 1, 4);
        s := SUBSTRING(dstr, 5);
        IF LENGTH(s) = 2 THEN
            mstr := '0' || SUBSTRING(s, 1, 1);
            daystr := '0' || SUBSTRING(s, 2, 1);
        ELSE -- = 3
            IF SUBSTRING(s, 1,1) = '0' OR (SUBSTRING(s, 1, 2)::INT < 13) THEN
                mstr := SUBSTRING(s, 1, 2);
                daystr := '0' || SUBSTRING(s, 3, 1);
            ELSE
                mstr := '0' || SUBSTRING(s, 1, 1);
                daystr := SUBSTRING(s, 2, 2);
            END IF;
        END IF;
        RETURN to_date(ystr || mstr || daystr, 'YYYYMMDD');
    END IF;
END;
$body$ LANGUAGE plpgsql
;

CREATE OR REPLACE FUNCTION public.parse_date (
    istr numeric
)  RETURNS DATE
  VOLATILE
AS $body$
BEGIN
    RETURN public.parse_date(TRIM(to_char(istr, '999999999')));
END;
$body$ LANGUAGE plpgsql


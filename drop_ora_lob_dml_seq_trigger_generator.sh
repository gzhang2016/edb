#!/bin/bash
dr="/opt/PostgresSQL/xdb_scripts/lob_sync_scripts/ora2edb"
psql ntp -t -c "select 'drop sequence XDB_ADMIN.'||sequence_name||' ;'  from dba_sequences@gzoci where SEQUENCE_OWNER='XDB_ADMIN' AND SEQUENCE_NAME LIKE 'LOB%' order by sequence_name;" > $dr/drop_ora_lob_dml_seq_trigger.sql

psql ntp -t -c  "select ' drop table  XDB_ADMIN.'|| table_NAME||' ;' FROM dba_tables@gzoci WHERE OWNER='XDB_ADMIN' AND TABLE_NAME LIKE 'LOB%' ORDER BY TABLE_NAME;" >> $dr/drop_ora_lob_dml_seq_trigger.sql


psql ntp -t -c "select ' drop trigger XDB_ADMIN.'|| TRIGGER_NAME||' ;' FROM dba_triggers@gzoci WHERE OWNER='XDB_ADMIN' AND TRIGGER_NAME LIKE 'LOB%' ORDER BY TRIGGER_NAME;" >> $dr/drop_ora_lob_dml_seq_trigger.sql

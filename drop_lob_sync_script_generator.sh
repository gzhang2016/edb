#!/bin/bash

# get lob table names
tblist=`cat /opt/PostgresSQL/xdb_scripts/lob_sync_scripts/lob_table_sync_list.txt`
#tblist="GZ_TEST"
dr="/opt/PostgresSQL/xdb_scripts/lob_sync_scripts/edb2ora"
edb_lob_sync_control_schema='LOB_SYNC_CTL'
ora_user='P42'
edb_schema='P42'

echo " " > $dr/drop_lobdml_status_edb_tables.sql
echo " " > $dr/drop_lobdml_edb_seq.sql
echo " " > $dr/drop_lobdml_edb_trg.sql
echo " " > $dr/drop_lob_edb2ora_sync_function.sql

for tb in $tblist
   do
     echo " DROP TABLE  $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_STATUS;" >> $dr/drop_lobdml_status_edb_tables.sql
     echo " DROP SEQUENCE  $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_SEQ;" >> $dr/drop_lobdml_edb_seq.sql
     echo " DROP TRIGGER   EDB_LOBDML_${tb}_TRG;" >> $dr/drop_lobdml_edb_trg.sql
     echo " DROP FUNCTION  $edb_lob_sync_control_schema.LOB_SYNC_EDB2ORA_${tb};" >> $dr/drop_lob_edb2ora_sync_function.sql

  done


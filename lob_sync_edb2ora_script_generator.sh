#!/bin/bash
dr="/opt/PostgresSQL/xdb_scripts/lob_sync_scripts/ora2edb"
psql ntp -t -c "select 'drop sequence XDB_ADMIN.'||sequence_name||' ;'  from dba_sequences@gzoci where SEQUENCE_OWNER='XDB_ADMIN' AND SEQUENCE_NAME LIKE 'LOB%' order by sequence_name;" > $dr/drop_ora_lob_dml_seq_trigger.sql

psql ntp -t -c  "select ' drop table  XDB_ADMIN.'|| table_NAME||' ;' FROM dba_tables@gzoci WHERE OWNER='XDB_ADMIN' AND TABLE_NAME LIKE 'LOB%' ORDER BY TABLE_NAME;" >> $dr/drop_ora_lob_dml_seq_trigger.sql


psql ntp -t -c "select ' drop trigger XDB_ADMIN.'|| TRIGGER_NAME||' ;' FROM dba_triggers@gzoci WHERE OWNER='XDB_ADMIN' AND TRIGGER_NAME LIKE 'LOB%' ORDER BY TRIGGER_NAME;" >> $dr/drop_ora_lob_dml_seq_trigger.sql



/opt/PostgresSQL/xdb_scripts/lob_sync_scripts/ora2edb: $ cd ../edb2ora/
/opt/PostgresSQL/xdb_scripts/lob_sync_scripts/edb2ora: $ ls -l
total 184
-rw-rw-r--. 1 edb edb   3615 Apr 24 19:14 create_lobdml_seq_edb.sql
-rw-rw-r--. 1 edb edb   5835 Apr 24 19:14 create_lobdml_status_tables_edb.sql
-rw-rw-r--. 1 edb edb  27115 Apr 24 19:14 create_lobdml_trg_edb.sql
-rw-rw-r--. 1 edb edb 117895 Apr 24 19:14 create_lob_edb2ora_sync_function.sql
-rw-rw-r--. 1 edb edb   1995 Apr 24 19:13 drop_lobdml_edb_seq.sql
-rw-rw-r--. 1 edb edb   1605 Apr 24 19:13 drop_lobdml_edb_trg.sql
-rw-rw-r--. 1 edb edb   1995 Apr 24 19:13 drop_lobdml_status_edb_tables.sql
-rw-rw-r--. 1 edb edb   2055 Apr 24 19:13 drop_lob_edb2ora_sync_function.sql
-rwx------. 1 edb edb    920 Apr 24 19:13 drop_lob_sync_script_generator.sh
-rwx------. 1 edb edb   7954 Apr 24 19:14 lob_sync_edb2ora_script_generator.sh
/opt/PostgresSQL/xdb_scripts/lob_sync_scripts/edb2ora: $ cat lob_sync_edb2ora_script_generator.sh
#!/bin/bash

# get lob table names
tblist=`cat /opt/PostgresSQL/xdb_scripts/lob_sync_scripts/lob_table_sync_list.txt`
#tblist="GZTEST_CLOB_LONG_TABLE_NAME"
dr="/opt/PostgresSQL/xdb_scripts/lob_sync_scripts/edb2ora"
edb_lob_sync_control_schema='LOB_SYNC_CTL'
ora_user='P42'
edb_schema='P42'
edblink='GZOCI'

psql ntp -c " create schema $edb_lob_sync_control_schema;"

echo " " > $dr/create_lobdml_status_tables_edb.sql
echo " " > $dr/create_lobdml_seq_edb.sql
echo " " > $dr/create_lobdml_trg_edb.sql
echo " " > $dr/create_lob_edb2ora_sync_function.sql

for tb in $tblist
   do
     echo "CREATE TABLE $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_STATUS ( DML_ID NUMBER PRIMARY KEY , PKEY INT, DML_TYPE VARCHAR(40), DML_TIME TIMESTAMP, SYNC_STATUS VARCHAR(40),SYNC_TIME TIMESTAMP );" >> $dr/create_lobdml_status_tables_edb.sql
    echo " CREATE SEQUENCE  $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_SEQ  MINVALUE 1  START WITH 1  INCREMENT BY 1  CACHE 20;" >> $dr/create_lobdml_seq_edb.sql

#create lob table trigger in edb. CANNOT  use schema "$edb_lob_sync_control_schema" in front of trigger name in edb

    echo "CREATE OR REPLACE TRIGGER EDB_LOBDML_${tb}_TRG" >> $dr/create_lobdml_trg_edb.sql
    echo " BEFORE  INSERT OR  UPDATE OR DELETE  ON   $edb_schema.${tb}" >> $dr/create_lobdml_trg_edb.sql
    echo " FOR EACH ROW" >> $dr/create_lobdml_trg_edb.sql
 echo "DECLARE">> $dr/create_lobdml_trg_edb.sql
 echo "BEGIN">> $dr/create_lobdml_trg_edb.sql
 echo "    IF INSERTING THEN">> $dr/create_lobdml_trg_edb.sql
 echo "     insert into      $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_STATUS    VALUES ($edb_lob_sync_control_schema.EDB_LOBDML_${tb}_SEQ.NEXTVAL,:NEW.${tb}_ID,'insert',CURRENT_TIMESTAMP,'no',NULL);">> $dr/create_lobdml_trg_edb.sql
 echo "    END IF;">> $dr/create_lobdml_trg_edb.sql
 echo "    IF  UPDATING THEN">> $dr/create_lobdml_trg_edb.sql
 echo "    insert into       $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_STATUS    VALUES ($edb_lob_sync_control_schema.EDB_LOBDML_${tb}_SEQ.NEXTVAL,:OLD.${tb}_ID,'update',CURRENT_TIMESTAMP,'no',NULL);">> $dr/create_lobdml_trg_edb.sql
 echo "    END IF;">> $dr/create_lobdml_trg_edb.sql
 echo "    IF DELETING THEN">> $dr/create_lobdml_trg_edb.sql
 echo "    insert into       $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_STATUS    VALUES ($edb_lob_sync_control_schema.EDB_LOBDML_${tb}_SEQ.NEXTVAL,:OLD.${tb}_ID,'delete',CURRENT_TIMESTAMP,'no',NULL);">> $dr/create_lobdml_trg_edb.sql
 echo "   END IF;">> $dr/create_lobdml_trg_edb.sql
 echo "END ;" >> $dr/create_lobdml_trg_edb.sql
 echo "/">> $dr/create_lobdml_trg_edb.sql
 echo "  ">> $dr/create_lobdml_trg_edb.sql

#generate sync function in edb

echo "CREATE OR REPLACE FUNCTION $edb_lob_sync_control_schema.LOB_SYNC_EDB2ORA_${tb}() RETURNS void AS ">>$dr/create_lob_edb2ora_sync_function.sql
echo "\$BODY\$">>$dr/create_lob_edb2ora_sync_function.sql
echo "DECLARE ">>$dr/create_lob_edb2ora_sync_function.sql
echo " cur_dmls  CURSOR FOR SELECT * FROM $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_STATUS WHERE sync_status = 'no' order by dml_id;">>$dr/create_lob_edb2ora_sync_function.sql
echo " rec_dml   RECORD;">>$dr/create_lob_edb2ora_sync_function.sql
echo " BEGIN">>$dr/create_lob_edb2ora_sync_function.sql
echo "   OPEN cur_dmls;">>$dr/create_lob_edb2ora_sync_function.sql
echo "    LOOP">>$dr/create_lob_edb2ora_sync_function.sql
echo "     FETCH cur_dmls INTO rec_dml;">>$dr/create_lob_edb2ora_sync_function.sql
echo "    -- exit when no more row to fetch">>$dr/create_lob_edb2ora_sync_function.sql
echo "      EXIT WHEN NOT FOUND;">>$dr/create_lob_edb2ora_sync_function.sql
echo "     IF  rec_dml.dml_type = 'insert' THEN ">>$dr/create_lob_edb2ora_sync_function.sql
echo "          IF ( select count(*) from $ora_user.${tb}@$edblink where ${tb}_ID = rec_dml.pkey ) = 0 THEN">>$dr/create_lob_edb2ora_sync_function.sql
echo "              insert into $ora_user.${tb}@$edblink select * from $edb_schema.${tb} where ${tb}_ID = rec_dml.pkey;">>$dr/create_lob_edb2ora_sync_function.sql
echo "              update $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_STATUS set sync_status = 'yes',sync_time=CURRENT_TIMESTAMP where pkey = rec_dml.pkey and  dml_type=rec_dml.dml_type;">>$dr/create_lob_edb2ora_sync_function.sql
echo "          ELSE  ">>$dr/create_lob_edb2ora_sync_function.sql
echo "           update $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_STATUS set sync_status = 'dupkey'  ">>$dr/create_lob_edb2ora_sync_function.sql
echo "           where pkey = rec_dml.pkey and dml_type=rec_dml.dml_type;">>$dr/create_lob_edb2ora_sync_function.sql
echo "          END IF;">>$dr/create_lob_edb2ora_sync_function.sql
echo "      END IF;    ">>$dr/create_lob_edb2ora_sync_function.sql
echo "      IF  rec_dml.dml_type = 'delete' THEN ">>$dr/create_lob_edb2ora_sync_function.sql
echo "          IF ( select count(*) from  $ora_user.${tb}@$edblink where ${tb}_ID = rec_dml.pkey  ) = 1 THEN">>$dr/create_lob_edb2ora_sync_function.sql
echo "              delete from $ora_user.${tb}@$edblink  where ${tb}_ID = rec_dml.pkey;">>$dr/create_lob_edb2ora_sync_function.sql
echo "              update $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_STATUS set sync_status = 'yes',sync_time=CURRENT_TIMESTAMP where pkey = rec_dml.pkey and  dml_type=rec_dml.dml_type;">>$dr/create_lob_edb2ora_sync_function.sql
echo "          ELSE  ">>$dr/create_lob_edb2ora_sync_function.sql
echo "             update $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_STATUS set sync_status = 'notExist' ">>$dr/create_lob_edb2ora_sync_function.sql
echo "             where pkey = rec_dml.pkey and dml_type=rec_dml.dml_type;">>$dr/create_lob_edb2ora_sync_function.sql
echo "          END IF;">>$dr/create_lob_edb2ora_sync_function.sql
echo "      END IF;">>$dr/create_lob_edb2ora_sync_function.sql
echo "      IF  rec_dml.dml_type = 'update' THEN ">>$dr/create_lob_edb2ora_sync_function.sql
echo "        update  $ora_user.${tb}@$edblink  set ">>$dr/create_lob_edb2ora_sync_function.sql

# get lob table column names

# col_list=(`psql -q ntp -c "select COLUMN_NAME from DBA_TAB_COLUMNS@$edblink where owner='$ora_user' and table_name ='${tb}' and COLUMN_NAME not in ('${tb}_ID') ;"|tail -n +3| head -n -2`)
  col_list=(`psql -q -t  ntp -c "select COLUMN_NAME from DBA_TAB_COLUMNS@$edblink where owner='$ora_user' and table_name ='${tb}' and COLUMN_NAME not in ('${tb}_ID') ;"`)
# for loop to set lob table column to new value in edb

                  for col_name in "${col_list[@]}"
                    do
                        if [[ $col_name = ${col_list[-1]} ]]
                          then
                          echo "             $col_name = (select $col_name from  $edb_schema.${tb} where ${tb}_ID = rec_dml.pkey)">>$dr/create_lob_edb2ora_sync_function.sql
                        else
                          echo "             $col_name = (select $col_name from  $edb_schema.${tb} where ${tb}_ID = rec_dml.pkey),">>$dr/create_lob_edb2ora_sync_function.sql
                        fi
                   done
echo "        where  ${tb}_ID = rec_dml.pkey ;">>$dr/create_lob_edb2ora_sync_function.sql
echo "">>$dr/create_lob_edb2ora_sync_function.sql
echo "        update $edb_lob_sync_control_schema.EDB_LOBDML_${tb}_STATUS set sync_status = 'yes',sync_time=CURRENT_TIMESTAMP ">>$dr/create_lob_edb2ora_sync_function.sql
echo "         where pkey = rec_dml.pkey and dml_type=rec_dml.dml_type;">>$dr/create_lob_edb2ora_sync_function.sql
echo "      END IF;">>$dr/create_lob_edb2ora_sync_function.sql
echo "   END LOOP;">>$dr/create_lob_edb2ora_sync_function.sql
echo "   CLOSE cur_dmls;">>$dr/create_lob_edb2ora_sync_function.sql
echo "END; ">>$dr/create_lob_edb2ora_sync_function.sql
echo " \$BODY\$">>$dr/create_lob_edb2ora_sync_function.sql
echo "LANGUAGE plpgsql;">>$dr/create_lob_edb2ora_sync_function.sql
echo "  ">>$dr/create_lob_edb2ora_sync_function.sql
echo "  ">>$dr/create_lob_edb2ora_sync_function.sql

  done


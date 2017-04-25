#!/bin/bash

# get lob table names
tblist=`cat /opt/PostgresSQL/xdb_scripts/lob_sync_scripts/lob_table_sync_list.txt`
#tblist="GZ_TEST"
dr="/opt/PostgresSQL/xdb_scripts/lob_sync_scripts/ora2edb"
ora_control_schema='XDB_ADMIN'
ora_user='P42'
edb_schema='P42'
edblink='GZOCI'


echo " " > $dr/create_lobdml_status_ora_tables.sql
echo " " > $dr/create_lobdml_ora_seq.sql
echo " " > $dr/create_lobdml_ora_trg.sql
echo " " > $dr/create_lob_ora2edb_sync_function.sql

for tb in $tblist
   do
        st_l=${#tb}
        echo "table $tb length:$st_l"
         if (( $st_l > "21" ))
           then
            echo "table $tb length > 21 charater!"
            short_tb=${tb:0:20}
            echo " just use first 21  character for table $tb: $short_tb"
            echo "CREATE TABLE $ora_control_schema.LOB_${short_tb}_DML ( DML_ID NUMBER PRIMARY KEY , PKEY INT, DML_TYPE VARCHAR(40), DML_TIME TIMESTAMP, SYNC_STATUS VARCHAR(40),SYNC_TIME  TIMESTAMP );" >> $dr/create_lobdml_status_ora_tables.sql
            echo " CREATE SEQUENCE  $ora_control_schema.LOB_${short_tb}_SEQ  MINVALUE 1  START WITH 1  INCREMENT BY 1  CACHE 20;" >> $dr/create_lobdml_ora_seq.sql

# create lob table trigger with short table name

            echo "CREATE OR REPLACE TRIGGER $ora_control_schema.LOBO_${short_tb}_TRG" >> $dr/create_lobdml_ora_trg.sql
            echo " BEFORE  INSERT OR  UPDATE OR DELETE  ON   $ora_user.${tb}" >> $dr/create_lobdml_ora_trg.sql
            echo " FOR EACH ROW" >> $dr/create_lobdml_ora_trg.sql
            echo "DECLARE">> $dr/create_lobdml_ora_trg.sql
            echo "BEGIN">> $dr/create_lobdml_ora_trg.sql
            echo "    IF INSERTING THEN">> $dr/create_lobdml_ora_trg.sql
            echo "     insert into      $ora_control_schema.LOB_${short_tb}_DML    VALUES ($ora_control_schema.LOB_${short_tb}_SEQ.NEXTVAL,:NEW.${tb}_ID,'insert',CURRENT_TIMESTAMP,'no',NULL);">> $dr/create_lobdml_ora_trg.sql
            echo "    END IF;">> $dr/create_lobdml_ora_trg.sql
            echo "    IF  UPDATING THEN">> $dr/create_lobdml_ora_trg.sql
            echo "    insert into       $ora_control_schema.LOB_${short_tb}_DML    VALUES ($ora_control_schema.LOB_${short_tb}_SEQ.NEXTVAL,:OLD.${tb}_ID,'update',CURRENT_TIMESTAMP,'no',NULL);">> $dr/create_lobdml_ora_trg.sql
            echo "    END IF;">> $dr/create_lobdml_ora_trg.sql
            echo "    IF DELETING THEN">> $dr/create_lobdml_ora_trg.sql
            echo "    insert into       $ora_control_schema.LOB_${short_tb}_DML    VALUES ($ora_control_schema.LOB_${short_tb}_SEQ.NEXTVAL,:OLD.${tb}_ID,'delete',CURRENT_TIMESTAMP,'no',NULL);">> $dr/create_lobdml_ora_trg.sql
            echo "   END IF;">> $dr/create_lobdml_ora_trg.sql
            echo "END ;" >> $dr/create_lobdml_ora_trg.sql
            echo "/">> $dr/create_lobdml_ora_trg.sql
            echo "  ">> $dr/create_lobdml_ora_trg.sql

#generate sync function in edb with short table name

echo "CREATE OR REPLACE FUNCTION LOB_OSF_${short_tb}() RETURNS void AS ">>$dr/create_lob_ora2edb_sync_function.sql
echo "\$BODY\$">>$dr/create_lob_ora2edb_sync_function.sql
echo "DECLARE ">>$dr/create_lob_ora2edb_sync_function.sql
echo " cur_dmls  CURSOR FOR SELECT * FROM $ora_control_schema.LOB_${short_tb}_DML@$edblink WHERE sync_status = 'no' order by dml_id;">>$dr/create_lob_ora2edb_sync_function.sql
echo " rec_dml   RECORD;">>$dr/create_lob_ora2edb_sync_function.sql
echo " BEGIN">>$dr/create_lob_ora2edb_sync_function.sql
echo "   OPEN cur_dmls;">>$dr/create_lob_ora2edb_sync_function.sql
echo "    LOOP">>$dr/create_lob_ora2edb_sync_function.sql
echo "     FETCH cur_dmls INTO rec_dml;">>$dr/create_lob_ora2edb_sync_function.sql
echo "    -- exit when no more row to fetch">>$dr/create_lob_ora2edb_sync_function.sql
echo "      EXIT WHEN NOT FOUND;">>$dr/create_lob_ora2edb_sync_function.sql
echo "     IF  rec_dml.dml_type = 'insert' THEN ">>$dr/create_lob_ora2edb_sync_function.sql
echo "          IF ( select count(*) from $edb_schema.${tb} where ${tb}_ID = rec_dml.pkey ) = 0 THEN">>$dr/create_lob_ora2edb_sync_function.sql
echo "              insert into $edb_schema.${tb} select * from $ora_user.${tb}@$edblink where ${tb}_ID = rec_dml.pkey;">>$dr/create_lob_ora2edb_sync_function.sql
echo "              update $ora_control_schema.LOB_${short_tb}_DML@$edblink set sync_status = 'yes',sync_time=CURRENT_TIMESTAMP where pkey = rec_dml.pkey and  dml_type=rec_dml.dml_type;">>$dr/create_lob_ora2edb_sync_function.sql
echo "          ELSE  ">>$dr/create_lob_ora2edb_sync_function.sql
echo "           update $ora_control_schema.LOB_${short_tb}_DML@$edblink set sync_status = 'dupkey' ">>$dr/create_lob_ora2edb_sync_function.sql
echo "           where pkey = rec_dml.pkey and dml_type=rec_dml.dml_type;">>$dr/create_lob_ora2edb_sync_function.sql
echo "          END IF;">>$dr/create_lob_ora2edb_sync_function.sql
echo "      END IF;    ">>$dr/create_lob_ora2edb_sync_function.sql
echo "      IF  rec_dml.dml_type = 'delete' THEN ">>$dr/create_lob_ora2edb_sync_function.sql
echo "          IF ( select count(*) from  $edb_schema.${tb} where ${tb}_ID = rec_dml.pkey  ) = 1 THEN">>$dr/create_lob_ora2edb_sync_function.sql
echo "              delete from $edb_schema.${tb}  where ${tb}_ID = rec_dml.pkey;">>$dr/create_lob_ora2edb_sync_function.sql
echo "              update $ora_control_schema.LOB_${short_tb}_DML@$edblink set sync_status = 'yes',sync_time=CURRENT_TIMESTAMP where pkey = rec_dml.pkey and  dml_type=rec_dml.dml_type;">>$dr/create_lob_ora2edb_sync_function.sql
echo "          ELSE  ">>$dr/create_lob_ora2edb_sync_function.sql
echo "             update $ora_control_schema.LOB_${short_tb}_DML@$edblink set sync_status = 'notExist' ">>$dr/create_lob_ora2edb_sync_function.sql
echo "             where pkey = rec_dml.pkey and dml_type=rec_dml.dml_type;">>$dr/create_lob_ora2edb_sync_function.sql
echo "          END IF;">>$dr/create_lob_ora2edb_sync_function.sql
echo "      END IF;">>$dr/create_lob_ora2edb_sync_function.sql
echo "      IF  rec_dml.dml_type = 'update' THEN ">>$dr/create_lob_ora2edb_sync_function.sql
echo "        update  $edb_schema.${tb} set ">>$dr/create_lob_ora2edb_sync_function.sql

# get lob table column names

 col_list=(`psql -q ntp -c "select COLUMN_NAME from DBA_TAB_COLUMNS@$edblink where owner='$edb_schema' and table_name ='${tb}' and COLUMN_NAME not in ('${tb}_ID') ;"|tail -n +3| head -n -2`)

# for loop to set lob table column to new value in edb

                  for col_name in "${col_list[@]}"
                    do
                        if [[ $col_name = ${col_list[-1]} ]]
                          then
                          echo "             $col_name = (select $col_name from  $ora_user.${tb}@$edblink where ${tb}_ID = rec_dml.pkey)">>$dr/create_lob_ora2edb_sync_function.sql
                        else
                          echo "             $col_name = (select $col_name from  $ora_user.${tb}@$edblink where ${tb}_ID = rec_dml.pkey),">>$dr/create_lob_ora2edb_sync_function.sql
                        fi
                   done
echo "        where  ${tb}_ID = rec_dml.pkey ;">>$dr/create_lob_ora2edb_sync_function.sql
echo "">>$dr/create_lob_ora2edb_sync_function.sql
echo "        update $ora_control_schema.LOB_${short_tb}_DML@$edblink set sync_status = 'yes',sync_time=CURRENT_TIMESTAMP ">>$dr/create_lob_ora2edb_sync_function.sql
echo "         where pkey = rec_dml.pkey and dml_type=rec_dml.dml_type;">>$dr/create_lob_ora2edb_sync_function.sql
echo "      END IF;">>$dr/create_lob_ora2edb_sync_function.sql
echo "   END LOOP;">>$dr/create_lob_ora2edb_sync_function.sql
echo "   CLOSE cur_dmls;">>$dr/create_lob_ora2edb_sync_function.sql
echo "END; ">>$dr/create_lob_ora2edb_sync_function.sql
echo " \$BODY\$">>$dr/create_lob_ora2edb_sync_function.sql
echo "LANGUAGE plpgsql;">>$dr/create_lob_ora2edb_sync_function.sql
echo "  ">>$dr/create_lob_ora2edb_sync_function.sql
echo "  ">>$dr/create_lob_ora2edb_sync_function.sql

          else
             echo "CREATE TABLE $ora_control_schema.LOB_${tb}_DML ( DML_ID NUMBER PRIMARY KEY , PKEY INT, DML_TYPE VARCHAR(40), DML_TIME TIMESTAMP, SYNC_STATUS VARCHAR(40),SYNC_TIME TIMESTAMP );" >> $dr/create_lobdml_status_ora_tables.sql
             echo " CREATE SEQUENCE  $ora_control_schema.LOB_${tb}_SEQ  MINVALUE 1  START WITH 1  INCREMENT BY 1  CACHE 20;" >> $dr/create_lobdml_ora_seq.sql

#create lob table trigger in ora

    echo "CREATE OR REPLACE TRIGGER $ora_control_schema.LOBO_${tb}_TRG" >> $dr/create_lobdml_ora_trg.sql
    echo " BEFORE  INSERT OR  UPDATE OR DELETE  ON   $ora_user.${tb}" >> $dr/create_lobdml_ora_trg.sql
    echo " FOR EACH ROW" >> $dr/create_lobdml_ora_trg.sql
 echo "DECLARE">> $dr/create_lobdml_ora_trg.sql
 echo "BEGIN">> $dr/create_lobdml_ora_trg.sql
 echo "    IF INSERTING THEN">> $dr/create_lobdml_ora_trg.sql
 echo "     insert into      $ora_control_schema.LOB_${tb}_DML    VALUES ($ora_control_schema.LOB_${tb}_SEQ.NEXTVAL,:NEW.${tb}_ID,'insert',CURRENT_TIMESTAMP,'no',NULL);">> $dr/create_lobdml_ora_trg.sql
 echo "    END IF;">> $dr/create_lobdml_ora_trg.sql
 echo "    IF  UPDATING THEN">> $dr/create_lobdml_ora_trg.sql
 echo "    insert into       $ora_control_schema.LOB_${tb}_DML    VALUES ($ora_control_schema.LOB_${tb}_SEQ.NEXTVAL,:OLD.${tb}_ID,'update',CURRENT_TIMESTAMP,'no',NULL);">> $dr/create_lobdml_ora_trg.sql
 echo "    END IF;">> $dr/create_lobdml_ora_trg.sql
 echo "    IF DELETING THEN">> $dr/create_lobdml_ora_trg.sql
 echo "    insert into       $ora_control_schema.LOB_${tb}_DML    VALUES ($ora_control_schema.LOB_${tb}_SEQ.NEXTVAL,:OLD.${tb}_ID,'delete',CURRENT_TIMESTAMP,'no',NULL);">> $dr/create_lobdml_ora_trg.sql
 echo "   END IF;">> $dr/create_lobdml_ora_trg.sql
 echo "END ;" >> $dr/create_lobdml_ora_trg.sql
 echo "/">> $dr/create_lobdml_ora_trg.sql
 echo "  ">> $dr/create_lobdml_ora_trg.sql

#generate sync function in edb

echo "CREATE OR REPLACE FUNCTION LOB_OSF_${tb}() RETURNS void AS ">>$dr/create_lob_ora2edb_sync_function.sql
echo "\$BODY\$">>$dr/create_lob_ora2edb_sync_function.sql
echo "DECLARE ">>$dr/create_lob_ora2edb_sync_function.sql
echo " cur_dmls  CURSOR FOR SELECT * FROM $ora_control_schema.LOB_${tb}_DML@$edblink WHERE sync_status = 'no' order by dml_id;">>$dr/create_lob_ora2edb_sync_function.sql
echo " rec_dml   RECORD;">>$dr/create_lob_ora2edb_sync_function.sql
echo " BEGIN">>$dr/create_lob_ora2edb_sync_function.sql
echo "   OPEN cur_dmls;">>$dr/create_lob_ora2edb_sync_function.sql
echo "    LOOP">>$dr/create_lob_ora2edb_sync_function.sql
echo "     FETCH cur_dmls INTO rec_dml;">>$dr/create_lob_ora2edb_sync_function.sql
echo "    -- exit when no more row to fetch">>$dr/create_lob_ora2edb_sync_function.sql
echo "      EXIT WHEN NOT FOUND;">>$dr/create_lob_ora2edb_sync_function.sql
echo "     IF  rec_dml.dml_type = 'insert' THEN ">>$dr/create_lob_ora2edb_sync_function.sql
echo "          IF ( select count(*) from $edb_schema.${tb} where ${tb}_ID = rec_dml.pkey ) = 0 THEN">>$dr/create_lob_ora2edb_sync_function.sql
echo "              insert into $edb_schema.${tb} select * from $ora_user.${tb}@$edblink where ${tb}_ID = rec_dml.pkey;">>$dr/create_lob_ora2edb_sync_function.sql
echo "              update $ora_control_schema.LOB_${tb}_DML@$edblink set sync_status = 'yes',sync_time=CURRENT_TIMESTAMP where pkey = rec_dml.pkey and  dml_type=rec_dml.dml_type;">>$dr/create_lob_ora2edb_sync_function.sql
echo "          ELSE  ">>$dr/create_lob_ora2edb_sync_function.sql
echo "           update $ora_control_schema.LOB_${tb}_DML@$edblink set sync_status = 'dupkey' ">>$dr/create_lob_ora2edb_sync_function.sql
echo "           where pkey = rec_dml.pkey and dml_type=rec_dml.dml_type;">>$dr/create_lob_ora2edb_sync_function.sql
echo "          END IF;">>$dr/create_lob_ora2edb_sync_function.sql
echo "      END IF;    ">>$dr/create_lob_ora2edb_sync_function.sql
echo "      IF  rec_dml.dml_type = 'delete' THEN ">>$dr/create_lob_ora2edb_sync_function.sql
echo "          IF ( select count(*) from  $edb_schema.${tb} where ${tb}_ID = rec_dml.pkey  ) = 1 THEN">>$dr/create_lob_ora2edb_sync_function.sql
echo "              delete from $edb_schema.${tb}  where ${tb}_ID = rec_dml.pkey;">>$dr/create_lob_ora2edb_sync_function.sql
echo "              update $ora_control_schema.LOB_${tb}_DML@$edblink set sync_status = 'yes',sync_time=CURRENT_TIMESTAMP where pkey = rec_dml.pkey and  dml_type=rec_dml.dml_type;">>$dr/create_lob_ora2edb_sync_function.sql
echo "          ELSE  ">>$dr/create_lob_ora2edb_sync_function.sql
echo "             update $ora_control_schema.LOB_${tb}_DML@$edblink set sync_status = 'notExist' ">>$dr/create_lob_ora2edb_sync_function.sql
echo "             where pkey = rec_dml.pkey and dml_type=rec_dml.dml_type;">>$dr/create_lob_ora2edb_sync_function.sql
echo "          END IF;">>$dr/create_lob_ora2edb_sync_function.sql
echo "      END IF;">>$dr/create_lob_ora2edb_sync_function.sql
echo "      IF  rec_dml.dml_type = 'update' THEN ">>$dr/create_lob_ora2edb_sync_function.sql
echo "        update  $edb_schema.${tb} set ">>$dr/create_lob_ora2edb_sync_function.sql

# get lob table column names

 col_list=(`psql -q ntp -c "select COLUMN_NAME from DBA_TAB_COLUMNS@$edblink where owner='$edb_schema' and table_name ='${tb}' and COLUMN_NAME not in ('${tb}_ID') ;"|tail -n +3| head -n -2`)

# for loop to set lob table column to new value in edb

                  for col_name in "${col_list[@]}"
                    do
                        if [[ $col_name = ${col_list[-1]} ]]
                          then
                          echo "             $col_name = (select $col_name from  $ora_user.${tb}@$edblink where ${tb}_ID = rec_dml.pkey)">>$dr/create_lob_ora2edb_sync_function.sql
                        else
                          echo "             $col_name = (select $col_name from  $ora_user.${tb}@$edblink where ${tb}_ID = rec_dml.pkey),">>$dr/create_lob_ora2edb_sync_function.sql
                        fi
                   done
echo "        where  ${tb}_ID = rec_dml.pkey ;">>$dr/create_lob_ora2edb_sync_function.sql
echo "">>$dr/create_lob_ora2edb_sync_function.sql
echo "        update $ora_control_schema.LOB_${tb}_DML@$edblink set sync_status = 'yes',sync_time=CURRENT_TIMESTAMP ">>$dr/create_lob_ora2edb_sync_function.sql
echo "         where pkey = rec_dml.pkey and dml_type=rec_dml.dml_type;">>$dr/create_lob_ora2edb_sync_function.sql
echo "      END IF;">>$dr/create_lob_ora2edb_sync_function.sql
echo "   END LOOP;">>$dr/create_lob_ora2edb_sync_function.sql
echo "   CLOSE cur_dmls;">>$dr/create_lob_ora2edb_sync_function.sql
echo "END; ">>$dr/create_lob_ora2edb_sync_function.sql
echo " \$BODY\$">>$dr/create_lob_ora2edb_sync_function.sql
echo "LANGUAGE plpgsql;">>$dr/create_lob_ora2edb_sync_function.sql
echo "  ">>$dr/create_lob_ora2edb_sync_function.sql
echo "  ">>$dr/create_lob_ora2edb_sync_function.sql
   fi
  done

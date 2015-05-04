CREATE OR REPLACE PACKAGE tapi_codegen IS

  -- Author  : PAVEL.KAPLYA, ARTUR.BAYTIN
  -- Created : 27.09.2013 14:28:20
  -- Purpose : Генерация кода пакета для осуществления DML
  -- Version : 0.4

  /*
  История Версий
  0.01 - 26.05.2014
  0.1 - 11.06.2014. Изменения:
        добавлена обработка сложных сущностьей когда PK одновременно является и FK
        добавлен тип записи таблицы
  0.2 - 11.07.2014. Изменения:
        поиск по составным уникальным индексам,
        получение записи по ROWID
        потабличный update для составных сущностей
  0.3 - 21.07.2014. Изменения:
        bulk-insert (только для простых сущностей)    
        вставка из курсора (только для простых сущностей)
        bulk-удаление
  0.4 - 03.05.2015. 
        Версия для публикации на GitHub
  */

  /*
  TODO: owner="Pazus" category="Finish" priority="3 - Low" created="03.05.2015"
  text="Реализовать BULK DML с возвращение и массива обработанных ошибок, нужно придумать, где объявлять соответствующие типы"
  */

  gc_version CONSTANT VARCHAR2(50) := '0.4';

  SUBTYPE t_data_default IS VARCHAR2(2000);
  SUBTYPE t_flag IS NUMBER(1);

  TYPE t_table IS RECORD(
     owner            sys.dba_tables.owner%TYPE
    ,table_name       sys.dba_tables.table_name%TYPE
    ,parentness_level INTEGER);

  TYPE t_column IS RECORD(
     owner          sys.dba_tab_cols.owner%TYPE
    ,table_name     sys.dba_tab_cols.table_name%TYPE
    ,column_name    sys.dba_tab_cols.column_name%TYPE
    ,nullable       sys.dba_tab_cols.nullable%TYPE
    ,data_default   t_data_default
    ,is_primary_key t_flag
    ,column_id      sys.dba_tab_cols.column_id%TYPE);

  TYPE t_table_list IS TABLE OF t_table;
  TYPE t_column_list IS TABLE OF t_column;
  TYPE t_unique_indexes_list IS TABLE OF sys.dba_indexes.index_name%TYPE;

  --Для тестирования
  /*
  PROCEDURE init(par_table_name user_tab_cols.table_name%TYPE);
  FUNCTION gen_specification RETURN CLOB;
  FUNCTION gen_body RETURN CLOB;
  */

  FUNCTION get_columns RETURN t_column_list
    PIPELINED;

  /*
    Капля П.
    Получение всех, включая родительские
  */
  FUNCTION get_tables RETURN t_table_list
    PIPELINED;
  /*
    Получение значения по умолчанию для поля таблицы
    %auth Байтин А.
    %param par_table_name Название таблицы
    %param par_col_name Название поля таблицы
    %return Значение по умолчанию для указанного поля par_col_name таблицы par_table_name
  */
  FUNCTION get_default_col_value
  (
    par_owner      VARCHAR2
   ,par_table_name VARCHAR2
   ,par_col_name   VARCHAR2
  ) RETURN VARCHAR2;

  /*
    Получение значения по умолчанию для поля таблицы
    %auth Капля П.
    %param par_constraint_name Название констрейнта
    %return Текст чека для констрейнта PAR_CONSTRAINT_NAME
  */
  FUNCTION get_cons_search_condition
  (
    par_owner           VARCHAR2
   ,par_constraint_name VARCHAR2
  ) RETURN VARCHAR2;

  FUNCTION gen_package_for_table
  (
    par_owner      VARCHAR2 DEFAULT USER
   ,par_table_name VARCHAR2
  ) RETURN CLOB;

  /*
    Капля П.
    Процедура перекомпиляции DML пакета для таблицы
  */
  PROCEDURE generate_and_compile
  (
    par_owner      VARCHAR2 DEFAULT USER
   ,par_table_name VARCHAR2
  );

  /*
    Капля П.
    Процедура перекомпиляции всех существующих DML пакетов
  */
  PROCEDURE generate_and_compile_all;

END tapi_codegen;
/
CREATE OR REPLACE PACKAGE BODY tapi_codegen IS

  SUBTYPE t_ddl_operation IS VARCHAR2(50);
  SUBTYPE t_parameter IS VARCHAR2(1000); -- Изменил т.к. валился на генерации T_PRODUCT_LINE по длине.
  SUBTYPE t_search_condition IS VARCHAR2(2000);
  SUBTYPE t_io_type IS VARCHAR2(3);
  SUBTYPE t_oracle_object_length IS NUMBER(2);
  SUBTYPE t_object_name IS user_tables.table_name%TYPE;

  gc_comment_separator CONSTANT VARCHAR2(30) := '|';

  gc_object_name_length CONSTANT INTEGER := 30;

  gc_parameter_prefix         CONSTANT VARCHAR2(4) := 'par_';
  gc_sequence_prefix          CONSTANT VARCHAR2(10) := 'SQ_';
  gc_record_parameter         CONSTANT t_object_name := gc_parameter_prefix || 'record';
  gc_record_list_parameter    CONSTANT t_object_name := gc_parameter_prefix || 'record_list';
  gc_record_type_prefix       CONSTANT t_object_name := 'tt_';
  gc_record_table_type_prefix CONSTANT t_object_name := 'tn_';
  gc_type_prefix              CONSTANT t_object_name := 'typ_';
  gc_package_prefix           CONSTANT VARCHAR2(4) := 'dm2_';
  gc_package_suffix           CONSTANT VARCHAR2(4) := '';
  gc_parameter_prefix_length  CONSTANT INTEGER := length(gc_parameter_prefix);
  gc_nested_table_type_name   CONSTANT t_object_name := gc_type_prefix || 'nested_table';

  gc_table_index_by_type_name  CONSTANT t_object_name := gc_type_prefix || 'associative_array';
  gc_record_cursor_type_name   CONSTANT t_object_name := gc_type_prefix || 'strong_cursor';
  gc_pk_cursor_type_name       CONSTANT t_object_name := gc_type_prefix || 'pk_cursor';
  gc_pk_nested_table_type_name CONSTANT t_object_name := gc_type_prefix || 'prymary_key_nested_table';

  gc_collect_bulk_ex_par_name    CONSTANT t_object_name := gc_parameter_prefix || 'exceptions_occured';
  gc_bulk_ex_list_par_name       CONSTANT t_object_name := gc_parameter_prefix || 'exceptions';
  gc_cursor_parameter_name       CONSTANT t_object_name := gc_parameter_prefix || 'cursor';
  gc_pk_list_parameter_name      CONSTANT t_object_name := gc_parameter_prefix || 'primary_key_list';
  gc_cursor_limit_parameter_name CONSTANT t_object_name := gc_parameter_prefix || 'limit';
  gc_default_limit               CONSTANT INTEGER := 100;

  gc_insert                      CONSTANT t_ddl_operation := 'INSERT';
  gc_insert_noout                CONSTANT t_ddl_operation := 'INSERT_NOOUT';
  gc_insert_record               CONSTANT t_ddl_operation := 'INSERT_RECORD';
  gc_insert_from_cursor          CONSTANT t_ddl_operation := 'INSERT_CURSOR';
  gc_bulk_insert_nested_table    CONSTANT t_ddl_operation := 'BULK_INSERT_NESTED_TABLE';
  gc_bulk_insert_table_index_by  CONSTANT t_ddl_operation := 'BULK_INSERT_INDEX_BY_TABLE';
  gc_bulk_insert_nested_table2   CONSTANT t_ddl_operation := 'BULK_INSERT_NESTED_TABLE_WITH_OUT';
  gc_bulk_insert_table_index_by2 CONSTANT t_ddl_operation := 'BULK_INSERT_INDEX_BY_TABLE_WITH_OUT';

  gc_bulk_delete        CONSTANT t_ddl_operation := 'BULK_DELETE';
  gc_bulk_delete2       CONSTANT t_ddl_operation := 'BULK_DELETE_WITH_OUT';
  gc_delete_from_cursor CONSTANT t_ddl_operation := 'DELETE_CURSOR';
  gc_delete             CONSTANT t_ddl_operation := 'DELETE';

  gc_update CONSTANT t_ddl_operation := 'UPDATE';

  gc_in  CONSTANT t_io_type := 'IN';
  gc_out CONSTANT t_io_type := 'OUT';

  gv_table_list         t_table_list;
  gv_column_list        t_column_list;
  gv_unique_indexes     t_unique_indexes_list;
  gv_primary_key_column sys.dba_tab_cols.column_name%TYPE;
  gv_table_name         sys.dba_tab_cols.table_name%TYPE;
  gv_owner              sys.dba_tab_cols.owner%TYPE;
  gv_current_schema     t_object_name;

  TYPE t_simple_column IS RECORD(
     owner                  sys.dba_tab_columns.owner%TYPE
    ,table_name             sys.dba_tab_columns.table_name%TYPE
    ,column_name            sys.dba_tab_columns.column_name%TYPE
    ,max_column_name_length INTEGER);
  TYPE tt_simple_column_list IS TABLE OF t_simple_column;

  FUNCTION get_table_comment
  (
    par_owner      VARCHAR2
   ,par_table_name VARCHAR2
  ) RETURN VARCHAR2 IS
    v_table_comment sys.dba_tab_comments.comments%TYPE;
  BEGIN
    BEGIN
      SELECT nvl(regexp_substr(t.comments, '[^' || gc_comment_separator || ']+'), par_table_name)
        INTO v_table_comment
        FROM sys.dba_tab_comments t
       WHERE t.owner = par_owner
         AND t.table_name = par_table_name;
    EXCEPTION
      WHEN standard.no_data_found THEN
        v_table_comment := par_table_name;
    END;
    RETURN v_table_comment;
  END get_table_comment;

  /*
    Получение значения по умолчанию для поля таблицы
    %auth Байтин А.
    %param par_table_name Название таблицы
    %param par_col_name Название поля таблицы
    %return Значение по умолчанию для указанного поля par_col_name таблицы par_table_name
  */
  FUNCTION get_default_col_value
  (
    par_owner      VARCHAR2
   ,par_table_name VARCHAR2
   ,par_col_name   VARCHAR2
  ) RETURN VARCHAR2 IS
    v_data_default t_data_default;
  BEGIN
    SELECT ut.data_default
      INTO v_data_default
      FROM sys.dba_tab_cols ut
     WHERE ut.table_name = par_table_name
       AND ut.owner = par_owner
       AND ut.column_name = par_col_name;
  
    v_data_default := TRIM(rtrim(v_data_default, chr(10)));
  
    IF upper(v_data_default) = 'NULL'
    THEN
      v_data_default := NULL;
    END IF;
    -- trim потому что иногда перенос строки есть
    RETURN v_data_default;
  END get_default_col_value;

  FUNCTION get_column_comment
  (
    par_owner       VARCHAR2
   ,par_table_name  VARCHAR2
   ,par_column_name VARCHAR2
  ) RETURN VARCHAR2 IS
    v_comment sys.dba_col_comments.comments%TYPE;
  BEGIN
    BEGIN
      SELECT nvl(regexp_substr(t.comments, '[^' || gc_comment_separator || ']+'), t.column_name)
        INTO v_comment
        FROM sys.dba_col_comments t
       WHERE t.table_name = par_table_name
         AND t.column_name = par_column_name
         AND t.owner = par_owner;
    EXCEPTION
      WHEN standard.no_data_found THEN
        v_comment := upper(par_column_name);
    END;
    RETURN v_comment;
  END get_column_comment;

  /*
    Получение значения по умолчанию для поля таблицы
    %auth Капля П.
    %param par_constraint_name Название констрейнта
    %return Текст чека для констрейнта PAR_CONSTRAINT_NAME
  */
  FUNCTION get_cons_search_condition
  (
    par_owner           VARCHAR2
   ,par_constraint_name VARCHAR2
  ) RETURN VARCHAR2 IS
    v_search_condition t_search_condition;
  BEGIN
    SELECT uc.search_condition
      INTO v_search_condition
      FROM sys.dba_constraints uc
     WHERE uc.constraint_name = par_constraint_name
       AND uc.owner = par_owner;
    RETURN v_search_condition;
  END get_cons_search_condition;

  /*
    Получение списка полей для указанной таблицы
    %auth Байтин А.
    %param par_table_name Название таблицы
    %return Массив полей таблицы par_table_name
  */
  PROCEDURE init
  (
    par_owner      VARCHAR2
   ,par_table_name VARCHAR2
  ) IS
    PROCEDURE check_table_exists
    (
      par_owner      VARCHAR2
     ,par_table_name VARCHAR2
    ) IS
      v_exists NUMBER(1);
    BEGIN
      SELECT COUNT(1)
        INTO v_exists
        FROM dual
       WHERE EXISTS (SELECT NULL
                FROM sys.dba_tables t
               WHERE t.owner = par_owner
                 AND t.table_name = par_table_name);
    
      IF v_exists = 0
      THEN
        raise_application_error(-20101
                               ,'Table ' || par_owner || '.' || par_table_name || ' doesn''t exist');
      END IF;
    END check_table_exists;
  BEGIN
    -- Проверка входного параметра
  
    check_table_exists(par_owner, par_table_name);
  
    gv_owner      := par_owner;
    gv_table_name := par_table_name;
  
    SELECT t.owner
          ,t.table_name
          ,LEVEL AS parentness_level
      BULK COLLECT
      INTO gv_table_list
      FROM sys.dba_tables t
			-- Ограничиваем первым уровнем вложенности, не понятно, как бороться с дублирующими столбцами
			where level <2
     START WITH t.table_name = gv_table_name
            AND t.owner = gv_owner
    CONNECT BY PRIOR (SELECT uc2.table_name
                        FROM sys.dba_constraints  uc_pk
                            ,sys.dba_cons_columns ucc_pk
                            ,sys.dba_constraints  uc_fk
                            ,sys.dba_cons_columns ucc_fk
                            ,sys.dba_constraints  uc2
                       WHERE uc_pk.table_name = t.table_name
                         AND uc_pk.owner = t.owner
                         AND uc_pk.constraint_type = 'P'
                         AND uc_pk.constraint_name = ucc_pk.constraint_name
                         AND uc_pk.owner = ucc_pk.owner
                         AND ucc_pk.table_name = ucc_fk.table_name
                         AND ucc_pk.column_name = ucc_fk.column_name
                         AND ucc_pk.owner = ucc_fk.owner
                         AND ucc_fk.constraint_name = uc_fk.constraint_name
                         AND ucc_fk.owner = uc_fk.owner
                         AND uc_fk.constraint_type = 'R'
                         AND uc_fk.r_constraint_name = uc2.constraint_name
                         AND uc_fk.r_owner = uc2.owner) = t.table_name
           AND PRIOR t.owner = t.owner;
  
    WITH tt AS
     (SELECT /*+cardinality (3)*/
       *
        FROM TABLE(get_tables)),
    checks AS
     (SELECT /*+materialize*/
       t.owner
      ,t.table_name
      ,t.column_name
      ,tapi_codegen.get_cons_search_condition(uc.owner, uc.constraint_name) search_condition
        FROM sys.dba_cons_columns t
            ,sys.dba_constraints  uc
            ,tt
       WHERE t.table_name = tt.table_name
         AND t.owner = tt.owner
         AND t.constraint_name = uc.constraint_name
         AND t.owner = uc.owner
         AND uc.constraint_type = 'C')
    
    SELECT ut.owner
          ,ut.table_name  AS table_name
          ,ut.column_name AS column_name
           -- Пришлось сделать столь сложную проверку т.к. иначе не видны чеки с NOVALIDATE
          ,CASE
             WHEN ut.nullable = 'N'
                  OR EXISTS
              (SELECT NULL
                     FROM checks c
                    WHERE c.table_name = ut.table_name
                      AND c.column_name = ut.column_name
                      AND c.search_condition = '"' || upper(ut.column_name) || '" IS NOT NULL') THEN
              'N'
             ELSE
              'Y'
           END AS nullable
           --,ut.nullable
          ,get_default_col_value(ut.owner, ut.table_name, ut.column_name)
          ,CASE
             WHEN EXISTS (SELECT NULL
                     FROM sys.dba_cons_columns ucc
                         ,sys.dba_constraints  uc
                    WHERE ucc.table_name = ut.table_name
                      AND ut.owner = ucc.owner
                      AND ucc.column_name = ut.column_name
                      AND ucc.owner = uc.owner
                      AND ucc.constraint_name = uc.constraint_name
                      AND uc.constraint_type = 'P') THEN
              1
             ELSE
              0
           END AS is_pk
          ,ut.column_id
      BULK COLLECT
      INTO gv_column_list
      FROM sys.dba_tab_cols ut
          ,tt               t
     WHERE ut.table_name = t.table_name
       AND ut.owner = t.owner
          --AND ut.column_name NOT IN ('GUID', 'FILIAL_ID', 'EXT_ID')
          --AND (t.parentness_level > 1 OR ut.column_name != 'ENT_ID')
       AND ut.virtual_column = 'NO'
       AND ut.hidden_column = 'NO';
  
    BEGIN
      SELECT cl.column_name
        INTO gv_primary_key_column
        FROM TABLE(get_columns) cl
            ,TABLE(get_tables) t
       WHERE cl.is_primary_key = 1
         AND t.table_name = cl.table_name
         AND t.owner = cl.owner
         AND t.parentness_level = 1;
    EXCEPTION
      WHEN no_data_found THEN
        raise_application_error(-20101, 'There is no primary key column');
    END;
  
    SELECT t2.index_name
      BULK COLLECT
      INTO gv_unique_indexes
      FROM sys.user_indexes t2
          ,TABLE(get_tables) t
     WHERE t2.table_name = t.table_name
       AND t2.table_owner = t.owner
       AND t2.uniqueness = 'UNIQUE'
       AND t2.index_type = 'NORMAL'
       AND NOT EXISTS (SELECT NULL
              FROM user_constraints c
             WHERE c.index_name = t2.index_name
               AND c.constraint_type = 'P'
               AND c.table_name = t.table_name
               AND c.owner = t.owner)
          -- Исключить из уникальных индексов поля-объекты.
       AND NOT EXISTS (SELECT NULL
              FROM user_ind_columns ic
             WHERE t2.index_name = ic.index_name
               AND instr(regexp_replace(ic.column_name, '"[^"]*"'), '.') != 0)
     ORDER BY t.parentness_level;
  
  END init;

  PROCEDURE append_lob
  (
    par_lob  IN OUT CLOB
   ,par_buff IN VARCHAR2
  ) IS
  BEGIN
    IF length(par_buff) > 0
    THEN
      dbms_lob.writeappend(lob_loc => par_lob, amount => length(par_buff), buffer => par_buff);
    END IF;
  END append_lob;

  PROCEDURE append_lob
  (
    par_lob  IN OUT CLOB
   ,par_buff IN CLOB
  ) IS
    v_length INTEGER;
    v_amount INTEGER;
    v_offset INTEGER := 1;
    v_buffer VARCHAR2(32767);
  BEGIN
    v_length := dbms_lob.getlength(par_buff);
    WHILE v_length > 0
    LOOP
      v_amount := least(v_length, 32767);
      v_buffer := dbms_lob.substr(lob_loc => par_buff, amount => v_amount, offset => v_offset);
    
      v_offset := v_offset + v_amount;
      v_length := v_length - v_amount;
    
      dbms_lob.writeappend(lob_loc => par_lob, amount => v_amount, buffer => v_buffer);
    END LOOP;
  END append_lob;

  /*FUNCTION get_unique_columns RETURN tt_one_col
    PIPELINED IS
  BEGIN
    assert_deprecated(gv_column_list IS NULL
          ,'Список полей не инициализирован, выполните init_column_list!');
    FOR v_idx IN gv_column_list.first .. gv_column_list.last
    LOOP
      PIPE ROW(gv_column_list(v_idx));
    END LOOP;
  END get_unique_columns;*/

  /*
    Капля П.
    Получение всех, включая родительские
  */
  FUNCTION get_tables RETURN t_table_list
    PIPELINED IS
  BEGIN
    FOR i IN 1 .. gv_table_list.count
    LOOP
      PIPE ROW(gv_table_list(i));
    END LOOP;
  END get_tables;

  /*
    Байтин А.
    Получение параметров для использования в SQL
  */
  FUNCTION get_columns RETURN t_column_list
    PIPELINED IS
  BEGIN
  
    FOR v_idx IN gv_column_list.first .. gv_column_list.last
    LOOP
      PIPE ROW(gv_column_list(v_idx));
    END LOOP;
  END get_columns;

  FUNCTION get_shorten_variable_name
  (
    par_original_name VARCHAR2
   ,par_max_length    INTEGER
  ) RETURN VARCHAR2 IS
  
    TYPE t_parts IS RECORD(
       part_name       VARCHAR2(100)
      ,separator       VARCHAR2(1)
      ,part_length     INTEGER
      ,par_count       INTEGER
      ,max_part_length INTEGER);
  
    TYPE tt_parts IS TABLE OF t_parts;
  
    v_parts tt_parts;
  
    v_current_index INTEGER;
  
    FUNCTION get_parts_agregated RETURN VARCHAR2 IS
      v_aggregated VARCHAR2(1000);
    BEGIN
      FOR i IN 1 .. v_parts.count
      LOOP
        v_aggregated := v_aggregated || v_parts(i).part_name;
        IF i < v_parts.count
        THEN
          v_aggregated := v_aggregated || v_parts(i).separator;
        END IF;
      END LOOP;
      RETURN v_aggregated;
    END get_parts_agregated;
  
    FUNCTION get_longest_part_index RETURN VARCHAR2 IS
      v_index INTEGER := v_parts.count;
    BEGIN
      FOR i IN 1 .. v_parts.count
      LOOP
        IF nvl(length(v_parts(v_index).part_name), 0) < nvl(length(v_parts(i).part_name), 0)
        THEN
          v_index := i;
        END IF;
      END LOOP;
      RETURN v_index;
    END get_longest_part_index;
  
  BEGIN
  
    SELECT NAME
          ,separator
          ,nvl(length(NAME), 0) AS part_length
          ,COUNT(NAME) over() AS parts_count
          ,MAX(nvl(length(NAME), 0)) over() AS max_part_length
      BULK COLLECT
      INTO v_parts
      FROM (SELECT regexp_replace(regexp_substr(par_original_name, '.*?([#_$]|$)', 1, rownum), '[#_$]') AS NAME
                  ,regexp_replace(regexp_substr(par_original_name, '.*?([#_$]|$)', 1, rownum)
                                 ,'[^#_$]') AS separator
                  ,LEVEL AS original_order
              FROM dual
            CONNECT BY regexp_substr(par_original_name, '.*?([#_$]|$)', 1, rownum) IS NOT NULL)
     ORDER BY original_order;
  
    WHILE length(get_parts_agregated) > par_max_length
    LOOP
      v_current_index := get_longest_part_index;
      v_parts(v_current_index).part_name := substr(v_parts(v_current_index).part_name
                                                  ,1
                                                  ,length(v_parts(v_current_index).part_name) - 1);
    END LOOP;
  
    RETURN lower(get_parts_agregated);
  END get_shorten_variable_name;

  FUNCTION get_shorten_parameter(par_column_name VARCHAR2) RETURN VARCHAR2 IS
    v_parameter_name t_object_name;
  BEGIN
    v_parameter_name := lower(gc_parameter_prefix ||
                              get_shorten_variable_name(par_column_name
                                                       ,gc_object_name_length -
                                                        gc_parameter_prefix_length));
  
    RETURN v_parameter_name;
  END get_shorten_parameter;

  FUNCTION get_table_parent_level
  (
    par_owner      VARCHAR2
   ,par_table_name VARCHAR2
  ) RETURN INTEGER IS
    v_parent_level INTEGER;
  BEGIN
    SELECT parentness_level
      INTO v_parent_level
      FROM TABLE(get_tables)
     WHERE table_name = upper(par_table_name)
       AND owner = upper(par_owner);
  
    RETURN v_parent_level;
  END get_table_parent_level;

  PROCEDURE parse_and_replace
  (
    par_string     IN OUT VARCHAR2
   ,par_owner      VARCHAR2 DEFAULT gv_owner
   ,par_table_name VARCHAR2 DEFAULT gv_table_name
  ) IS
  BEGIN
    par_string := REPLACE(par_string
                         ,'<#PAR_PREFIX><#PK_FIELD_NAME>'
                         ,get_shorten_parameter(gv_primary_key_column));
    par_string := REPLACE(par_string, '<#PAR_PREFIX>', lower(gc_parameter_prefix));
    par_string := REPLACE(par_string, '<#PK_FIELD_NAME>', lower(gv_primary_key_column));
    par_string := REPLACE(par_string, '<#OWNER>', lower(par_owner));
    par_string := REPLACE(par_string, '<#TABLE_NAME>', lower(par_table_name));
    par_string := REPLACE(par_string
                         ,'<#TABLE_COMMENT>'
                         , REPLACE(get_table_comment('INS', par_table_name), q'{'}', q'{''}'));
    par_string := REPLACE(par_string, '<#RECORD_PARAM_LIST>', lower(gc_record_list_parameter));
    par_string := REPLACE(par_string, '<#RECORD_PK_LIST>', lower(gc_pk_list_parameter_name));
    par_string := REPLACE(par_string, '<#RECORD_PARAM>', lower(gc_record_parameter));
    par_string := REPLACE(par_string, '<#RECORD_TYPE>', lower(gc_record_type_prefix || par_table_name));
  
    par_string := REPLACE(par_string, '<#CURSOR_PARAM>', lower(gc_cursor_parameter_name));
    par_string := REPLACE(par_string
                         ,'<#CURSOR_LIMIT_PARAM_NAME>'
                         ,lower(gc_cursor_limit_parameter_name));
    par_string := REPLACE(par_string, '<#RECORD_CURSOR_PARAM_TYPE>', lower(gc_record_cursor_type_name));
    par_string := REPLACE(par_string, '<#PK_CURSOR_PARAM_TYPE>', lower(gc_pk_cursor_type_name));
  
    par_string := REPLACE(par_string, '<#RECORD_PK_LIST_TYPE>', lower(gc_pk_nested_table_type_name));
  
    par_string := REPLACE(par_string, '<#ASSOC_ARRAY_TYPE_NAME>', lower(gc_table_index_by_type_name));
    par_string := REPLACE(par_string, '<#NESTED_TABLE_TYPE_NAME>', lower(gc_nested_table_type_name));
  
    par_string := REPLACE(par_string
                         ,'<#BULK_DML_EXCEPTION_OCCURED_FLAG_PARAM>'
                         ,lower(gc_collect_bulk_ex_par_name));
    par_string := REPLACE(par_string
                         ,'<#BULK_DML_EXCEPTION_LIST_PARAM>'
                         ,lower(gc_bulk_ex_list_par_name));
    par_string := REPLACE(par_string, '<#BULK_DML_DEFAULT_LIMIT>', gc_default_limit);
  END parse_and_replace;

  PROCEDURE parse_and_replace_col_template
  (
    par_string      IN OUT VARCHAR2
   ,par_column_name VARCHAR2
   ,par_table_name  VARCHAR2 DEFAULT gv_table_name
  ) IS
  BEGIN
    par_string := REPLACE(par_string
                         ,'<#RECORD_FIELD>'
                         ,lower(gc_record_parameter || '.' || par_column_name));
    par_string := REPLACE(par_string
                         ,'<#PAR_PREFIX><#COLUMN_NAME>'
                         ,get_shorten_parameter(par_column_name));
    par_string := REPLACE(par_string, '<#COLUMN_NAME>', lower(par_column_name));
    par_string := REPLACE(par_string
                         ,'<#COLUMN_COMMENT>'
                         , REPLACE(get_column_comment('INS', par_table_name, par_column_name)
                                  , q'{'}'
                                 ,q'{''}'));
  END parse_and_replace_col_template;

  FUNCTION get_aggregated_column_list
  (
    par_column_list tt_simple_column_list
   ,par_separator   VARCHAR2 DEFAULT '_'
  ) RETURN VARCHAR2 IS
    v_aggregated_column_list VARCHAR2(500);
  BEGIN
    FOR i IN par_column_list.first .. par_column_list.last
    LOOP
      v_aggregated_column_list := v_aggregated_column_list || par_column_list(i).column_name ||
                                  par_separator;
    END LOOP;
  
    RETURN lower(rtrim(v_aggregated_column_list, par_separator));
  END get_aggregated_column_list;

  FUNCTION get_column_list(par_index_name user_indexes.index_name%TYPE) RETURN tt_simple_column_list IS
    v_column_list tt_simple_column_list;
  BEGIN
    SELECT lower(t.table_owner)
          ,lower(t.table_name)
          ,lower(t.column_name)
          ,MAX(length(t.column_name)) over()
      BULK COLLECT
      INTO v_column_list
      FROM sys.dba_ind_columns t
     WHERE t.index_name = par_index_name
     ORDER BY t.column_position;
  
    RETURN v_column_list;
  END get_column_list;

  FUNCTION gen_single_parameter
  (
    par_column_name            user_tab_cols.column_name%TYPE
   ,par_max_column_name_length t_oracle_object_length
   ,par_data_default           t_data_default
   ,par_nullable               user_tab_cols.nullable%TYPE
   ,par_io_type                t_io_type
   ,par_table_name             VARCHAR2 DEFAULT gv_table_name
  ) RETURN t_parameter IS
    v_parameter_str t_parameter;
  BEGIN
    -- Проверка входных параметров
  
    v_parameter_str := rpad(get_shorten_parameter(par_column_name)
                           ,par_max_column_name_length + 4
                           ,' ') || ' ' || par_io_type || ' ' || lower(par_table_name) || '.' ||
                       lower(par_column_name) || '%TYPE';
  
    IF par_data_default IS NOT NULL
    THEN
      v_parameter_str := v_parameter_str || ' DEFAULT ' || par_data_default;
    ELSIF par_nullable = 'Y'
    THEN
      v_parameter_str := v_parameter_str || ' DEFAULT NULL';
    END IF;
  
    RETURN v_parameter_str;
  END gen_single_parameter;

  /*
    Байтин А.
    Формирование списка параметров
  */
  FUNCTION gen_parameter_list(par_ddl_operation t_ddl_operation) RETURN CLOB IS
    v_parameter_str  t_parameter;
    v_parameter_list CLOB;
  BEGIN
  
    dbms_lob.createtemporary(lob_loc => v_parameter_list, cache => TRUE);
    FOR vr_column IN (SELECT table_name
                            ,column_name
                            ,nullable
                            ,data_default
                            ,is_primary_key
                            ,io_type
                            ,owner
                            ,MAX(length(column_name)) over() AS max_column_name_length
                        FROM (SELECT cl.table_name
                                    ,cl.owner
                                    ,cl.column_name
                                    ,cl.nullable
                                    ,cl.data_default
                                    ,cl.is_primary_key
                                    ,CASE
                                       WHEN cl.is_primary_key = 1
                                            AND par_ddl_operation = gc_insert THEN
                                        gc_out
                                       ELSE
                                        gc_in
                                     END AS io_type
                                FROM TABLE(tapi_codegen.get_columns) cl
                                    ,TABLE(get_tables) t
                               WHERE cl.table_name = t.table_name
                                 AND cl.owner = t.owner
                                 AND (t.parentness_level = 1 OR cl.is_primary_key = 0)
                              --AND cl.column_name != 'ENT_ID'
                               ORDER BY CASE
                                          WHEN cl.is_primary_key = 1
                                               AND par_ddl_operation = gc_insert THEN
                                           2
                                          WHEN cl.is_primary_key = 1
                                               AND par_ddl_operation IN (gc_update, gc_delete) THEN
                                           0
                                          ELSE
                                           1
                                        END
                                       ,CASE
                                          WHEN cl.nullable = 'N' THEN
                                           0
                                          ELSE
                                           1
                                        END
                                       ,CASE
                                          WHEN cl.data_default IS NULL THEN
                                           0
                                          ELSE
                                           1
                                        END
                                       ,t.parentness_level
                                       ,cl.column_id)
                       WHERE rownum = 1
                         AND par_ddl_operation = gc_delete
                          OR par_ddl_operation IN (gc_insert, gc_update)
                          OR (par_ddl_operation = gc_insert_noout AND is_primary_key = 0))
    LOOP
      IF v_parameter_str IS NOT NULL
      THEN
        append_lob(par_lob => v_parameter_list, par_buff => chr(10) || '   ,');
      ELSE
        append_lob(par_lob => v_parameter_list, par_buff => '    ');
      END IF;
      v_parameter_str := gen_single_parameter(par_column_name            => vr_column.column_name
                                             ,par_max_column_name_length => vr_column.max_column_name_length
                                             ,par_data_default           => vr_column.data_default
                                             ,par_nullable               => vr_column.nullable
                                             ,par_io_type                => vr_column.io_type
                                             ,par_table_name             => vr_column.table_name);
    
      append_lob(par_lob => v_parameter_list, par_buff => v_parameter_str);
    END LOOP;
  
    RETURN v_parameter_list;
  END gen_parameter_list;

  FUNCTION gen_table_record_type RETURN VARCHAR2 IS
    v_declaration      VARCHAR2(32767);
    v_record_type_name t_object_name;
  BEGIN
    v_record_type_name := gc_record_type_prefix ||
                          get_shorten_variable_name(gv_table_name
                                                   ,gc_object_name_length -
                                                    length(gc_record_type_prefix));
  
    v_declaration := '  TYPE ' || v_record_type_name || ' IS RECORD(';
  
    IF gv_table_list.count = 1
    THEN
      /*
        Если таблица без парента, то генерим rowtype совместимый record
      */
      /*v_declaration := '  SUBTYPE tt_<#TABLE_NAME> IS <#TABLE_NAME>%ROWTYPE';
      parse_and_replace(v_declaration);*/
    
      FOR rec IN (WITH checks AS
                     (SELECT /*+materialize*/
                      t.table_name
                     ,t.column_name
                     ,t.owner
                     ,tapi_codegen.get_cons_search_condition(uc.owner, uc.constraint_name) search_condition
                       FROM sys.dba_cons_columns t
                           ,sys.dba_constraints  uc
                      WHERE t.table_name = gv_table_name
											  and t.owner = gv_owner
                        AND t.constraint_name = uc.constraint_name
                        AND t.owner = uc.owner
                        AND uc.constraint_type = 'C')
                    SELECT ut.table_name  AS table_name
                          ,ut.column_name AS column_name
                          ,ut.owner
                           -- Пришлось сделать столь сложную проверку т.к. иначе не видны чеки с NOVALIDATE
                          ,CASE
                             WHEN ut.nullable = 'N'
                                  OR EXISTS
                              (SELECT NULL
                                     FROM checks c
                                    WHERE c.table_name = ut.table_name
                                      AND c.column_name = ut.column_name
																			and c.owner = ut.owner
                                      AND c.search_condition =
                                          '"' || upper(ut.column_name) || '" IS NOT NULL') THEN
                              'N'
                             ELSE
                              'Y'
                           END AS nullable
                           --,ut.nullable
                          ,get_default_col_value(ut.owner, ut.table_name, ut.column_name) data_default
                          ,ut.column_id
                          ,row_number() over(ORDER BY ut.column_id) AS rn
                          ,MAX(length(ut.column_name)) over() + 1 AS max_length
                      FROM sys.dba_tab_cols ut
                     WHERE ut.table_name = gv_table_name
                       AND ut.owner = gv_owner
                       AND ut.virtual_column = 'NO'
                       AND ut.hidden_column = 'NO'
                     ORDER BY column_id)
      LOOP
        IF rec.rn = 1
        THEN
          v_declaration := v_declaration || chr(10) || '    ';
        ELSE
          v_declaration := v_declaration || chr(10) || '   ,';
        END IF;
      
        v_declaration := v_declaration || rpad(lower(rec.column_name), rec.max_length, ' ') ||
                         lower(rec.owner||'.'|| rec.table_name || '.' || rec.column_name) || '%TYPE';
      
        IF rec.data_default IS NOT NULL
        THEN
          /*
          IF rec.nullable = 'N'
          THEN
            v_declaration := v_declaration || ' NOT NULL';
          END IF;
          */
          v_declaration := v_declaration || ' DEFAULT ' || rec.data_default;
        END IF;
      
      END LOOP;
    
    ELSE
    
      FOR rec IN (SELECT cl.*
                        ,row_number() over(ORDER BY t.parentness_level, cl.column_id) AS rn
                        ,MAX(length(cl.column_name)) over() + 1 AS max_length
                    FROM TABLE(get_columns) cl
                        ,TABLE(get_tables) t
                   WHERE cl.table_name = t.table_name
                     AND cl.owner = t.owner
                     AND (cl.is_primary_key = 0 OR t.parentness_level = 1)
                  --AND cl.column_name != 'ENT_ID'
                   ORDER BY t.parentness_level
                           ,cl.column_id)
      LOOP
        IF rec.rn = 1
        THEN
          v_declaration := v_declaration || chr(10) || '    ';
        ELSE
          v_declaration := v_declaration || chr(10) || '   ,';
        END IF;
      
        v_declaration := v_declaration || rpad(lower(rec.column_name), rec.max_length, ' ') ||
                         lower(rec.owner||'.'||rec.table_name || '.' || rec.column_name) || '%TYPE';
      
        IF rec.data_default IS NOT NULL
        THEN
          IF rec.nullable = 'N'
          THEN
            v_declaration := v_declaration || ' NOT NULL';
          END IF;
          v_declaration := v_declaration || ' DEFAULT ' || rec.data_default;
        END IF;
      
      END LOOP;
    
    END IF;
    v_declaration := v_declaration || ');';
  
    v_declaration := v_declaration || chr(10) || chr(10) || '  TYPE ' || gc_pk_nested_table_type_name ||
                     ' IS TABLE OF ' || gv_table_name || '.' || gv_primary_key_column || '%TYPE;';
    v_declaration := v_declaration || chr(10) || chr(10) || '  TYPE ' || gc_nested_table_type_name ||
                     ' IS TABLE OF ' || v_record_type_name || ';';
    v_declaration := v_declaration || chr(10) || '  TYPE ' || gc_table_index_by_type_name ||
                     ' IS TABLE OF ' || v_record_type_name || ' INDEX BY PLS_INTEGER' || ';';
    /*
    Заготовка на будущее.
    v_declaration := v_declaration || chr(10) || chr(10) || '  TYPE ' || gc_pk_cursor_type_name ||
                     ' IS REF CURSOR RETURN ' || lower(gv_table_name ||'.' || gv_primary_key_column) ||'%TYPE';
    */
  
    v_declaration := v_declaration || chr(10) || chr(10) || '  TYPE ' || gc_record_cursor_type_name ||
                     ' IS REF CURSOR RETURN ' || v_record_type_name;
  
    RETURN v_declaration;
  
    RETURN v_declaration;
  END gen_table_record_type;

  FUNCTION gen_procedure_header(par_ddl_operation t_ddl_operation) RETURN VARCHAR2 IS
    v_ddl_operation t_ddl_operation;
    v_header        VARCHAR2(4000);
  BEGIN
    IF par_ddl_operation IN (gc_insert_noout
                            ,gc_insert_record
                            ,gc_bulk_insert_nested_table
                            ,gc_bulk_insert_table_index_by
                            ,gc_bulk_insert_nested_table2
                            ,gc_bulk_insert_table_index_by2
                            ,gc_insert_from_cursor)
    THEN
      v_ddl_operation := gc_insert;
    ELSIF par_ddl_operation IN (gc_bulk_delete, gc_bulk_delete2, gc_delete_from_cursor)
    THEN
      v_ddl_operation := gc_delete;
    ELSE
      v_ddl_operation := par_ddl_operation;
    END IF;
  
    v_header := REPLACE('  PROCEDURE <#DDL_OPERATION>_record'
                       ,'<#DDL_OPERATION>'
                       ,lower(v_ddl_operation));
  
    IF par_ddl_operation IN (gc_bulk_insert_nested_table
                            ,gc_bulk_insert_table_index_by
                            ,gc_bulk_insert_nested_table2
                            ,gc_bulk_insert_table_index_by2
                            ,gc_bulk_delete
                            ,gc_bulk_delete2)
    THEN
      v_header := v_header || '_list';
    ELSIF par_ddl_operation IN (gc_insert_from_cursor, gc_delete_from_cursor)
    THEN
      v_header := v_header || '_from_cursor';
    END IF;
  
    RETURN v_header;
  
  END gen_procedure_header;

  FUNCTION gen_procedure_footer(par_ddl_operation t_ddl_operation) RETURN VARCHAR2 IS
    v_ddl_operation t_ddl_operation;
    v_footer        VARCHAR2(4000);
  BEGIN
    IF par_ddl_operation IN (gc_insert_noout
                            ,gc_insert_record
                            ,gc_bulk_insert_nested_table
                            ,gc_bulk_insert_table_index_by
                            ,gc_insert_from_cursor
                            ,gc_bulk_insert_nested_table2
                            ,gc_bulk_insert_table_index_by2)
    THEN
      v_ddl_operation := gc_insert;
    ELSIF par_ddl_operation IN (gc_bulk_delete, gc_bulk_delete2, gc_delete_from_cursor)
    THEN
      v_ddl_operation := gc_delete;
    ELSE
      v_ddl_operation := par_ddl_operation;
    END IF;
  
    v_footer := chr(10) || '  END ' || lower(v_ddl_operation) || '_record';
  
    IF par_ddl_operation IN (gc_bulk_insert_nested_table
                            ,gc_bulk_insert_table_index_by
                            ,gc_bulk_insert_nested_table2
                            ,gc_bulk_insert_table_index_by2
                            ,gc_bulk_delete
                            ,gc_bulk_delete2)
    THEN
      v_footer := v_footer || '_list';
    ELSIF par_ddl_operation IN (gc_insert_from_cursor, gc_delete_from_cursor)
    THEN
      v_footer := v_footer || '_from_cursor';
    END IF;
  
    RETURN v_footer || ';';
  
  END gen_procedure_footer;

  /*
    Байтин А.
    Формирование спецификации операции (INSERT, UPDATE, DELETE)
  */
  FUNCTION gen_procedure_spec(par_ddl_operation t_ddl_operation) RETURN CLOB IS
    v_spec           CLOB;
    v_parameter_list CLOB;
    v_procedure_text VARCHAR2(250);
  BEGIN
  
    dbms_lob.createtemporary(lob_loc => v_spec, cache => TRUE);
    IF par_ddl_operation = gc_update
    THEN
      v_parameter_list := '<#RECORD_PARAM> IN <#RECORD_TYPE>';
    ELSIF par_ddl_operation = gc_insert_record
    THEN
      v_parameter_list := '<#RECORD_PARAM> IN OUT <#RECORD_TYPE>';
    ELSIF par_ddl_operation = gc_bulk_insert_nested_table
    THEN
      v_parameter_list := '<#RECORD_PARAM_LIST> IN OUT NOCOPY <#NESTED_TABLE_TYPE_NAME>';
    ELSIF par_ddl_operation = gc_bulk_insert_table_index_by
    THEN
      v_parameter_list := '<#RECORD_PARAM_LIST> IN OUT NOCOPY <#ASSOC_ARRAY_TYPE_NAME>';
    ELSIF par_ddl_operation = gc_bulk_insert_nested_table2
    THEN
      v_parameter_list := '    <#RECORD_PARAM_LIST> IN OUT NOCOPY <#NESTED_TABLE_TYPE_NAME>
   ,<#BULK_DML_EXCEPTION_OCCURED_FLAG_PARAM> OUT BOOLEAN
   ,<#BULK_DML_EXCEPTION_LIST_PARAM> OUT ex.typ_bulk_ex_list_by_integer' ||
                          chr(10);
    ELSIF par_ddl_operation = gc_bulk_insert_table_index_by2
    THEN
      v_parameter_list := '    <#RECORD_PARAM_LIST> IN OUT NOCOPY <#ASSOC_ARRAY_TYPE_NAME>
   ,<#BULK_DML_EXCEPTION_OCCURED_FLAG_PARAM> OUT BOOLEAN
   ,<#BULK_DML_EXCEPTION_LIST_PARAM> OUT ex.typ_bulk_ex_list_by_integer' ||
                          chr(10);
    ELSIF par_ddl_operation = gc_insert_from_cursor
    THEN
      v_parameter_list := '<#CURSOR_PARAM> <#RECORD_CURSOR_PARAM_TYPE>, <#CURSOR_LIMIT_PARAM_NAME> INTEGER DEFAULT <#BULK_DML_DEFAULT_LIMIT>';
    ELSIF par_ddl_operation = gc_delete_from_cursor
    THEN
      v_parameter_list := '<#CURSOR_PARAM> <#PK_CURSOR_PARAM_TYPE>, <#CURSOR_LIMIT_PARAM_NAME> INTEGER DEFAULT <#BULK_DML_DEFAULT_LIMIT>';
    ELSIF par_ddl_operation = gc_delete
    THEN
      v_parameter_list := get_shorten_parameter(gv_primary_key_column) ||
                          ' IN <#TABLE_NAME>.<#PK_FIELD_NAME>%TYPE';
    ELSIF par_ddl_operation = gc_bulk_delete
    THEN
      v_parameter_list := '<#RECORD_PK_LIST> IN <#RECORD_PK_LIST_TYPE>';
    ELSIF par_ddl_operation = gc_bulk_delete2
    THEN
      v_parameter_list := '    <#RECORD_PK_LIST> IN <#RECORD_PK_LIST_TYPE>
   ,<#BULK_DML_EXCEPTION_OCCURED_FLAG_PARAM> OUT BOOLEAN
   ,<#BULK_DML_EXCEPTION_LIST_PARAM> OUT ex.typ_bulk_ex_list_by_integer' ||
                          chr(10);
    ELSE
      v_parameter_list := gen_parameter_list(par_ddl_operation => par_ddl_operation) || chr(10);
    END IF;
    parse_and_replace(v_parameter_list);
  
    v_procedure_text := gen_procedure_header(par_ddl_operation => par_ddl_operation);
  
    IF par_ddl_operation NOT IN (gc_insert_record
                                ,gc_update
                                ,gc_delete
                                ,gc_bulk_insert_nested_table
                                ,gc_bulk_insert_table_index_by
                                ,gc_insert_from_cursor
                                ,gc_bulk_delete
                                ,gc_delete_from_cursor)
    THEN
      v_procedure_text := v_procedure_text || chr(10) || '  (' || chr(10);
    ELSE
      v_procedure_text := v_procedure_text || '(';
    END IF;
  
    append_lob(par_lob => v_spec, par_buff => v_procedure_text);
  
    append_lob(par_lob => v_spec, par_buff => v_parameter_list);
  
    IF par_ddl_operation NOT IN (gc_insert_record
                                ,gc_update
                                ,gc_delete
                                ,gc_bulk_insert_nested_table
                                ,gc_bulk_insert_table_index_by
                                ,gc_insert_from_cursor
                                ,gc_bulk_delete
                                ,gc_delete_from_cursor)
    THEN
      v_procedure_text := '  )';
    ELSE
      v_procedure_text := ')';
    END IF;
    append_lob(par_lob => v_spec, par_buff => v_procedure_text);
  
    RETURN v_spec;
  END gen_procedure_spec;

  FUNCTION gen_dml_exception_handling RETURN VARCHAR2 IS
    v_text VARCHAR2(32767);
  BEGIN
  
    RETURN v_text;
  
  END gen_dml_exception_handling;

  FUNCTION gen_select_record RETURN VARCHAR2 IS
    v_function_body VARCHAR2(32767);
  BEGIN
    IF gv_table_list.count = 1
    THEN
      v_function_body := '      SELECT * INTO vr_record FROM <#TABLE_NAME> t1 WHERE ';
    ELSE
      v_function_body := '      SELECT ';
    
      FOR rec IN (SELECT lower(cl.column_name) AS column_name
                        ,row_number() over(ORDER BY t.parentness_level, cl.column_id) AS rn
                        ,MAX(length(cl.column_name)) over() + 1 AS max_length
                        ,t.parentness_level AS table_order_num
                    FROM TABLE(get_columns) cl
                        ,TABLE(get_tables) t
                   WHERE cl.table_name = t.table_name
                     AND (cl.is_primary_key = 0 OR t.parentness_level = 1)
                  --AND cl.column_name != 'ENT_ID'
                   ORDER BY t.parentness_level
                           ,cl.column_id)
      LOOP
        IF rec.rn = 1
        THEN
          v_function_body := v_function_body || 't' || rec.table_order_num || '.' || rec.column_name;
        
        ELSE
          v_function_body := v_function_body || chr(10) || '            ,t' || rec.table_order_num || '.' ||
                             rec.column_name;
        END IF;
      
      END LOOP;
    
      v_function_body := v_function_body || chr(10) || '        INTO vr_record';
      v_function_body := v_function_body || chr(10) || '        FROM ';
      FOR rec IN (SELECT lower(t.table_name) AS table_name
                        ,lower(t.owner) owner
                        ,row_number() over(ORDER BY t.parentness_level) AS rn
                    FROM TABLE(get_tables) t
                   ORDER BY t.parentness_level)
      LOOP
        IF rec.rn = 1
        THEN
          v_function_body := v_function_body || rec.table_name || ' t' || rec.rn;
        
        ELSE
          v_function_body := v_function_body || chr(10) || '            ,' || rec.table_name || ' t' ||
                             rec.rn;
        END IF;
      
      END LOOP;
    
      v_function_body := v_function_body || chr(10) || '       WHERE ';
    
      FOR rec IN (SELECT lower(t.table_name) AS table_name
                        ,row_number() over(ORDER BY t.parentness_level) AS rn
                        ,lower(cl.column_name) AS column_name
                        ,lead(t.table_name) over(ORDER BY t.parentness_level) next_table_name
                        ,lead(lower(cl.column_name)) over(ORDER BY t.parentness_level) next_column_name
                    FROM TABLE(get_tables) t
                        ,TABLE(get_columns) cl
                   WHERE t.table_name = cl.table_name
                     AND cl.is_primary_key = 1
                   ORDER BY t.parentness_level)
      LOOP
        IF rec.next_table_name IS NOT NULL
        THEN
        
          IF rec.rn = 1
          THEN
            v_function_body := v_function_body || 't' || rec.rn || '.' || rec.column_name || ' = t' ||
                               to_char(rec.rn + 1) || '.' || rec.next_column_name;
          
          ELSE
            v_function_body := v_function_body || chr(10) || '         AND t' || rec.rn || '.' ||
                               rec.column_name || ' = t' || to_char(rec.rn + 1) || '.' ||
                               rec.next_column_name;
          END IF;
        
        END IF;
      END LOOP;
    
      v_function_body := v_function_body || chr(10) || '         AND ';
    
    END IF;
    RETURN v_function_body;
  END gen_select_record;

  FUNCTION gen_get_row_function_spec RETURN VARCHAR2 IS
    v_function_text VARCHAR2(2000);
    v_param_name    t_object_name;
    v_rpad_length   INTEGER;
    c_lock_param_name CONSTANT t_object_name := gc_parameter_prefix || 'lock_record';
  BEGIN
    v_param_name := get_shorten_parameter(gv_primary_key_column);
  
    v_rpad_length   := greatest(length(v_param_name), length(c_lock_param_name));
    v_function_text := '
	FUNCTION get_record
  (
    ' || rpad(v_param_name, v_rpad_length, ' ') || ' IN <#TABLE_NAME>.<#PK_FIELD_NAME>%TYPE
   ,' || rpad(c_lock_param_name, v_rpad_length, ' ') || ' IN BOOLEAN DEFAULT FALSE
  ) RETURN ' || gc_record_type_prefix || '<#TABLE_NAME>';
  
    parse_and_replace(v_function_text);
  
    RETURN v_function_text;
  END gen_get_row_function_spec;

  FUNCTION gen_get_row_function_body RETURN VARCHAR2 IS
    v_function_text VARCHAR2(32767);
    v_function_body VARCHAR2(32767);
    c_lock_param_name CONSTANT t_object_name := gc_parameter_prefix || 'lock_record';
  BEGIN
    v_function_text := gen_get_row_function_spec;
    v_function_body := ' IS
    vr_record ' || gc_record_type_prefix || '<#TABLE_NAME>;
  BEGIN
		IF ' || c_lock_param_name || '
	  THEN' || chr(10) || gen_select_record || 't' ||
                       get_table_parent_level(gv_owner, gv_table_name) || '.<#PK_FIELD_NAME> = ' ||
                       get_shorten_parameter(gv_primary_key_column) || '
			 FOR UPDATE NOWAIT;
		ELSE' || chr(10) || gen_select_record || 't' ||
                       get_table_parent_level(gv_owner, gv_table_name) || '.<#PK_FIELD_NAME> = ' ||
                       get_shorten_parameter(gv_primary_key_column) || ';
		END IF;';
  
    v_function_body := v_function_body || chr(10) || '    RETURN vr_record;
  EXCEPTION
    WHEN no_data_found THEN
      RETURN NULL;
  END get_record';
  
    parse_and_replace(v_function_body);
  
    v_function_text := v_function_text || v_function_body;
  
    RETURN v_function_text;
  END gen_get_row_function_body;

  FUNCTION gen_lock_procedure_spec RETURN VARCHAR2 IS
    v_function_text VARCHAR2(4000);
  BEGIN
    v_function_text := '  PROCEDURE lock_record(' || get_shorten_parameter(gv_primary_key_column) ||
                       ' IN <#TABLE_NAME>.<#PK_FIELD_NAME>%TYPE)';
  
    parse_and_replace(v_function_text);
  
    RETURN v_function_text;
  END gen_lock_procedure_spec;

  FUNCTION gen_lock_procedure_body RETURN VARCHAR2 IS
    v_function_text VARCHAR2(32767);
    v_function_body VARCHAR2(32767);
  BEGIN
  
    v_function_body := ' IS
    v_dummy <#TABLE_NAME>.<#PK_FIELD_NAME>%TYPE;
  BEGIN';
  
    FOR rec IN (SELECT lower(t.table_name) AS table_name
                      ,lower(cl.column_name) AS column_name
                      ,lower(t.owner) AS owner
                  FROM TABLE(get_tables) t
                      ,TABLE(get_columns) cl
                 WHERE t.table_name = cl.table_name
                   AND cl.is_primary_key = 1
                 ORDER BY parentness_level DESC)
    LOOP
      v_function_body := v_function_body || chr(10) || chr(10) || '    SELECT ' || rec.column_name ||
                         ' INTO v_dummy FROM ' || rec.table_name || ' WHERE ' || rec.column_name ||
                         ' = ' || get_shorten_parameter(gv_primary_key_column) ||
                         ' FOR UPDATE NOWAIT;';
    END LOOP;
  
    v_function_body := v_function_body || chr(10) || '  END lock_record';
  
    parse_and_replace(v_function_body);
  
    v_function_text := gen_lock_procedure_spec || v_function_body;
  
    RETURN v_function_text;
  END gen_lock_procedure_body;

  FUNCTION gen_get_id_by_unique_spec(par_index_name user_indexes.index_name%TYPE) RETURN VARCHAR2 IS
    v_function_text VARCHAR2(4000);
    v_column_list   tt_simple_column_list;
  BEGIN
    v_column_list := get_column_list(par_index_name => par_index_name);
  
    v_function_text := chr(10) || '  FUNCTION id_by_' ||
                       get_shorten_variable_name(get_aggregated_column_list(v_column_list)
                                                ,gc_object_name_length - length('id_by_')) || '
  (';
  
    FOR i IN v_column_list.first .. v_column_list.last
    LOOP
      v_function_text := v_function_text || chr(10) || CASE i
                           WHEN 1 THEN
                            '    '
                           ELSE
                            '   ,'
                         END ||
                         rpad(get_shorten_parameter(v_column_list(i).column_name)
                             ,least(greatest(v_column_list(i)
                                             .max_column_name_length + gc_parameter_prefix_length
                                            ,gc_parameter_prefix_length + length('raise_on_error'))
                                   ,gc_object_name_length)
                             ,' ') || ' IN ' || v_column_list(i).table_name || '.' || v_column_list(i)
                        .column_name || '%TYPE';
    END LOOP;
    v_function_text := v_function_text || chr(10) || '   ,<#PAR_PREFIX>raise_on_error IN BOOLEAN DEFAULT TRUE
  ) RETURN <#TABLE_NAME>.<#PK_FIELD_NAME>%TYPE';
  
    parse_and_replace(v_function_text);
  
    RETURN v_function_text;
  END gen_get_id_by_unique_spec;

  FUNCTION gen_get_id_by_unique_body(par_index_name user_indexes.index_name%TYPE) RETURN VARCHAR2 IS
    v_function_text VARCHAR2(32767);
    v_body          VARCHAR2(32767);
    v_column_list   tt_simple_column_list;
  
    v_func_name t_object_name;
  BEGIN
    v_column_list := get_column_list(par_index_name => par_index_name);
  
    v_func_name := 'rec_by_' ||
                   get_shorten_variable_name(get_aggregated_column_list(v_column_list)
                                            ,gc_object_name_length - length('rec_by_'));
  
    v_body := ' IS
  BEGIN
	  RETURN ' || v_func_name || '(';
    FOR i IN v_column_list.first .. v_column_list.last
    LOOP
      v_body := v_body || get_shorten_parameter(v_column_list(i).column_name) ||
                CASE i
                  WHEN v_column_list.count THEN
                   '' --'    '
                  ELSE
                   ', '
                END;
    END LOOP;
    v_body := v_body || ', <#PAR_PREFIX>raise_on_error).<#PK_FIELD_NAME>;
  END ';
  
    v_body := v_body || 'id_by_' ||
              get_shorten_variable_name(get_aggregated_column_list(v_column_list)
                                       ,gc_object_name_length - length('id_by_'));
  
    parse_and_replace(v_body);
  
    v_function_text := gen_get_id_by_unique_spec(par_index_name => par_index_name) || v_body;
  
    RETURN v_function_text;
  END gen_get_id_by_unique_body;

  FUNCTION gen_get_rec_by_unique_spec(par_index_name user_indexes.index_name%TYPE) RETURN VARCHAR2 IS
    v_function_text VARCHAR2(32767);
    v_column_list   tt_simple_column_list;
  BEGIN
    v_column_list := get_column_list(par_index_name => par_index_name);
  
    v_function_text := chr(10) || '  FUNCTION rec_by_' ||
                       get_shorten_variable_name(get_aggregated_column_list(v_column_list)
                                                ,gc_object_name_length - length('rec_by_')) || '
  (';
  
    FOR i IN v_column_list.first .. v_column_list.last
    LOOP
      v_function_text := v_function_text || chr(10) || CASE i
                           WHEN 1 THEN
                            '    '
                           ELSE
                            '   ,'
                         END ||
                         rpad(get_shorten_parameter(v_column_list(i).column_name)
                             ,least(greatest(v_column_list(i)
                                             .max_column_name_length + gc_parameter_prefix_length
                                            ,gc_parameter_prefix_length + length('raise_on_error'))
                                   ,gc_object_name_length)
                             ,' ') || ' IN ' || v_column_list(i).table_name || '.' || v_column_list(i)
                        .column_name || '%TYPE';
    END LOOP;
    v_function_text := v_function_text || chr(10) || '   ,<#PAR_PREFIX>raise_on_error IN BOOLEAN DEFAULT TRUE
  ) RETURN ' || gc_record_type_prefix || '<#TABLE_NAME>';
  
    parse_and_replace(v_function_text);
  
    RETURN v_function_text;
  END gen_get_rec_by_unique_spec;

  FUNCTION gen_get_rec_by_unique_body(par_index_name user_indexes.index_name%TYPE) RETURN VARCHAR2 IS
    v_function_text  VARCHAR2(32767);
    v_body           VARCHAR2(32767);
    v_column_list    tt_simple_column_list;
    v_column_comment user_col_comments.comments%TYPE;
  BEGIN
    v_column_list := get_column_list(par_index_name => par_index_name);
  
    v_body := ' IS
    vr_record <#RECORD_TYPE>;
  BEGIN
    BEGIN
' || gen_select_record;
    FOR i IN v_column_list.first .. v_column_list.last
    LOOP
      v_body := v_body || CASE i
                  WHEN 1 THEN
                   NULL
                  ELSE
                   CASE gv_table_list.count
                     WHEN 1 THEN
                      ''
                     ELSE
                      chr(10) || '        '
                   END || ' AND '
                END || 't' ||
                get_table_parent_level(v_column_list(i).owner, v_column_list(i).table_name) || '.' || v_column_list(i)
               .column_name || ' = ' || get_shorten_parameter(v_column_list(i).column_name); -- SELECT * INTO v_record FROM <#TABLE_NAME> WHERE <#COLUMN_NAME> = <#PAR_PREFIX><#COLUMN_NAME>;
    END LOOP;
    v_body := v_body || q'{;
    EXCEPTION
      WHEN no_data_found THEN
        IF par_raise_on_error
        THEN
          RAISE;
        END IF;
    END;
    RETURN vr_record;
  END }';
  
    v_body := v_body || 'rec_by_' ||
              get_shorten_variable_name(get_aggregated_column_list(v_column_list)
                                       ,gc_object_name_length - length('rec_by_'));
  
    parse_and_replace(v_body);
  
    v_function_text := gen_get_rec_by_unique_spec(par_index_name => par_index_name) || v_body;
  
    RETURN v_function_text;
  END gen_get_rec_by_unique_body;

  FUNCTION gen_get_rec_by_rowid_spec RETURN VARCHAR2 IS
    v_function_text VARCHAR2(2000);
  BEGIN
  
    v_function_text := chr(10) || '  FUNCTION get_rec_by_rowid' || '
  (
    <#PAR_PREFIX>rowid IN UROWID
   ,<#PAR_PREFIX>raise_on_error IN BOOLEAN DEFAULT TRUE
  ) RETURN ' || gc_record_type_prefix || '<#TABLE_NAME>';
  
    parse_and_replace(v_function_text);
  
    RETURN v_function_text;
  END gen_get_rec_by_rowid_spec;

  FUNCTION gen_get_rec_by_rowid_body RETURN VARCHAR2 IS
    v_function_text VARCHAR2(32767);
    v_body          VARCHAR2(32767);
  BEGIN
  
    v_body := ' IS
    vr_record ' || gc_record_type_prefix || '<#TABLE_NAME>;
  BEGIN
    BEGIN
' || gen_select_record || 't' || get_table_parent_level(gv_owner, gv_table_name) ||
              '.rowid = <#PAR_PREFIX>rowid;' || -- SELECT * INTO v_record FROM <#TABLE_NAME> WHERE <#COLUMN_NAME> = <#PAR_PREFIX><#COLUMN_NAME>;
              q'{
    EXCEPTION
      WHEN no_data_found THEN
        IF par_raise_on_error
        THEN
          RAISE;
        END IF;
    END;
    RETURN vr_record;
  END get_rec_by_rowid}';
  
    parse_and_replace(v_body);
  
    v_function_text := gen_get_rec_by_rowid_spec() || v_body;
  
    RETURN v_function_text;
  END gen_get_rec_by_rowid_body;

  /*
    Формирование тела процедуры INSERT
  */
  FUNCTION gen_insert_body RETURN CLOB IS
    v_body                   CLOB;
    v_spec                   CLOB;
    v_header_text            VARCHAR2(1000);
    v_column_text            VARCHAR2(250);
    v_param_text             VARCHAR2(250);
    v_column_list            CLOB;
    v_param_list             CLOB;
    v_column_list_length     NUMBER;
    v_param_list_length      NUMBER;
    v_most_parent_table_name user_tables.table_name%TYPE;
  
    PROCEDURE check_sequence_exists(par_table_name user_tables.table_name%TYPE) IS
      v_exists NUMBER(1);
    BEGIN
      SELECT COUNT(*)
        INTO v_exists
        FROM dual
       WHERE EXISTS
       (SELECT NULL
                FROM user_objects uo
               WHERE uo.object_type = 'SEQUENCE'
               START WITH uo.object_name = upper(gc_sequence_prefix || par_table_name)
              CONNECT BY PRIOR uo.object_type = 'SYNONYM'
                     AND PRIOR
                          (SELECT table_name FROM user_synonyms us WHERE us.synonym_name = uo.object_name) =
                          uo.object_name);
      IF v_exists = 0
      THEN
        raise_application_error(-20101
                               ,'Sequence ' || upper(gc_sequence_prefix || par_table_name) ||
                                ' doesn''t exist');
      END IF;
    END;
  BEGIN
  
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
    dbms_lob.createtemporary(lob_loc => v_column_list, cache => TRUE);
    dbms_lob.createtemporary(lob_loc => v_param_list, cache => TRUE);
  
    v_spec := gen_procedure_spec(par_ddl_operation => gc_insert);
  
    append_lob(par_lob => v_body, par_buff => v_spec);
  
    SELECT MAX(table_name) keep(dense_rank FIRST ORDER BY parentness_level DESC)
      INTO v_most_parent_table_name
      FROM TABLE(get_tables);
  
    -- Свалимся на ошибке, если нет sequence для формирования PK
    check_sequence_exists(v_most_parent_table_name);
  
    -- Begin
    v_header_text := ' IS
  BEGIN
    SELECT ' || gc_sequence_prefix ||
                     '<#TABLE_NAME>.nextval INTO <#PAR_PREFIX><#PK_FIELD_NAME> FROM dual;' || chr(10);
  
    parse_and_replace(v_header_text, v_most_parent_table_name);
  
    append_lob(par_lob => v_body, par_buff => v_header_text);
  
    FOR rec_tab IN (SELECT t.*
                          ,row_number() over(ORDER BY parentness_level DESC) total_count_of_tables
                      FROM TABLE(get_tables) t
                     ORDER BY parentness_level DESC)
    LOOP
    
      v_header_text := chr(10) || '    INSERT INTO ' || rec_tab.table_name || chr(10);
    
      append_lob(par_lob => v_body, par_buff => v_header_text);
    
      v_column_list_length := dbms_lob.getlength(lob_loc => v_column_list);
      v_param_list_length  := dbms_lob.getlength(lob_loc => v_param_list);
    
      IF v_column_list_length > 0
      THEN
        dbms_lob.erase(lob_loc => v_column_list, amount => v_column_list_length);
        v_column_list := TRIM(v_column_list);
      END IF;
    
      IF v_param_list_length > 0
      THEN
        dbms_lob.erase(lob_loc => v_param_list, amount => v_param_list_length);
        v_param_list := TRIM(v_param_list);
      END IF;
    
      -- Перечень полей и параметров в INSERT
      FOR vr_columns IN (SELECT rownum AS rn
                               ,cl.*
                               ,COUNT(*) over() AS total_count
                           FROM (SELECT *
                                   FROM TABLE(tapi_codegen.get_columns) cl
                                  WHERE table_name = rec_tab.table_name
                                    AND owner = rec_tab.owner
                                 --AND cl.column_name != 'ENT_ID'
                                  ORDER BY is_primary_key DESC
                                          ,column_id) cl)
      LOOP
        -- Первое поле со скобкой
        IF vr_columns.rn = 1
        THEN
          v_column_text := '      (' || lower(vr_columns.column_name);
        
          v_param_text := '      (' || get_shorten_parameter(CASE
                                                               WHEN vr_columns.is_primary_key = 1 THEN
                                                                gv_primary_key_column
                                                               ELSE
                                                                vr_columns.column_name
                                                             END);
        ELSE
          -- Последующие с запятыми
          v_column_text := chr(10) || '      ,' || lower(vr_columns.column_name);
          v_param_text  := chr(10) || '      ,' || get_shorten_parameter(vr_columns.column_name);
        END IF;
        -- Последнее поле со скобкой
        IF vr_columns.rn = vr_columns.total_count
        THEN
          v_column_text := v_column_text || ')';
          v_param_text  := v_param_text || ');' || chr(10);
        END IF;
        -- Записываем
        append_lob(par_lob => v_column_list, par_buff => v_column_text);
        append_lob(par_lob => v_param_list, par_buff => v_param_text);
      END LOOP;
      -- Объединяем получившееся
      append_lob(par_lob => v_body, par_buff => v_column_list);
    
      append_lob(par_lob => v_body, par_buff => chr(10) || '    VALUES' || chr(10));
    
      append_lob(par_lob => v_body, par_buff => v_param_list);
    
    END LOOP;
  
    append_lob(par_lob => v_body, par_buff => gen_dml_exception_handling);
  
    append_lob(par_lob => v_body, par_buff => gen_procedure_footer(par_ddl_operation => gc_insert));
  
    dbms_lob.freetemporary(v_column_list);
    dbms_lob.freetemporary(v_param_list);
  
    RETURN v_body;
  END gen_insert_body;

  /*
    Формирование тела процедуры INSERT без выходного параметра
  */
  FUNCTION gen_insert_noout_body RETURN CLOB IS
    v_body                   CLOB;
    v_spec                   CLOB;
    v_header_text            VARCHAR2(250);
    v_assignment_text        VARCHAR2(250);
    v_column_list            CLOB;
    v_param_list             CLOB;
    v_max_column_name_length INTEGER;
  BEGIN
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
    dbms_lob.createtemporary(lob_loc => v_column_list, cache => TRUE);
    dbms_lob.createtemporary(lob_loc => v_param_list, cache => TRUE);
  
    v_spec := gen_procedure_spec(par_ddl_operation => gc_insert_noout);
  
    append_lob(par_lob => v_body, par_buff => v_spec);
  
    -- Begin
    v_header_text := ' IS
		v_id <#TABLE_NAME>.<#PK_FIELD_NAME>%TYPE;
	BEGIN
		insert_record(';
  
    parse_and_replace(v_header_text);
  
    append_lob(par_lob => v_body, par_buff => v_header_text);
  
    -- Перечень полей и параметров в UPDATE
    FOR vr_columns IN (SELECT row_number() over(ORDER BY decode(cl.nullable, 'N', 0, 1), decode(cl.data_default, NULL, 0, 1), t.parentness_level, cl.column_id) AS rn
                             ,lower(cl.column_name) AS column_name
                             ,COUNT(*) over() AS total_count
                             ,4 + greatest(MAX(length(cl.column_name)) over()
                                          ,length(gv_primary_key_column)) AS max_column_name_length
                         FROM TABLE(tapi_codegen.get_columns) cl
                             ,TABLE(get_tables) t
                        WHERE cl.is_primary_key = 0
                          AND t.table_name = cl.table_name
                       --AND cl.column_name != 'ENT_ID'
                        ORDER BY decode(cl.nullable, 'N', 0, 1)
                                ,decode(cl.data_default, NULL, 0, 1)
                                ,t.parentness_level
                                ,cl.column_id)
    LOOP
      -- Первое поле со скобкой
      v_assignment_text := rpad(get_shorten_parameter(vr_columns.column_name)
                               ,vr_columns.max_column_name_length
                               ,' ') || ' => <#PAR_PREFIX><#COLUMN_NAME>';
    
      parse_and_replace_col_template(v_assignment_text, vr_columns.column_name);
      parse_and_replace(v_assignment_text);
    
      v_max_column_name_length := vr_columns.max_column_name_length;
    
      IF vr_columns.rn > 1
      THEN
        -- Последующие с запятыми
        v_assignment_text := chr(10) || '                 ,' || v_assignment_text;
      END IF;
      -- Записываем
      append_lob(par_lob => v_body, par_buff => v_assignment_text);
    
      IF vr_columns.rn = vr_columns.total_count
      THEN
        v_assignment_text := chr(10) || '                 ,' ||
                             rpad(get_shorten_parameter(gv_primary_key_column)
                                 ,vr_columns.max_column_name_length
                                 ,' ') || ' => v_id);' || chr(10);
      
        append_lob(par_lob => v_body, par_buff => v_assignment_text);
      END IF;
    END LOOP;
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_footer(par_ddl_operation => gc_insert_noout));
  
    dbms_lob.freetemporary(v_column_list);
    dbms_lob.freetemporary(v_param_list);
  
    RETURN v_body;
  END gen_insert_noout_body;

  /*
    Капля П.С.
    Формирование тела процедуры INSERT record'а
  */
  FUNCTION gen_insert_record_body RETURN CLOB IS
    v_body            CLOB;
    v_spec            CLOB;
    v_header_text     VARCHAR2(250);
    v_assignment_text VARCHAR2(250);
    v_column_list     CLOB;
    v_param_list      CLOB;
  BEGIN
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
    dbms_lob.createtemporary(lob_loc => v_column_list, cache => TRUE);
    dbms_lob.createtemporary(lob_loc => v_param_list, cache => TRUE);
  
    v_spec := gen_procedure_spec(par_ddl_operation => gc_insert_record);
  
    append_lob(par_lob => v_body, par_buff => v_spec);
  
    -- Begin
    v_header_text := ' IS
  BEGIN
    insert_record(';
  
    parse_and_replace(v_header_text);
  
    append_lob(par_lob => v_body, par_buff => v_header_text);
  
    -- Перечень полей и параметров в UPDATE
    FOR vr_columns IN (SELECT row_number() over(ORDER BY cl.is_primary_key, decode(cl.nullable, 'N', 0, 1), decode(cl.data_default, NULL, 0, 1), t.parentness_level, cl.column_id) AS rn
                             ,lower(cl.column_name) AS column_name
                             ,COUNT(*) over() AS total_count
                             ,4 + greatest(MAX(length(cl.column_name)) over()
                                          ,length(gv_primary_key_column)) AS max_column_name_length
                         FROM TABLE(tapi_codegen.get_columns) cl
                             ,TABLE(get_tables) t
                        WHERE cl.table_name = t.table_name
                          AND (t.parentness_level = 1 OR cl.is_primary_key = 0)
                       --AND cl.column_name != 'ENT_ID'
                        ORDER BY cl.is_primary_key
                                ,decode(cl.nullable, 'N', 0, 1)
                                ,decode(cl.data_default, NULL, 0, 1)
                                ,t.parentness_level
                                ,cl.column_id)
    LOOP
    
      v_assignment_text := rpad(get_shorten_parameter(vr_columns.column_name)
                               ,vr_columns.max_column_name_length
                               ,' ') || ' => ' || gc_record_parameter || '.' || vr_columns.column_name;
      -- Первое поле со скобкой
      -- Последующие с запятыми
      IF vr_columns.rn > 1
      THEN
        v_assignment_text := chr(10) || '                 ,' || v_assignment_text;
      END IF;
      -- Записываем
      append_lob(par_lob => v_body, par_buff => v_assignment_text);
    END LOOP;
  
    append_lob(par_lob => v_body, par_buff => ');' || chr(10));
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_footer(par_ddl_operation => gc_insert_record));
  
    dbms_lob.freetemporary(v_column_list);
    dbms_lob.freetemporary(v_param_list);
  
    RETURN v_body;
  END gen_insert_record_body;

  FUNCTION gen_bulk_exception_handling RETURN CLOB IS
    v_exception_block CLOB;
  BEGIN
    dbms_lob.createtemporary(lob_loc => v_exception_block, cache => TRUE);
  
    RETURN v_exception_block;
  END gen_bulk_exception_handling;

  PROCEDURE gen_insert_record_list_body
  (
    par_body               IN OUT NOCOPY CLOB
   ,par_collect_exceptions BOOLEAN
  ) IS
  BEGIN
    append_lob(par_lob  => par_body
              ,par_buff => ' IS
    v_index PLS_INTEGER;
  BEGIN');
  
    IF par_collect_exceptions
    THEN
      append_lob(par_lob  => par_body
                ,par_buff => chr(10) || '    ' || gc_collect_bulk_ex_par_name || ' := FALSE;');
    END IF;
  
    append_lob(par_lob  => par_body
              ,par_buff => chr(10) || '    v_index := <#RECORD_PARAM_LIST>.first;

    WHILE v_index IS NOT NULL LOOP
      SELECT ' || gc_sequence_prefix ||
                           '<#TABLE_NAME>.nextval INTO <#RECORD_PARAM_LIST>(v_index).<#PK_FIELD_NAME> FROM dual;
      v_index := <#RECORD_PARAM_LIST>.next(v_index);
    END LOOP;

    FORALL i IN INDICES OF <#RECORD_PARAM_LIST>');
  
    IF par_collect_exceptions
    THEN
      append_lob(par_lob => par_body, par_buff => ' SAVE EXCEPTIONS');
    END IF;
  
    append_lob(par_lob  => par_body
              ,par_buff => chr(10) ||
                           '      INSERT INTO <#TABLE_NAME> VALUES <#RECORD_PARAM_LIST>(i);' ||
                           chr(10));
  
    parse_and_replace(par_body);
  
    IF par_collect_exceptions
    THEN
      append_lob(par_lob => par_body, par_buff => gen_bulk_exception_handling);
    END IF;
  END gen_insert_record_list_body;

  FUNCTION gen_bulk_ins_nest_table_body RETURN CLOB IS
    v_body CLOB;
    v_spec CLOB;
  BEGIN
    v_spec := gen_procedure_spec(par_ddl_operation => gc_bulk_insert_nested_table);
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
  
    append_lob(par_lob => v_body, par_buff => v_spec);
  
    gen_insert_record_list_body(v_body, FALSE);
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_footer(par_ddl_operation => gc_bulk_insert_nested_table));
  
    RETURN v_body;
  
  END gen_bulk_ins_nest_table_body;

  FUNCTION gen_bulk_ins_nest_table2_body RETURN CLOB IS
    v_body CLOB;
    v_spec CLOB;
  BEGIN
    v_spec := gen_procedure_spec(par_ddl_operation => gc_bulk_insert_nested_table2);
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
  
    append_lob(par_lob => v_body, par_buff => v_spec);
  
    gen_insert_record_list_body(v_body, TRUE);
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_footer(par_ddl_operation => gc_bulk_insert_nested_table2));
  
    RETURN v_body;
  
  END gen_bulk_ins_nest_table2_body;

  FUNCTION gen_bulk_insert_ass_array_body RETURN CLOB IS
    v_body CLOB;
    v_spec CLOB;
  BEGIN
    v_spec := gen_procedure_spec(par_ddl_operation => gc_bulk_insert_table_index_by);
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
  
    append_lob(par_lob => v_body, par_buff => v_spec);
  
    gen_insert_record_list_body(v_body, FALSE);
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_footer(par_ddl_operation => gc_bulk_insert_table_index_by));
  
    RETURN v_body;
  
  END gen_bulk_insert_ass_array_body;

  FUNCTION gen_bulk_ins_ass_array2_body RETURN CLOB IS
    v_body CLOB;
    v_spec CLOB;
  BEGIN
    v_spec := gen_procedure_spec(par_ddl_operation => gc_bulk_insert_table_index_by2);
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
  
    append_lob(par_lob => v_body, par_buff => v_spec);
  
    gen_insert_record_list_body(v_body, TRUE);
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_footer(par_ddl_operation => gc_bulk_insert_table_index_by2));
  
    RETURN v_body;
  
  END gen_bulk_ins_ass_array2_body;

  FUNCTION gen_insert_from_cursor_body RETURN CLOB IS
    v_body CLOB;
    v_spec CLOB;
  BEGIN
    v_spec := gen_procedure_spec(par_ddl_operation => gc_insert_from_cursor);
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
  
    append_lob(par_lob => v_body, par_buff => v_spec);
  
    append_lob(par_lob  => v_body
              ,par_buff => ' IS
    v_array ' || gc_nested_table_type_name || ';
  BEGIN
	  LOOP
			FETCH ' || gc_cursor_parameter_name || ' 
        BULK COLLECT INTO v_array LIMIT ' || gc_cursor_limit_parameter_name || ';

      insert_record_list(' || gc_record_list_parameter ||
                           ' => v_array);	
			
			EXIT WHEN v_array.COUNT < ' || gc_cursor_limit_parameter_name || ';
		END LOOP;');
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_footer(par_ddl_operation => gc_insert_from_cursor));
  
    RETURN v_body;
  
  END gen_insert_from_cursor_body;

  PROCEDURE gen_update_statement
  (
    par_table_name              VARCHAR2
   ,par_primary_key_column_name VARCHAR2
   ,par_body                    IN OUT NOCOPY CLOB
  ) IS
    v_assignment_text VARCHAR2(250);
    v_where_clause    VARCHAR2(1000);
  BEGIN
    append_lob(par_lob  => par_body
              ,par_buff => chr(10) || chr(10) || '    UPDATE ' || lower(par_table_name));
    -- Перечень полей и параметров в UPDATE
    FOR vr_columns IN (SELECT rownum AS rn
                             ,cl.*
                             ,COUNT(*) over() AS total_count
                             ,greatest(MAX(length(cl.column_name)) over()
                                      ,length(gv_primary_key_column)) AS max_column_name_length
                         FROM TABLE(tapi_codegen.get_columns) cl
                        WHERE cl.table_name = par_table_name
                          AND cl.is_primary_key = 0
                       --AND cl.column_name != 'ENT_ID'
                       )
    LOOP
      -- Первое поле со скобкой
      IF vr_columns.rn = 1
      THEN
        v_assignment_text := chr(10) || '      SET ' || rpad(lower(vr_columns.column_name)
                                                            ,vr_columns.max_column_name_length
                                                            ,' ') || ' = <#RECORD_FIELD>';
      ELSE
        -- Последующие с запятыми
        v_assignment_text := chr(10) || '         ,' || rpad(lower(vr_columns.column_name)
                                                            ,vr_columns.max_column_name_length
                                                            ,' ') || ' = <#RECORD_FIELD>';
      END IF;
    
      parse_and_replace_col_template(v_assignment_text, vr_columns.column_name, par_table_name);
    
      -- Записываем
      append_lob(par_lob => par_body, par_buff => v_assignment_text);
    END LOOP;
  
    v_where_clause := chr(10) || '    WHERE ' || lower(par_primary_key_column_name) ||
                      ' = <#RECORD_FIELD>;';
  
    parse_and_replace_col_template(v_where_clause, gv_primary_key_column);
  
    append_lob(par_lob => par_body, par_buff => v_where_clause);
  END gen_update_statement;

  /*
    Байтин А.
    
    Формирование тела процедуры UPDATE
  */
  FUNCTION gen_update_body RETURN CLOB IS
    v_assignment_text VARCHAR2(250);
    v_spec            CLOB;
    v_body            CLOB;
    v_where_clause    VARCHAR2(1000);
  BEGIN
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
  
    v_spec := gen_procedure_spec(par_ddl_operation => gc_update);
  
    append_lob(par_lob => v_body, par_buff => v_spec);
  
    append_lob(par_lob => v_body, par_buff => ' IS' || chr(10) || '  BEGIN');
  
    FOR vr_tables IN (SELECT t.*
                            ,row_number() over(ORDER BY parentness_level DESC) total_count_of_tables
                            ,(SELECT cl.column_name
                                FROM TABLE(get_columns) cl
                               WHERE cl.table_name = t.table_name
                                 AND cl.is_primary_key = 1) pr_key_column_name
                        FROM TABLE(get_tables) t
                       ORDER BY parentness_level DESC)
    LOOP
    
      gen_update_statement(par_table_name              => vr_tables.table_name
                          ,par_primary_key_column_name => vr_tables.pr_key_column_name
                          ,par_body                    => v_body);
    
    END LOOP;
  
    append_lob(par_lob => v_body, par_buff => chr(10) || gen_dml_exception_handling);
  
    append_lob(par_lob => v_body, par_buff => gen_procedure_footer(par_ddl_operation => gc_update));
  
    RETURN v_body;
  END gen_update_body;

  FUNCTION gen_update_table_spec(par_table_name VARCHAR2) RETURN VARCHAR2 IS
    v_function_spec VARCHAR2(32767);
  BEGIN
    v_function_spec := '  PROCEDURE update_' ||
                       get_shorten_variable_name(lower(par_table_name)
                                                ,gc_object_name_length - length('update_')) ||
                       '(<#RECORD_PARAM> IN ' || gc_record_type_prefix || lower(gv_table_name) || ')';
  
    parse_and_replace(v_function_spec, par_table_name);
  
    RETURN v_function_spec;
  END gen_update_table_spec;

  FUNCTION gen_update_table_body(par_table_name VARCHAR2) RETURN CLOB IS
    v_assignment_text    VARCHAR2(250);
    v_spec               CLOB;
    v_body               CLOB;
    v_where_clause       VARCHAR2(1000);
    v_primary_key_column t_object_name;
  BEGIN
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
  
    v_spec := gen_update_table_spec(par_table_name => par_table_name);
  
    append_lob(par_lob  => v_body
              ,par_buff => v_spec || ' IS
  BEGIN');
  
    SELECT cl.column_name
      INTO v_primary_key_column
      FROM TABLE(get_columns) cl
     WHERE cl.table_name = par_table_name
       AND cl.is_primary_key = 1;
  
    gen_update_statement(par_table_name              => par_table_name
                        ,par_primary_key_column_name => v_primary_key_column
                        ,par_body                    => v_body);
  
    append_lob(par_lob => v_body, par_buff => chr(10) || gen_dml_exception_handling);
  
    append_lob(par_lob => v_body, par_buff => chr(10) || ' 	END update_' || lower(par_table_name));
  
    RETURN v_body;
  END gen_update_table_body;

  PROCEDURE gen_delete_from_list
  (
    par_body               IN OUT NOCOPY CLOB
   ,par_collect_exceptions BOOLEAN
  ) IS
  
  BEGIN
  
    append_lob(par_lob  => par_body
              ,par_buff => ' IS
  BEGIN');
  
    IF par_collect_exceptions
    THEN
      append_lob(par_lob  => par_body
                ,par_buff => chr(10) || '    ' || gc_collect_bulk_ex_par_name || ' := FALSE;');
    END IF;
  
    FOR vr_tables IN (SELECT t.*
                            ,row_number() over(ORDER BY parentness_level DESC) total_count_of_tables
                            ,(SELECT cl.column_name
                                FROM TABLE(get_columns) cl
                               WHERE cl.table_name = t.table_name
                                 AND cl.is_primary_key = 1) pr_key_column_name
                        FROM TABLE(get_tables) t
                       ORDER BY parentness_level DESC)
    LOOP
      append_lob(par_lob  => par_body
                ,par_buff => chr(10) || '    FORALL i IN INDICES OF <#RECORD_PK_LIST>');
    
      parse_and_replace(par_body);
    
      IF par_collect_exceptions
      THEN
        append_lob(par_lob => par_body, par_buff => ' SAVE EXCEPTIONS');
      END IF;
    
      append_lob(par_lob  => par_body
                ,par_buff => chr(10) || '      DELETE FROM ' || lower(vr_tables.table_name) ||
                             ' WHERE ' || lower(vr_tables.pr_key_column_name) || ' = ' ||
                             gc_pk_list_parameter_name || '(i);' || chr(10));
    END LOOP;
  
    IF par_collect_exceptions
    THEN
      append_lob(par_lob => par_body, par_buff => gen_bulk_exception_handling);
    END IF;
  
  END gen_delete_from_list;

  FUNCTION gen_delete_from_list_body RETURN CLOB IS
    v_body CLOB;
  BEGIN
  
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
  
    append_lob(par_lob => v_body, par_buff => gen_procedure_spec(par_ddl_operation => gc_bulk_delete));
  
    gen_delete_from_list(v_body, FALSE);
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_footer(par_ddl_operation => gc_bulk_delete));
  
    RETURN v_body;
  
  END gen_delete_from_list_body;

  FUNCTION gen_delete_from_list_body2 RETURN CLOB IS
    v_body CLOB;
  BEGIN
  
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_spec(par_ddl_operation => gc_bulk_delete2));
  
    gen_delete_from_list(v_body, TRUE);
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_footer(par_ddl_operation => gc_bulk_delete2));
  
    RETURN v_body;
  
  END gen_delete_from_list_body2;

  FUNCTION gen_gelete_from_cursor RETURN CLOB IS
    v_body CLOB;
  BEGIN
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_spec(par_ddl_operation => gc_delete_from_cursor));
  
    append_lob(par_lob  => v_body
              ,par_buff => ' IS
    v_array ' || gc_pk_nested_table_type_name || ';
  BEGIN
	  LOOP
			FETCH ' || gc_cursor_parameter_name || ' 
        BULK COLLECT INTO v_array LIMIT ' || gc_cursor_limit_parameter_name || ';

      delete_record_list(' || gc_pk_list_parameter_name ||
                           ' => v_array);	
			
			EXIT WHEN v_array.COUNT < ' || gc_cursor_limit_parameter_name || ';
		END LOOP;');
  
    append_lob(par_lob  => v_body
              ,par_buff => gen_procedure_footer(par_ddl_operation => gc_delete_from_cursor));
  
    RETURN v_body;
  END gen_gelete_from_cursor;

  FUNCTION gen_delete_body RETURN CLOB IS
    v_body          CLOB;
    v_spec          CLOB;
    v_operator_text VARCHAR2(4000);
    v_table_name    user_tab_cols.table_name%TYPE;
    v_column_name   user_tab_cols.column_name%TYPE;
  
  BEGIN
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
  
    v_spec := gen_procedure_spec(par_ddl_operation => gc_delete);
  
    append_lob(par_lob => v_body, par_buff => v_spec);
  
    v_operator_text := ' IS
  BEGIN' || chr(10);
  
    FOR vr_tables IN (SELECT t.*
                            ,row_number() over(ORDER BY parentness_level DESC) total_count_of_tables
                            ,(SELECT cl.column_name
                                FROM TABLE(get_columns) cl
                               WHERE cl.table_name = t.table_name
                                 AND cl.is_primary_key = 1) pr_key_column_name
                        FROM TABLE(get_tables) t
                       ORDER BY parentness_level DESC)
    LOOP
      v_operator_text := v_operator_text || chr(10) || '    DELETE FROM ' ||
                         lower(vr_tables.table_name) || ' WHERE ' ||
                         lower(vr_tables.pr_key_column_name) || ' = ' ||
                         get_shorten_parameter(gv_primary_key_column) || ';';
    END LOOP;
  
    /*  
    SELECT lower(t.table_name)
          ,lower(cl.column_name)
      INTO v_table_name
          ,v_column_name
      FROM TABLE(get_tables) t
          ,TABLE(get_columns) cl
     WHERE t.table_name = cl.table_name
       AND cl.is_primary_key = 1
       AND t.parentness_level = (SELECT MAX(parentness_level) FROM TABLE(get_tables));
    
    v_operator_text := v_operator_text || chr(10) || '    DELETE FROM ' || v_table_name || ' WHERE ' ||
                       v_column_name || ' = <#PAR_PREFIX><#PK_FIELD_NAME>;';
                       
    
    parse_and_replace(v_operator_text);
    */
  
    v_operator_text := v_operator_text || chr(10) || gen_dml_exception_handling;
  
    v_operator_text := v_operator_text || gen_procedure_footer(par_ddl_operation => gc_delete);
  
    append_lob(par_lob => v_body, par_buff => v_operator_text);
    RETURN v_body;
  END gen_delete_body;

  FUNCTION gen_get_version_num_spec RETURN VARCHAR2 IS
  
    v_function_text VARCHAR2(4000);
  BEGIN
    v_function_text := '  FUNCTION get_version_id RETURN VARCHAR2';
  
    RETURN v_function_text;
  
  END gen_get_version_num_spec;

  FUNCTION gen_get_version_num_body RETURN VARCHAR2 IS
  
    v_function_text VARCHAR2(32767);
  BEGIN
    v_function_text := gen_get_version_num_spec || ' IS
	BEGIN
		RETURN ''' || gc_version || ''';
	END;';
  
    RETURN v_function_text;
  
  END gen_get_version_num_body;

  /*
    формирование текста заголовка процедуры вставки
    %auth Байтин А.
    %param par_table_name Название таблицы
    %return Текст заголовка процедуры вставки в таблицу par_table_name
  */
  FUNCTION gen_specification RETURN CLOB IS
    v_specification CLOB;
  BEGIN
    dbms_lob.createtemporary(lob_loc => v_specification, cache => TRUE);
    /* Заголовок пакета */
    append_lob(par_lob  => v_specification
              ,par_buff => 'CREATE OR REPLACE PACKAGE ' || lower(gv_owner) || '.' || gc_package_prefix ||
                           lower(gv_table_name) || gc_package_suffix || ' IS' || chr(10));
  
    append_lob(par_lob  => v_specification
              ,par_buff => chr(10) || gen_table_record_type || ';' || chr(10));
  
    append_lob(par_lob => v_specification, par_buff => gen_get_version_num_spec || ';' || chr(10));
    /* Генерация спецификаций процедур */
    -- Получение ID по полям с уникальностью
    IF gv_unique_indexes.count > 0
    THEN
      FOR v_idx IN gv_unique_indexes.first .. gv_unique_indexes.last
      LOOP
        append_lob(par_lob  => v_specification
                  ,par_buff => gen_get_id_by_unique_spec(par_index_name => gv_unique_indexes(v_idx)));
        append_lob(par_lob => v_specification, par_buff => ';' || chr(10));
      END LOOP;
    
      FOR v_idx IN gv_unique_indexes.first .. gv_unique_indexes.last
      LOOP
        append_lob(par_lob  => v_specification
                  ,par_buff => gen_get_rec_by_unique_spec(par_index_name => gv_unique_indexes(v_idx)));
        append_lob(par_lob => v_specification, par_buff => ';' || chr(10));
      END LOOP;
    END IF;
    -- Получение записи
    append_lob(par_lob => v_specification, par_buff => gen_get_row_function_spec);
    append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
  
    -- Получение записи по ROWID
    append_lob(par_lob => v_specification, par_buff => gen_get_rec_by_rowid_spec);
    append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
  
    -- Вставка без выходного параметра
    append_lob(par_lob  => v_specification
              ,par_buff => gen_procedure_spec(par_ddl_operation => gc_insert_noout));
    append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
  
    -- Вставка record'а
    append_lob(par_lob  => v_specification
              ,par_buff => gen_procedure_spec(par_ddl_operation => gc_insert_record));
    append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
  
    -- Вставка
    append_lob(par_lob  => v_specification
              ,par_buff => gen_procedure_spec(par_ddl_operation => gc_insert));
    append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
  
    IF gv_table_list.count = 1
    THEN
      -- Вставка record_list'а через Nested_table
      append_lob(par_lob  => v_specification
                ,par_buff => gen_procedure_spec(par_ddl_operation => gc_bulk_insert_nested_table));
      append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
      /*
      append_lob(par_lob  => v_specification
                ,par_buff => gen_procedure_spec(par_ddl_operation => gc_bulk_insert_nested_table2));
      append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
      */
    
      -- Вставка record_list'а через ассоциативный массив
      append_lob(par_lob  => v_specification
                ,par_buff => gen_procedure_spec(par_ddl_operation => gc_bulk_insert_table_index_by));
      append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
      /*
      append_lob(par_lob  => v_specification
                ,par_buff => gen_procedure_spec(par_ddl_operation => gc_bulk_insert_table_index_by2));
      append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
      */
      -- Вставка из курсора
      append_lob(par_lob  => v_specification
                ,par_buff => gen_procedure_spec(par_ddl_operation => gc_insert_from_cursor));
      append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
    END IF;
  
    -- Изменение
    append_lob(par_lob  => v_specification
              ,par_buff => gen_procedure_spec(par_ddl_operation => gc_update));
    append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
  
    -- Потабличное изменение
    IF gv_table_list.count > 1
    THEN
      FOR i IN gv_table_list.first .. gv_table_list.last
      LOOP
        append_lob(par_lob  => v_specification
                  ,par_buff => gen_update_table_spec(gv_table_list(i).table_name));
        append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
      END LOOP;
    END IF;
  
    -- Удаление
    append_lob(par_lob  => v_specification
              ,par_buff => gen_procedure_spec(par_ddl_operation => gc_delete));
    append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
  
    -- Удаление из списка первичных кючей
    append_lob(par_lob  => v_specification
              ,par_buff => gen_procedure_spec(par_ddl_operation => gc_bulk_delete));
    append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
  
    -- Удаление из списка первичных кючей с флагом SAVE EXCEPTIONS
    /*
    append_lob(par_lob  => v_specification
              ,par_buff => gen_procedure_spec(par_ddl_operation => gc_bulk_delete2));
    append_lob(par_lob => v_specification, par_buff => ';' || chr(10) || chr(10));
    */
  
    -- Заблокировать запись
    append_lob(par_lob => v_specification, par_buff => gen_lock_procedure_spec);
    append_lob(par_lob  => v_specification
              ,par_buff => ';' || chr(10) || 'END ' || gc_package_prefix || lower(gv_table_name) ||
                           gc_package_suffix || ';');
  
    RETURN v_specification;
  END gen_specification;

  /*
    Байтин А.
    Генерация тела пакета
  */
  FUNCTION gen_body RETURN CLOB IS
    v_body CLOB;
  BEGIN
    dbms_lob.createtemporary(lob_loc => v_body, cache => TRUE);
    /* Заголовок тела пакета */
    append_lob(par_lob  => v_body
              ,par_buff => 'CREATE OR REPLACE PACKAGE BODY ' || gv_owner || '.' || gc_package_prefix ||
                           lower(gv_table_name) || gc_package_suffix || ' IS' || chr(10));
    /* Генерация тел процедур */
  
    append_lob(par_lob => v_body, par_buff => gen_get_version_num_body || chr(10));
  
    -- Получение ID по полям с уникальностью
    IF gv_unique_indexes.count > 0
    THEN
      FOR v_idx IN gv_unique_indexes.first .. gv_unique_indexes.last
      LOOP
        append_lob(par_lob  => v_body
                  ,par_buff => gen_get_id_by_unique_body(par_index_name => gv_unique_indexes(v_idx)));
        append_lob(par_lob => v_body, par_buff => ';' || chr(10));
      END LOOP;
    
      FOR v_idx IN gv_unique_indexes.first .. gv_unique_indexes.last
      LOOP
        append_lob(par_lob  => v_body
                  ,par_buff => gen_get_rec_by_unique_body(par_index_name => gv_unique_indexes(v_idx)));
        append_lob(par_lob => v_body, par_buff => ';' || chr(10));
      END LOOP;
    END IF;
  
    -- Получение записи
    append_lob(par_lob => v_body, par_buff => gen_get_row_function_body);
    append_lob(par_lob => v_body, par_buff => ';' || chr(10) || chr(10));
  
    -- Получение записи по rowid
    append_lob(par_lob => v_body, par_buff => gen_get_rec_by_rowid_body);
    append_lob(par_lob => v_body, par_buff => ';' || chr(10) || chr(10));
  
    -- Вставка
    append_lob(par_lob => v_body, par_buff => gen_insert_noout_body);
    append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
  
    -- Вставка
    append_lob(par_lob => v_body, par_buff => gen_insert_record_body);
    append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
  
    -- Вставка
    append_lob(par_lob => v_body, par_buff => gen_insert_body);
    append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
  
    IF gv_table_list.count = 1
    THEN
      -- Вставка record_list'а через Nested_table
      append_lob(par_lob => v_body, par_buff => gen_bulk_ins_nest_table_body);
      append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
      /*
      append_lob(par_lob => v_body, par_buff => gen_bulk_ins_nest_table2_body);
      append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
      */
    
      -- Вставка record_list'а через ассоциативный массив
      append_lob(par_lob => v_body, par_buff => gen_bulk_insert_ass_array_body);
      append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
      /*
      append_lob(par_lob => v_body, par_buff => gen_bulk_ins_ass_array2_body);
      append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
      */
    
      -- Вставка из курсора
      append_lob(par_lob => v_body, par_buff => gen_insert_from_cursor_body);
      append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
    END IF;
  
    -- Изменение
    append_lob(par_lob => v_body, par_buff => gen_update_body);
    append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
  
    -- Потабличное изменение
    IF gv_table_list.count > 1
    THEN
      FOR i IN gv_table_list.first .. gv_table_list.last
      LOOP
        append_lob(par_lob => v_body, par_buff => gen_update_table_body(gv_table_list(i).table_name));
        append_lob(par_lob => v_body, par_buff => ';' || chr(10) || chr(10));
      END LOOP;
    END IF;
  
    -- Удаление
    append_lob(par_lob => v_body, par_buff => gen_delete_body);
    append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
  
    -- Удаление из списка первичных кючей
    append_lob(par_lob => v_body, par_buff => gen_delete_from_list_body);
    append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
  
    -- Удаление из списка первичных кючей с флагом SAVE EXCEPTIONS
    /*
    append_lob(par_lob => v_body, par_buff => gen_delete_from_list_body2);
    append_lob(par_lob => v_body, par_buff => chr(10) || chr(10));
    */
  
    -- Заблокировать запись
    append_lob(par_lob => v_body, par_buff => gen_lock_procedure_body);
    append_lob(par_lob  => v_body
              ,par_buff => ';' || chr(10) || 'END ' || gc_package_prefix || lower(gv_table_name) ||
                           gc_package_suffix || ';');
  
    RETURN v_body;
  END gen_body;

  FUNCTION gen_package_for_table
  (
    par_owner      VARCHAR2 DEFAULT USER
   ,par_table_name VARCHAR2
  ) RETURN CLOB IS
    v_specification CLOB;
    v_body          CLOB;
    v_package       CLOB;
    c_end_statement CONSTANT VARCHAR2(10) := chr(10) || '/' || chr(10);
  BEGIN
    init(par_owner, par_table_name);
  
    dbms_lob.createtemporary(lob_loc => v_package, cache => TRUE);
  
    v_specification := gen_specification;
    v_body          := gen_body;
  
    append_lob(par_lob => v_package, par_buff => v_specification);
  
    append_lob(par_lob => v_package, par_buff => c_end_statement);
  
    append_lob(par_lob => v_package, par_buff => v_body);
  
    append_lob(par_lob => v_package, par_buff => c_end_statement);
  
    RETURN v_package;
  END gen_package_for_table;

  PROCEDURE generate_and_compile
  (
    par_owner      VARCHAR2 DEFAULT USER
   ,par_table_name VARCHAR2
  ) IS
    v_pkg_spec CLOB;
    v_pkg_body CLOB;
    c1         INTEGER;
    c2         INTEGER;
    i1         INTEGER;
    i2         INTEGER;
  
    v_upperbound INTEGER;
    v_amount     INTEGER := 32767;
  
    v_spec_sql dbms_sql.varchar2a;
    v_body_sql dbms_sql.varchar2a;
  
  BEGIN
    init(par_owner, par_table_name);
  
    v_pkg_spec := gen_specification;
    v_pkg_body := gen_body;
  
    BEGIN
      c1 := dbms_sql.open_cursor;
      c2 := dbms_sql.open_cursor;
    
      v_upperbound := CEIL(dbms_lob.getlength(v_pkg_spec) / v_amount);
      FOR i IN 1 .. v_upperbound
      LOOP
        v_spec_sql(i) := dbms_lob.substr(v_pkg_spec, v_amount, ((i - 1) * v_amount) + 1);
      END LOOP;
      dbms_sql.parse(c             => c1
                    ,STATEMENT     => v_spec_sql
                    ,lb            => 1
                    ,ub            => v_upperbound
                    ,lfflg         => FALSE
                    ,language_flag => dbms_sql.native);
    
      v_upperbound := CEIL(dbms_lob.getlength(v_pkg_body) / v_amount);
      FOR i IN 1 .. v_upperbound
      LOOP
        v_body_sql(i) := dbms_lob.substr(v_pkg_body, v_amount, ((i - 1) * v_amount) + 1);
      END LOOP;
      dbms_sql.parse(c             => c2
                    ,STATEMENT     => v_body_sql
                    ,lb            => 1
                    ,ub            => v_upperbound
                    ,lfflg         => FALSE
                    ,language_flag => dbms_sql.native);
    
      i1 := dbms_sql.execute(c1);
      i2 := dbms_sql.execute(c2);
    
      dbms_sql.close_cursor(c1);
      dbms_sql.close_cursor(c2);
    EXCEPTION
      WHEN OTHERS THEN
      
        IF dbms_sql.is_open(c1)
        THEN
          dbms_sql.close_cursor(c1);
        END IF;
        IF dbms_sql.is_open(c2)
        THEN
          dbms_sql.close_cursor(c2);
        END IF;
      
        RAISE;
    END;
  END generate_and_compile;

  PROCEDURE generate_and_compile_all IS
  
  BEGIN
    FOR rec IN (SELECT t.object_name
                      ,substr(t.object_name, 5) AS table_name
                      ,t.owner
                  FROM sys.dba_objects t
                 WHERE t.object_type = 'PACKAGE'
                   AND t.object_name LIKE upper(gc_package_prefix) ||
                       '%'
                   AND EXISTS (SELECT NULL
                          FROM sys.dba_tables u
                         WHERE u.table_name = substr(t.object_name, length(gc_package_prefix) + 1)
                           AND t.owner = t.owner))
    LOOP
      generate_and_compile(par_owner => rec.owner, par_table_name => rec.table_name);
    END LOOP;
  END generate_and_compile_all;

BEGIN
  SELECT sys_context('userenv', 'current_schema') INTO gv_current_schema FROM dual;
END tapi_codegen;
/

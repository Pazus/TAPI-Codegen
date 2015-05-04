BEGIN
  FOR rec IN (SELECT t.table_name FROM user_tables t WHERE t.table_name = 'TAPI_TEST_1')
  LOOP
    EXECUTE IMMEDIATE 'drop table ' || rec.table_name;
  END LOOP;

  FOR rec IN (SELECT t.sequence_name FROM user_sequences t WHERE t.sequence_name = 'SQ_TAPI_TEST_1')
  LOOP
    EXECUTE IMMEDIATE 'drop sequence ' || rec.sequence_name;
  END LOOP;

END;
/

CREATE TABLE tapi_test_1(f1 NUMBER
                        ,f2 NUMBER DEFAULT NULL
                        ,f3 NUMBER NOT NULL
                        ,f4 NUMBER DEFAULT 0 NOT NULL);
-- Create/Recreate primary, unique and foreign key constraints 
ALTER TABLE tapi_test_1 add CONSTRAINT pk_tapi_test_1 primary key(f1);

ALTER TABLE tapi_test_1 add CONSTRAINT uk_tapi_test_1 UNIQUE(f2);

grant
  SELECT ON tapi_test_1 TO plpdf;

CREATE sequence sq_tapi_test_1;

begin
	tapi_codegen.generate_and_compile(par_owner => USER,par_table_name => 'TAPI_TEST_1');
	--look fro DML_TAPI_TEST_1 package
end;
/

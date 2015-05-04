# TAPI-Codegen
PL/SQL Table API Generator

Package to generate the rich Table API for Oracle.

## Key features:
  * DML operation procedures
  * Data retrieval funcions for the whole table record or ID by ROWID, primary key or any nonfunctional unique indexes
  * Bulk DML operations
  * Insert from cursor
  * Record type for the table with default values
  * Dynamic primary key generation from sequence
  
## Example of usage
To generate API package for a table execute:

		:c := tapi_codegen.gen_package_for_table(par_owner => USER, par_table_name => 'TAPI_TEST_1');

Package code is returned as CLOB (:c).

You can generate and compile the package automaticaly by executing

		tapi_codegen.generate_and_compile(par_owner => USER, par_table_name => 'TAPI_TEST_1');
		
## TODO:
  * Bulk DML api with exception handling
  * Multilevel entities with primary key as foreign key
  * Custom exception handling block
  * Cursors for retrieval by columns with foreign key

## Credits
The authors of TAPI-Codegen are
* Pavel Kaplya ([Pazus](http://github.com/pazus))
* Artur Baytin
 
## License
See the ([LICENSE](https://github.com/Pazus/TAPI-Codegen/blob/master/LICENSE)) file for license rights and limitations .

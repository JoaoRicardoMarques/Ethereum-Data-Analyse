import duckdb


class contract:
    def create(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)     
        a="id BIGINT PRIMARY KEY"
        b="address VARCHAR"
        c="bytecode VARCHAR"
        d="block_number BIGINT"
        e="FOREIGN KEY (id) REFERENCES transactions(id)"
        con.sql(f"CREATE TABLE contracts({a},{b},{c},{d},{e})")
        con.sql(f"CREATE SEQUENCE contract_id START 0 MINVALUE 0")
        con.close()

    def drop(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql("DROP TABLE IF EXISTS contracts")
        con.close()

    def insert(duckPath,arquivePath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql(f"INSERT INTO contracts SELECT nextval('contract_id'), * FROM read_csv_auto('{arquivePath}')")
        con.close()

    def delete(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql(f"DELETE FROM contracts")
        con.close()
   
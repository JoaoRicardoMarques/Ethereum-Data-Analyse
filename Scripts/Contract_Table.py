import duckdb
from Contractor import contractor

class contract:
    def create(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)     
        a="id BIGINT PRIMARY KEY"
        b="address VARCHAR"
        c="bytecode VARCHAR"
        d="function_sighashes VARCHAR"
        e="is_erc20 BOOLEAN"
        f="is_erc721 BOOLEAN"
        g="block_number BIGINT"
        h="FOREIGN KEY (block_number) REFERENCES blocks(number)"
        con.sql(f"CREATE TABLE contracts({a},{b},{c},{d},{e},{f},{g},{h})")
        con.close()

    def drop(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql("DROP TABLE IF EXISTS contracts")
        con.close()
    def insert(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        
        con.close()
    def delete(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql(f"DELETE FROM contracts")
        con.close
   
import duckdb
from Contractor import contractor

class contract:
    def create(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)     
        a="id BIGINT PRIMARY KEY"
        b="id_transactions BIGINT"
        c="address VARCHAR"
        d="bytecode VARCHAR"
        e="function_sighashes VARCHAR"
        f="is_erc20 BOOLEAN"
        g="is_erc721 BOOLEAN"
        h="block_number BIGINT"
        i="FOREIGN KEY (id_transactions) REFERENCES transactions(id)"
        con.sql(f"CREATE TABLE contracts({a},{b},{c},{d},{e},{f},{g},{h})")
        con.sql(f"CREATE SEQUENCE contract_id")
        con.close()

    def drop(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql("DROP TABLE IF EXISTS contracts")
        con.close()

    def insert(duckPath,arquivePath):
        con=duckdb.connect(database=duckPath,read_only=False)
        contractor.contractor(duckPath)
        con.sql(f"INSERT INTO contracts SELECT nextval('contract_id'), * FROM read_csv_auto('{arquivePath}')")
        con.close()

    def delete(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql(f"DELETE FROM contracts")
        con.close()
   
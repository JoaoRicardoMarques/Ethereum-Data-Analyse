import duckdb
class transactions:
    def create(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)

        a = 'id bigint primary key'
        b = 'hash VARCHAR'
        c = 'nonce BIGINT'
        d = 'block_hash VARCHAR'
        e = 'block_number BIGINT'
        f = 'foreign key (block_number) references blocks(number)'
        g = 'transaction_index BIGINT'
        h = 'from_address VARCHAR'
        i = 'to_address VARCHAR'
        j = 'value VARCHAR'
        k = 'gas BIGINT'
        l = 'gas_price BIGINT'
        m = 'input VARCHAR'
        n = 'block_timestamp BIGINT'
        o = 'max_fee_per_gas BIGINT'
        p = 'max_priority_fee_per_gas BIGINT'
        q = 'transaction_type BIGINT'

        con.sql(f"CREATE TABLE transactions({a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o},{p},{q})")
        con.sql("CREATE SEQUENCE transactions_id START 0 MINVALUE 0")
        con.close()

    def drop(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql("DROP TABLE IF EXISTS transactions")
        con.sql("DROP SEQUENCE transactions_id")
        con.close()

    def insert(duckPath, arquivePath):
        con = duckdb.connect(database=duckPath, read_only=False)      
        con.sql(f"INSERT INTO transactions SELECT nextval('transactions_id'), * FROM read_csv_auto('{arquivePath}') WHERE to_address IS NULL AND input != '0x'")
        con.close()

    def delete(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql(f"DELETE FROM transactions")
        con.close()

    def show(duckPath,interval1,interval2):
        con=duckdb.connect(database=duckPath,read_only=False)
        result=con.sql(f"SELECT * FROM transactions").fetchall()
        for i in result:
            print(i)
        con.close()
    
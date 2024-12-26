import duckdb
import os

class blocks:
    def create(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        
        a= 'number bigint primary key'
        b= 'hash varchar'
        c= 'parent_hash varchar'
        d= 'nonce varchar'
        e= 'sha3_uncles varchar'
        f= 'logs_bloom varchar'
        g= 'transactions_root varchar'
        h= 'state_root varchar'
        i= 'receipts_root varchar'
        j= 'miner varchar'
        k= 'difficulty varchar'
        l= 'total_difficulty varchar'
        m= 'size bigint'
        n= 'extra_data varchar'
        o= 'gas_limit bigint'
        p= "gas_used bigint"
        q= 'timestamp bigint'
        r= 'transaction_count bigint'
        s= 'base_fee_per_gas bigint'
        t= 'withdrawals_root varchar'
        u= 'withdrawals varchar'
        
        con.sql(f"CREATE TABLE blocks({a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o},{p},{q},{r},{s},{t},{u})")
        con.close()

    def drop(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql("DROP TABLE IF EXISTS blocks")
        con.close()

    def insert(duckPath, arquivePath1, arquivePath2):
        con = duckdb.connect(database=":memory:", read_only=False)
        blocks = con.sql(f"SELECT DISTINCT block_number FROM read_csv_auto('{arquivePath2}') WHERE to_address IS NULL AND input != '0x'").fetchall()
        con.close()

        con = duckdb.connect(database=duckPath,read_only=False)
        for block in blocks:
            tuple = con.sql(f"SELECT * FROM read_csv_auto('{arquivePath1}') WHERE number = {block[0]}").fetchone()
            con.sql(f"INSERT INTO blocks VALUES ({','.join([repr(value) if value is not None else 'NULL' for value in  tuple])})")

    def delete(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql(f"DELETE FROM blocks")
        con.close()

    def show(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        result=con.sql(f"SELECT number FROM blocks").fetchall()
        for i in result:
            print(i)
        con.close()

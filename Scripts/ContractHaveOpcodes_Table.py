import duckdb
from Disassembler import disassembler

class contracts_have_opcodes:
    def create(duckpath):
        con=duckdb.connect(database=duckpath, read_only=False)
        a='id_contract BIGINT'
        b='id_opcodes BIGINT'
        c='Qntd INT'
        d='FOREIGN KEY(id_contract) REFERENCES contracts(id)'
        e='FOREIGN KEY(id_opcodes) REFERENCES opcodes(id)'
        con.sql(f"CREATE TABLE contracts_have_opcodes({a},{b},{c},{d},{e})")
        con.close()
    def drop(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql("DROP TABLE IF EXISTS contracts_have_opcodes")
        con.close()
    def insert(duckPath):
        disassembler.disassembler(duckPath)
    def delete(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql("DELETE FROM contracts_have_opcodes")
        con.close()
    def show(duckPath):
        con=duckdb.connect(database=duckPath,read_only=True)
        result=con.sql("SELECT * FROM contracts_have_opcodes")
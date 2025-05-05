import duckdb
from disassembler import disasm

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
        con=duckdb.connect(database=duckPath,read_only=False)
        amount=con.sql("SELECT COUNT(*) FROM contracts").fetchone()[0]
        for i in range(0,amount):
            bytecode = con.sql(f"SELECT bytecode FROM contracts WHERE id = {i}").fetchone()[0]
            if bytecode == None:
                continue
            else:
                if bytecode.startswith("0x"):
                    disasm(i,bytecode,duckPath)
                else:
                    prefixe = "0x"
                    bytecode = prefixe+bytecode
                    disasm(i,bytecode,duckPath)

        disasm.disassembler(duckPath)
    def delete(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql("DELETE FROM contracts_have_opcodes")
        con.close()
    def show(duckPath):
        con=duckdb.connect(database=duckPath,read_only=True)
        results=con.sql("SELECT * FROM contracts_have_opcodes")
        for result in results:
            print(result)
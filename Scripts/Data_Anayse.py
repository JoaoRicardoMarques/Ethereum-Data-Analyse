import duckdb
from list import list

duckPath = "/home/joao/Ethereum-Data-Analyse/Database/blocks-0-399999.db"

con = duckdb.connect(database=duckPath,read_only=False)

con.sql("CREATE TABLE IF NOT EXISTS opcode_analyse (opcode_id BIGINT, opcode_name VARCHAR, opcode_amount BIGINT)")

instructions = list.list()

for instruction in instructions:
    result = con.sql(f"SELECT SUM(qntd) FROM contracts_have_opcodes WHERE id_opcodes={instruction[0]}").fetchone()
    amount = result[0] if result[0] is not None else 0
    con.sql(f"INSERT INTO opcode_analyse (opcode_id, opcode_name, opcode_amount) VALUES ('{instruction[0]}','{instruction[1]}',{amount})")
con.close()


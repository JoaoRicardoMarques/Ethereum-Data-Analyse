import duckdb

def table(duckPath,initialBlock,finalBlock):
    con=duckdb.connect(database=duckPath,read_only=False)
    con.sql(f"CREATE TABLE analyse-of-blocks-{initialBlock}-{finalBlock} (opcode VARCHAR PRIMARY KEY, ocorrences BIGINT) ")
    con.close()

def count(duckPath,dataPath):
    con=duckdb.connect(database=dataPath,read_only=True)
    opcodes = con.sql("SELECT name FROM opcodes")

    con.close()
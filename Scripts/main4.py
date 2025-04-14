import duckdb
import pandas as pd
import matplotlib.pyplot as plt

duckPath = "/home/joao/Ethereum-Data-Analyse/Database/blocks-400000-799999.db"
argument = "SELECT COUNT(*) FROM opcodes"

con = duckdb.connect(database=duckPath, read_only=True)
result=con.sql(f"{argument}").fetchone()[0]
print(result)
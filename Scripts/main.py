from Block_Table import blocks
from Transaction_Table import transactions
from Contract_Table import contract

duckPath = "Database/blocks-2800000-3199999.db"
apiPath = "https://mainnet.infura.io/v3/a1396b66469549e8af981d99a0316269"

initial = 3200000  
final = 3209999    


for i in range(40):
    blocksPaths = f"Data/Blocks/blocks-000{initial}-000{final}.csv"
    transactionsPaths = f"Data/Transactions/transactions-of-blocks-000{initial}-000{final}.csv"

    print(f"Insert blocks {initial} - {final}")
    blocks.insert(duckPath, blocksPaths, transactionsPaths)

    print(f"Insert transactions {initial} - {final}")
    transactions.insert(duckPath, transactionsPaths)

    initial += 10000
    final += 10000


   
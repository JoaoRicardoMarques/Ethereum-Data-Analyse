from Block_Table import blocks
from Transaction_Table import transactions
from Contract_Table import contract
from Opcodes_Table import opcodes
from ContractHaveOpcodes_Table import contracts_have_opcodes

initial = 50000
final = initial + 9999
duckPath = f"~/Ethereum Data Analyse/Database/database-{initial}-{initial+49999}.db"

blocks.create(duckPath)
transactions.create(duckPath)
#contract.create(duckPath)
#opcodes.create(duckPath)
#contracts_have_opcodes.create(duckPath)

for i in range(5):
    blockPath=f"/home/joao/Ethereum Data Analyse/Data/Blocks/blocks-00000{initial}-00000{final}.csv"
    transactionPath=f"/home/joao/Ethereum Data Analyse/Data/Transactions/transactions-of-blocks-00000{initial}-00000{final}.csv"

    blocks.insert(duckPath,blockPath)
    transactions.insert(duckPath,transactionPath)
    #contract.insert(duckPath,contractPath)
    #opcodes.insert(duckPath)
    #contracts_have_opcodes.insert(duckPath)
    
    initial+=10000
    final+=10000




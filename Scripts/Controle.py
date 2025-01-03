from Block_Table import blocks
from Transaction_Table import transactions
from Contract_Table import contract
from Opcodes_Table import opcodes
from ContractHaveOpcodes_Table import contracts_have_opcodes

initial = 50000
final = initial + 9999
duckPath = f"~/Ethereum-Data-Analyse/Database/database-{initial}-{initial+49999}.db"
apiPath="https://mainnet.infura.io/v3/a1396b66469549e8af981d99a0316269"

blocks.create(duckPath)
transactions.create(duckPath)
contract.create(duckPath)
opcodes.create(duckPath)
contracts_have_opcodes.create(duckPath)

for i in range(5):
    blockPath=f"~/Ethereum-Data-Analyse/Data/Blocks/blocks-00000{initial}-00000{final}.csv"
    transactionPath=f"~/Ethereum-Data-Analyse/Data/Transactions/transactions-of-blocks-00000{initial}-00000{final}.csv"

    blocks.teste2(duckPath,blockPath,transactionPath)
    transactions.insert(duckPath,transactionPath)
    
    initial+=10000
    final+=10000

contract.insert(duckPath,apiPath)
opcodes.insert(duckPath)
contracts_have_opcodes.insert(duckPath)


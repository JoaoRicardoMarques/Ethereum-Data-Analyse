from Block_Table import blocks
from Transaction_Table import transactions
from Contract_Table import contract
from Opcodes_Table import opcodes
from ContractHaveOpcodes_Table import contracts_have_opcodes

database = input("nome da database:")

initial = int(input("bloco inicial:"))
final = initial + 9999

duckPath = f'/home/joao/Ethereum-Data-Analyse/Database/{database}.db'
apiPath="https://mainnet.infura.io/v3/a1396b66469549e8af981d99a0316269"

blocks.create(duckPath)
transactions.create(duckPath)
contract.create(duckPath)
opcodes.create(duckPath)
contracts_have_opcodes.create(duckPath)

for i in range(40):
    if initial < 100000:
        if initial == 0:
            blockPath=f"~/Ethereum-Data-Analyse/Data/Blocks/blocks-0000000000-0000009999.csv"
            transactionPath=f"~/Ethereum-Data-Analyse/Data/Transactions/transactions-of-blocks-0000000000-0000009999.csv"

            blocks.insert(duckPath,blockPath,transactionPath)
            transactions.insert(duckPath,transactionPath)

            initial+=10000
            final+=10000
        else:
            blockPath=f"~/Ethereum-Data-Analyse/Data/Blocks/blocks-00000{initial}-00000{final}.csv"
            transactionPath=f"~/Ethereum-Data-Analyse/Data/Transactions/transactions-of-blocks-00000{initial}-00000{final}.csv"

            blocks.insert(duckPath,blockPath,transactionPath)
            transactions.insert(duckPath,transactionPath)

            initial+=10000
            final+=10000

    if initial < 1000000 and initial > 99999:
        blockPath=f"~/Ethereum-Data-Analyse/Data/Blocks/blocks-0000{initial}-0000{final}.csv"
        transactionPath=f"~/Ethereum-Data-Analyse/Data/Transactions/transactions-of-blocks-0000{initial}-0000{final}.csv"

        blocks.insert(duckPath,blockPath,transactionPath)
        transactions.insert(duckPath,transactionPath)

        initial+=10000
        final+=10000

contract.insert(duckPath,apiPath)
opcodes.insert(duckPath)
contracts_have_opcodes.insert(duckPath)
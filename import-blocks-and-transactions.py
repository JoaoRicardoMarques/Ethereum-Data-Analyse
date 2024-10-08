#!/usr/bin/python3

import duckdb

con = duckdb.connect('ethereum-data-contracts.db')



CREATE TABLE blocks AS SELECT * FROM read_csv_auto(['blocks-10000-19999.csv', 'blocks-20000-29999.csv']);

COPY blocks FROM 'blocks-30000-39999.csv' (DELIMITER ',', HEADER);


# create a table and load data into it
con.sql('CREATE TABLE test(i INTEGER)')
con.sql('INSERT INTO test VALUES (42)')
# query the table
con.table('test').show()
# explicitly close the connection
con.close()
# Note: connections also closed implicitly when they go out of scope


if __name__ == "__main__":
    if(len(sys.argv) < 3):
        raise TypeError("Use: python export-blocks-and-transactions.py initialblocknumber lastblocknumber loopstep")

    initial_block = int(argv[1])
    last_block = int(argv[2])
    step = int(argv[3])

    log_file = open(os.path.join(os.getcwd(), "log-{}-{}-{}.txt".format(initial_block, last_block, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))), 'w')

    for i in range(initial_block, last_block, step) :
        # print(i, i+step-1)
        start = i
        end = start+step-1
        print(colored("Getting blocks from {} to {}.".format(start, end), 'green'))
        check = check_blocks(start, end)
        print(check)
        if(not check):
            request = export_blocks_and_transactions(1,start,end,log_file)
            if (not request):
                break

    log_file.close()

    print(colored("End of Test Execution.",'green'))

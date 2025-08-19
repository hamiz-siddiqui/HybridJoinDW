This project is the ETL implementation of HybridJoin

Coded in Java using Eclipse, import into that to avoid unnecessary behavioural issues

Steps to execute:

1. Simply load up your java database connector and apache common collections 4 into modulepath if not already done
2. Once that is done, simply run the project
3. If needed, run the SQL files regarding the base database from which the transactions will be extracted from
4. Then when prompted, enter the required information and allow the join to work
5. Each row is going to be outputted to be shown, after a batch is deleted it tells you how many has been deleted and the size of the queue
6. Optionally, you can run the queries file to view the precompiled list of queries to extract meaningful information from the warehouse

Issues in Java's Thread.class does not allow the thread to completely exit and hangs the program
Once the join is completed and prompted, feel free to go over to the warehouse and see the data
If you want you can see the data being added to the tables inside the warehouse

Credits
Name: Hamiz Siddiqui
Algorithm used: HybridJoin - Dr. Asif Naeem
Work Completed - 18/11/2023
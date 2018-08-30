# NLJ_SMJ_Implementation
Implementation of NLJ (Nested Loop Join) and SMJ (Sort Merge Join). The program accepts the following command line arguments:

**-f1**: full path to file1
**-a1**: the column to use as join attribute from file1 (counting from 0)
**-f2**: same as above for file2
**-a2**: same as above for file2
**-j**: SMJ or NLJ
**-m**: we use as memory metric the number of records*
**-t**: a directory to use for reading/writing temporary files
**-o**: the file to store the result of the join

For example, in order to join two relations stored in files “F1.csv” and “F2.csv” on the first column of F1 and the second column of F2, using Sort-Merge Join, having available memory = 200 records and saving the results to file “results.csv” one should execute the following command:

*java –jar joinalgs.jar –f1 F1.csv –a1 0 –f2 F2.csv –a2 1 –j SMJ –m 200 –t tmp –o results.csv
*

The goal is to compute the join in the shortest time possible without the use of data management libraries or a DBMS.


***


The program was executed via the **eclipse interface.** In order to give arguments using eclipse we go to: Run &rightarrow; Run configurations &rightarrow;, and we choose the arguments tab. In the specific field we can insert our
arguments with **exactly the same form as the following example:**

–f1 src/Files/D.csv –a1 3
–f2 src/Files/C.csv –a2 0
–j SMJ –m 200
–t "path of Temp folder"
–o "Path of results.csv file"

Obviously you can change the path of each file from the above example accordingly.


***

**Nested Loop Join (NLJ) algorithm**
Implemented a simple blocked-based NLJ algorithm. That means that if we have two relations R and S, first of all we will make the decision of which relationship to choose as outer and which as inner. In general, we read the selected outer relation in blocks of size m-1, where m the inserted number of buffers. For this purpose we use a simple static table of size m-1. When our table becomes full of records, we use the last available block (m) to load a single record from the inner file and try to make a join for each record inside the table. More specifically, for every m-1 records of the outer file we read the whole inner file and we write possible joined records in the results.csv file.


**Sort Merge Join (SMJ) algorithm**
Implemented a simple sort merge join algorithm. It is executed in two levels; a two-phase sorting algorithm and a merge join algorithm. During the two-phase sorting algorithm we create sorted sublists and in the end we reassemble the starting file in a sorted ascending order. During the second level, a merge-join algorithm was implemented. In the end we print the results and we delete the temporary files that were created during the execution of the SMJ algorithm.



# csv2mysql
Java tool to turn set of .csv files into a MySQL database

Many interesting data sets are distributed as "comma-separated value" (.csv) files.  If they are large (e.g., they exceed the 
capabilities of a spreadsheet program), it is convenient to load them into a relational database so that SQL queries can be 
used to explore them.  The tool described here reads a set of such files and heuristically produces a set of MySQL table 
definitions and LOAD DATA statements that, if sourced in MySQL, will import the data.  Because of its heuristics, it requires 
little or no guidance in determining the appropriate column data types, though there are options specifying whether the input 
files contain header lines, whether blank columns are to be treated as NULL, whether the text encoding is UTF8, and whether 
UNIQUE KEY constraints are to be sought. It is also possible to specify the format of the .csv file by giving the comma, quote 
and escape characters.

The file Documentation/CSV2MYSQL.html gives a more complete description of the tool, its options, and usage instructions.

This program is released under the MIT License and uses the opencsv package, documented at http://opencsv.sourceforge.net.  
Its code is included in the program.

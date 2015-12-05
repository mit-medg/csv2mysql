CSV2MYSQL: Heuristic Creation of MySQL Tables and Load Scripts
==============================================================

Peter Szolovits  
MIT Computer Science and Artificial Intelligence Laboratory  
psz@mit.edu  

Introduction
------------

Many interesting data sets are distributed as "comma-separated value"
(`.csv`) files.  If they are large (e.g., they exceed the capabilities
of a spreadsheet program), it is convenient to load them into a
relational database so that SQL queries can be used to explore them. 
The tool described here reads a set of such files and heuristically
produces a set of MySQL table definitions and `LOAD DATA` statements
that, if sourced in MySQL, will import the data.  Because of its
heuristics, it requires little or no guidance in determining the
appropriate column data types, though there are options specifying
whether the input files contain header lines, whether blank columns are
to be treated as `NULL`, whether the text encoding is `UTF8`, and
whether `UNIQUE KEY` constraints are to be sought. It is also possible
to specify the format of the `.csv` file by giving the comma, quote and
escape characters.  

This program is released under the MIT License (see below) and uses the
**opencsv** package, documented at <http://opencsv.sourceforge.net>. 
Its code is included in the program.  

Using the Program
-----------------

Csv2mysql is implemented in Java, built in Eclipse.  Eclipse can create
a runnable `.jar` file, which can then be invoked as:  

    java -jar csv2mysql.jar [options] file1 file2 ...

The following options are available:  

arg  | Meaning
---- | ------------------------------------------------------
`-c` | Next argument is the *comma* character \[default `,`\]
`-q` | Next argument is the *quote* character \[default `"`\]
`-e` | Next argument is the *escape* character \[default `\`\]   
`-o` | Next argument is the output file \[default `mysql_load.sql`\]   
`-g` | First line of input files does *not* contain column names; use generated ones   
`-b` | Empty columns are *not* treated as `NULL` values, but as themselves; `NULL``s` in MySQL are normally `\N` or `"\N"`  
`-u` | Text encoding is `UTF8`  
`-k` | For each column, check if all the data values are distinct; create `UNIQUE KEY` constraints for those that are; this is slow for very large data sets  
`-m` | Maximum number of distinct values to track in a column \[default 1,000,000\]      
`-p` | Report progress during scan of the data.  If multiple input files are specified, the program reports processing of each one, and prints a "." for every 100,000 lines of the input file that are read.  If `-k` is also given, it reports each time it has determined that a certain column of data is *not* unique.  

If the program is run with no arguments, it prints the above
information.  

Heuristics
----------

The program reads through all of the input data, one file at a time. 
Each file is processed separately, so there is no attempt to assure
consistency among different tables.  The data types that are recognized
are described in the following sub-sections.  

### NULLs

Empty fields in the input data are treated as `NULL` unless the `-b` option
is given; in that case, it is treated as an empty string, and `NULL` must be
denoted by `\N` or `"\N"`.

If none of the entries in
a column are `NULL`, we specify `NOT NULL` for the column.  

### Integers

An integer is an optional `+` or `-` sign followed by any number of the
digits 0-9.  (We have no provisions for any radix other than 10.) MySQL
has multiple integer data types of different sizes:  

| MySQL Type | Maximum Signed Value | Minimum Signed Value | Maximum Unsigned Value |
| --- | ---:| ---:| ---:|
| `TINYINT` | `127` | `-128` | `255`  | 
| `SMALLINT` | `32767` | `-32768` | `65535` |
| `MEDIUMINT` | `8388607` | `-8388608` | `16777215` |
| `INT` | `2147483647` | `-2147483648` | `4294967295` |
| `BIGINT` | `9223372036854775807` | `-9223372036854775808` | `18446744073709551615` |
| `DECIMAL` | `10^66 - 1`` (65 "9"'s)` | `-10^66 + 1` | `10^66 - 1` |
---

We choose the column data type that minimally fits the range of values
seen.  If they are all non-negative, we choose `UNSIGNED` types.  

### Floating-point

A floating point value is specified as an optional sign, a series of
digits with possibly one decimal point among them, optionally followed
by `E`, an optional sign, and a series of digits representing the power
of 10 in scientific notation. We choose to represent such floating point
values as one of the following MySQL types depending on their range of
values:  

| MySQL Type | Largest Negative | Smallest Negative | Smallest Positive | Largest Positive |
| --- | ---:| ---:| ---:| ---:|
| `FLOAT` | `-3.402823466E+38` | `-1.175494351E-38` | `1.175494351E-38` | `3.402823466E+38` |
| `DOUBLE` | `-1.7976931348623157E+308` | `-2.2250738585072014E-308` | `2.2250738585072014E-308` | `1.7976931348623157E+308` | 

Note that an integer can also appear in a floating-point field, but
will be treated as floating-point if not all values in that field are
integers.  If all the values in a field fit within a `FLOAT`, that type
will be selected; otherwise, `DOUBLE`.  In principle, we could also
support larger `DECIMAL` fields, but do not do so.  

### Date, Time and DateTime

MySQL provides Date, Time and DateTime data types, which we support. 
There is also a TimeStamp data type, but its range is much more limited
than in other implementation such as Oracle, so we do not consider that
possibility but use DateTime instead.  If every value in a field is `NULL` or a
valid Date, Time, or DateTime, then the field will be declared with that
data type.

Dates are normally written as YYYY-MM-DD, 
though two-digit years
or dates using / instead of - as a separator are also allowed. 
Times are written in 24-hour notation, as HH:MM:SS possibly followed by
a decimal fractional second; the separators are either a colon, period or
dash.  A DateTime is a date specification followed by either a space or
the character T, followed by a time specification.  Invalid dates or
times are not considered to be dates or times and will probably cause the
field to be interpreted as a text field.  Timezones are not handled.

As a special case, we also recognize dates and times in a format often
used in exporting from Oracle, which is of the form 15-jan-2015
14:30:25.  That may be followed by a space and a time zone specificiation
(e.g., EST).  If a column consistently codes an Oracle style date, time or
datetime/timestamp, we arrange to translate it on input into the SQL standard
format, but ignore the time zone portion, if any.  

### Character

Data fields all of whose values cannot be parsed as any of the other
data types are treated as character types.  These may be surrounded by
quotes but need not be, unless they contain characters (such as
quotation marks) that can confuse the reader.  MySQL supports various
lengths of character fields.  We use the following:  

| MySQL Type | Maximum Length | Maximum Length if UTF8 |
| --- | ---:| ---:|
| `VARCHAR(255)` | `255` | `84` |
| `TINYTEXT` (*not used* because it is equivalent to `VARCHAR(255)`) | `255` | `84` |
| `TEXT` | `65535` | `21844` |
| `MEDIUMTEXT` | `16777215` | `5592402` | 
| `LONGTEXT` | `4294967295L` | `1431655765` |

We choose the field that is minimally able to store all values of a
field.  For convenience, we also output the longest value as a comment
in the table definition.  

### Unique Keys

If requested, we test to see whether every value (except `NULL`) in a
column is distinct.  If so, we create a `UNIQUE KEY` constraint for that
column.  If a column has all integer or floating-point values, we test
for distinctness by actually converting each value into the
corresponding type. This is because although "001" and "1" are distinct
as strings, they are identical as integers.  Similarly, "3" and "3.0"
are the same floating-point value.  

We are able to maintain efficiently a representation of many distinct
integer values if they are dense enough to allow us to represent them as
a tree of ranges, which can be merged when they meet.  This is often the
case for identifiers or serial numbers.  Because other data types are
not "dense" in this sense, we must actually keep all distinct values in
order to find a non-unique one.  This explodes memory for very large
tables, so we limit to a maximum number of distinct values for each
column (see `-m` \[default 1M\]). Because the program stops looking for
duplicates after that limit, it cannot determine whether a column can
have a `UNIQUE KEY`, so it assumes not.  

Caveats
-------

The above heuristics may not create the intended data types.  For example,
some coding systems do distinguish between codes "001" and "01", but if
every code in a column can be interpreted as an integer, this program
will do so and thus lose these distinctions.  

The program also cannot determine which among the `UNIQUE KEY`s should
be a table's `PRIMARY KEY`, or whether compound keys need to be defined
to reflect the semantics and/or access patterns to the data.  It is also
unable to identify `FOREIGN KEY` constraints.  

Using the -k option asks the program to retain all values in each column
so that it can check if those values are unique.  Although it stops
doing so for any column as soon as the first duplicate is found, the
memory demands for a very large table with one or more unique columns
can be huge.  Therefore, in such cases it is necessary to start the Java
Virtual Machine with non-default memory parameters that can accommodate
the large hash tables and range trees that hold these values. Obviously, this must be
run on a system with enough memory.  For example, on one very large
table containing about 250M rows, the following worked (though these `jvm` arguments may
have provided more memory than needed):  

    java -Xms5128m -Xmx10512m -jar ~/bin/csv2mysql.jar -k -u test.csv

Even with extra memory, performance on very large data sets becomes
abysmal.  Thus, rather than using the `-k` option, it may be more
sensible to run without it, load the data into the database system, and
then check to see whether all values are distinct using its facilities. 
If so, `ALTER TABLE` can create the `UNIQUE KEY` constraints.  

MIT License
-----------

Copyright © 2015, Peter Szolovits, MIT  

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:  

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.  

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. 




package edu.mit.csail.medg.csv2mysql;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;

/** Program to construct MySql table definitions and data import statements
 * from csv files.
 * 
 * The program may be
      invoked as
    </p>
    <pre>java -jar csv2mysql.jar [options] file1 file2 ...</pre>
    <p>The following options are available:<br>
       <table width="100%" height="295" border="1" cellpadding="2"
      cellspacing="2">
      <tbody>
        <tr>
          <td valign="top" width="20" align="left"><tt>-c</tt></td>
          <td valign="top">Next argument is the <i>comma</i> character
            [default <tt>,</tt>] </td>
        </tr>
        <tr>
          <td valign="top" width="20"><tt>-q</tt></td>
          <td valign="top">Next argument is the <i>quote</i> character
            [default <tt>"</tt>]</td>
        </tr>
        <tr>
          <td valign="top" width="20"><tt>-e</tt></td>
          <td valign="top">Next argument is the <i>escape</i> character
            [default <tt>\</tt>]<br>
          </td>
        </tr>
        <tr>
          <td valign="top" width="20"><tt>-o</tt></td>
          <td valign="top">Next argument is the output file [default <tt>mysql_load.sql</tt>]<br>
          </td>
        </tr>
        <tr>
          <td valign="top" width="20"><tt>-g</tt></td>
          <td valign="top">First line of input files does <i>not</i>
            contain column names; use generated ones<br>
          </td>
        </tr>
        <tr>
          <td valign="top" width="20"><tt>-b</tt></td>
          <td valign="top">Empty columns are <i>not</i> treated as <tt>NULL</tt>
            values, but as themselves; <tt>NULL</tt><tt>s</tt> in MySQL
            are normally <tt>\N</tt> or <tt>"\N"</tt></td>
        </tr>
        <tr>
          <td valign="top" width="20"><tt>-u</tt></td>
          <td valign="top">Text encoding is <tt>UTF8</tt><br>
          </td>
        </tr>
        <tr>
          <td valign="top" width="20"><tt>-k</tt></td>
          <td valign="top">For each column, check if all the data values
            are distinct; create <tt>UNIQUE KEY</tt> constraints for
            those that are; this is slow for very large data sets<br>
          </td>
        </tr>
        <tr>
          <td valign="top" width="20"><tt>-m</tt></td>
          <td valign="top">Maximum number of distinct non-integer values
            to track in a column [default 1,000,000]; if exceeded, stop
            considering <tt>UNIQUE KEY</tt> for that column<br>
          </td>
        </tr>
        <tr>
          <td valign="top" width="20"><tt>-p</tt></td>
          <td valign="top">Report progress during scan of the
            data.&nbsp; If multiple input files are specified, the
            program reports processing of each one, and prints a "." for
            every 100,000 lines of the input file that are read.&nbsp;
            If <tt>-k</tt> is also given, it reports each time it has
            determined that a certain column of data is <i>not</i>
            unique.<br>
          </td>
        </tr>
      </tbody>
    </table>

 * The program can be invoked in Unix-style systems (including Mac OS X, Linux) via a command such as
 * java -jar csv2mysql.jar *.csv
 * 
 * We incorporate opencsv-3.3, slightly modified to remove its dependence on Apache StringUtils, to parse
 * the input csv files.
 * 
 * @author psz
 * April 18, 2015
 */
/*
 *  -g No column names are given in the first line of the csv; use generated names.
 *  -o File name to hold output; default is mysql_load.sql
 *  -c comma, given as next argument
 *  -q quote, given as next argument
 *  -e escape, given as next argument
 *  -u text encoding is UTF8
 *  -z integers whose first digit is 0 are taken to be strings
 *  -k try to create unique keys
 *  -m max number of possibly unique values/key to process
 *  -b empty column is NOT treated as NULL (normally \N), but as value
 *  -p print progress reports as the program runs

 */
public class Csv2Mysql {
	
	static char commaC = CSVParser.DEFAULT_SEPARATOR;
	static char quoteC = CSVParser.DEFAULT_QUOTE_CHARACTER;
	static char escapeC = CSVParser.DEFAULT_ESCAPE_CHARACTER;
	static boolean namesOnLine1 = true;
	static boolean utf = false;
	static boolean keys = false;
	static int maxVals = 1000000;
	static boolean blanksAreNull = true;
	static boolean progress = false;
	static final int reportEvery = 100000;
	static String outFileName = "mysql_load.sql";
	static ArrayList<String> files = new ArrayList<String>();
	static FileWriter fw = null;

	/** The main program. Takes arguments that are either options or file names.
	 * @param args
	 */
	public static void main(String[] args) {
		
		boolean printedHelp = false;

		for (int a = 0; a < args.length; a++) {
			String arg = args[a];
			if (arg.equals("-g")) namesOnLine1 = false;
			else if (arg.equals("-o") && a+1 < args.length) {
				a++;
				outFileName = args[a];
			}
			else if (arg.equals("-c") && a+1 < args.length) {
				a++;
				// It's a very bad idea to undefine the separator character!!!
				commaC = (args[a].length() > 0) ? args[a].charAt(0) : CSVParser.NULL_CHARACTER;
			}
			else if (arg.equals("-q") && a+1 < args.length) {
				a++;
				quoteC = (args[a].length() > 0) ? args[a].charAt(0) : CSVParser.NULL_CHARACTER;
			}
			else if (arg.equals("-e") && a+1 < args.length) {
				a++;
				escapeC = (args[a].length() > 0) ? args[a].charAt(0) : CSVParser.NULL_CHARACTER;
			}
			else if (arg.equals("-h")) {
				printHelp();
				printedHelp = true;
			}
			else if (arg.equals("-u")) 
				utf = true;
			else if (arg.equals("-k"))
				keys = true;
			else if (arg.equals("-z"))
				intPat = intPatNZ;  
			else if (arg.equals("-m") && a+1 < args.length) {
				a++;
				maxVals = new Integer(args[a]);
			}
			else if (arg.equals("-b"))
				blanksAreNull = false;
			else if (arg.equals("-p"))
				progress = true;
			else files.add(arg);
		}
		if (files.size()==0) {
			if (!printedHelp) printHelp();
		} else {
			try {
				File inFile1 = new File(files.get(0));
				File outFile1 = new File(inFile1.getParent(), outFileName);
				fw = new FileWriter(outFile1);
			} catch (IOException e) {
				System.err.println("Could not create output file.");
				e.printStackTrace();
				System.exit(1);
			}
			long startTime = System.nanoTime();
			for (String inFile: files)
				try {
					processFile(inFile);
				} catch (IOException e1) {
					System.err.println("Error in processing input file " + inFile);
					e1.printStackTrace();
				}
			try {
				fw.close();
			} catch (IOException e) {
				System.err.println("Error closing output file.");
				e.printStackTrace();
				System.exit(2);
			}
			if (progress) System.out.println("Completed " + files.size() + " files in " + toTime(System.nanoTime() - startTime));
		}
	}

	/** Processes one file for Csv2Mysql.
	 * @param inFile The file name to process
	 * @throws IOException
	 */
	private static void processFile(String inFile) throws IOException {
		
		if (progress) System.out.println("Processing " + inFile);
		CSVReader r = null;
		File inf = new File(inFile);
		try {
			FileReader fr = new FileReader(inf);
			r = new CSVReader(fr, commaC, quoteC, escapeC);
		} catch (FileNotFoundException e) {
			System.err.println("Could not open input file " + inFile);
//			e.printStackTrace();
		}
		int nCols = -1;
		String[] line;
		boolean treatLineAsNames = namesOnLine1; // Only applies to first line
		boolean treatedLineAsNames = false;	// Whether the first line really was names, so we skip on import.
		/*
		 * A column in SQL can always be a string, in which case we just compute its max length, to know what kind of 
		 * string data type to make it.  However, if every element of a column is of the correct format to be represented
		 * as an integer, float or double, date, time or datetime, then we should choose that representation.
		 * Therefore, we iterate through the entries in the csv file and for each one determine whether each of its column
		 * values excludes any of these possibilities. We also determine whether any of the entries are empty (i.e., 
		 * representing NULL), to keep track of whether to permit null values in that column.
		 * Although in principle we could try to keep track of the maximum precision and range of floating point numbers
		 * so we can distinguish between what should be represented as FLOAT or DOUBLE, we simply choose DOUBLE for all
		 * floating point.  We ignore the possibility of boolean, bit-string, etc., values.
		 * We accept datetime in the standard Oracle format (e.g., "09-sep-2012 15:00:00 US/Eastern"), but we ignore
		 * the timezone part; importing does, however, generate warnings about "Truncated incorrect datetime value".
		 * 
		 * For each column, we keep track of whether we have evidence that its values can be of each possible type.
		 * Values are 0 = unknown, 1 = possible, -1 = impossible (some value cannot be that type)
		 * We testing a new value unless the column's interpretation is already impossible. If it matches, we set 1,
		 * but if not, -1.
		 * 
		 * If unique keys are to be created, we keep track of whether all the values in a column are unique.
		 * In MySQL, multiple NULL values may appear in a column that is the basis of a unique key, but
		 * this is said not to be the case in some SQL implementations, such as MS SQL Server. It is, however, the
		 * standard.  For each column, we maintain a HashSet of all the non-NULL values, but shortcut the process as
		 * soon as we find a duplicate value.  If all values are unique, we do add a UNIQUE KEY constraint in the
		 * generated table definition.
		 * The problem with this method is that at the time we check for uniqueness, we do not yet know the eventual
		 * data type of the column, so the uniqueness is in terms of string representation.  However, if the column
		 * turns out to be INT, say, then multiple distinct strings may represent the same value, e.g., "01" and "1".
		 * Therefore, we should do a further integrity check after the column data type has been determined. 
		 */
		int[] canBeInt = null, canBeFloat = null, canBeDouble = null, canBeDate = null, canBeTime = null, 
				canBeDateTime = null, canBeOracleDateTime = null, canBeOracleDate = null;
		boolean[] nullable = null;
		BigInteger[] minInts = null, maxInts = null;
		long[] colLengths = null;
		String[] cols = null;
		ArrayList<HashSet<String>> vals = null;
		ArrayList<RangeTree> ivals = null;
		int printCol = 0;
		BigInteger iv = null;
		boolean triedBigInt = false;

		int lineNo = 0;
		long startTime = System.nanoTime();
		/* We keep track for each column of the following:
		 * Are all elements parsable as integers? Min and Max values
		 * As floats? Min and Max values
		 * As dates, times, or timestamps? (only in formats that can be input to MySql)
		 * Only as character strings? Max length.
		 */
		while ((line = r.readNext()) != null) {
			lineNo++;
			if (progress && (lineNo % reportEvery) == 0) {
				System.out.print(".");
				printCol++;
			}
			if (nCols < 0) {
				nCols = line.length;
				// Initialize all the tracking vars now that we know how many columns
				cols = new String[nCols];
				canBeInt = new int[nCols];
				canBeFloat = new int[nCols];
				canBeDouble = new int[nCols];
				canBeDate = new int[nCols];
				canBeTime = new int[nCols];
				canBeDateTime = new int[nCols];
				canBeOracleDateTime = new int[nCols];
				canBeOracleDate = new int[nCols];
				nullable = new boolean[nCols];
				minInts = new BigInteger[nCols];
				maxInts = new BigInteger[nCols];
				colLengths = new long[nCols];
				vals = new ArrayList<HashSet<String>>(nCols);
				ivals = new ArrayList<RangeTree>(nCols);
				for (int i = 0; i < nCols; i++) {
					cols[i] = "V" + i;
					canBeInt[i] = 0;
					canBeDouble[i] = 0;
					canBeDate[i] = 0;
					canBeTime[i] = 0;
					canBeDateTime[i] = 0;
					canBeOracleDateTime[i] = 0;
					canBeOracleDate[i] = 0;
					nullable[i] = false;
					minInts[i] = new BigInteger("99999999999999999999999999999999999999999999999999999999999999999");
					maxInts[i] = new BigInteger("-99999999999999999999999999999999999999999999999999999999999999999");
					colLengths[i] = 0L;
					if (keys) {
						vals.add(new HashSet<String>());
						ivals.add(new RangeTree());
					}
				}
			}
			else if (nCols != line.length) {
				complainWrongLength(line, lineNo, nCols);
				break;
			}
			if (!treatLineAsNames || !goodNames(line)) {
				for (int c = 0; c < line.length; c++) {
					String v = line[c].trim();
					if (v.equals("\\N") || (blanksAreNull && v.equals(""))) nullable[c] = true;
					else {
						if (keys) {
							RangeTree t = ivals.get(c);
							if ((t != null) && (canBeInt[c] >= 0)) {
								BigInteger vi = interpretAsBigInt(v);
								if (!t.add(vi)) {
									// Either non-integer or duplicate
									ivals.set(c,  null);
									if (progress && vi !=null) {
										if (printCol > 0) System.out.println("");
										System.out.println("Col " + c + " (" + cols[c] + ") is not unique as integers.");
										printCol = 0;
									}
								}
							}
							HashSet<String> s = vals.get(c);
							if (s != null) {
								if (!s.contains(v)) 
									s.add(v);
								else {
									vals.set(c, null);
									if (progress) {
										if (printCol > 0) System.out.println("");
										System.out.println("Col " + c + " (" + cols[c] + ") is not unique.");
										printCol = 0;
									}
								}
								if (s.size() > maxVals) {
									vals.set(c,  null);
									if (progress) {
										if (printCol > 0) System.out.println("");
										System.out.println("Col " + c + " (" + cols[c] + ") has more unique string values than max: " + maxVals 
												+ ".\n We stop examining this column for unique keys as other than integers.");
										printCol = 0;
									}
								}
							}
						}
						triedBigInt = false; // Don't convert to BigInteger twice (for INT or FLOAT/DOUBLE)
						if (canBeDate[c] >= 0) canBeDate[c] = isDate(v) ? 1 : -1;
						if (canBeOracleDate[c] >= 0) canBeOracleDate[c] = isOracleDate(v) ? 1 : -1;
						if (canBeTime[c] >= 0) canBeTime[c] = isTime(v) ? 1 : -1;
						if (canBeDateTime[c] >= 0) canBeDateTime[c] = isDateTime(v) ? 1 : -1;
						if (canBeOracleDateTime[c] >= 0) canBeOracleDateTime[c] = isOracleDateTime(v) ? 1 : -1;
						if (canBeInt[c] >= 0) {
							iv = interpretAsBigInt(v);
							triedBigInt = true;
							if (iv == null) canBeInt[c] = -1;
							else {
								canBeInt[c] = 1;
								if (iv.compareTo(minInts[c]) < 0) minInts[c] = iv;
								if (iv.compareTo(maxInts[c]) > 0) maxInts[c] = iv;
							}
						}
						if (canBeDouble[c] >= 0 || canBeFloat[c] >= 0) {
							// int can be float or double
							iv = (triedBigInt) ? iv : interpretAsBigInt(v);
							int floatType = floatKind(v);
							if (canBeFloat[c] >= 0)
								canBeFloat[c] = (floatType == FLOAT || iv != null) ? 1 : -1;
							if (canBeDouble[c] >= 0)
								canBeDouble[c] = (floatType == FLOAT || floatType == DOUBLE || iv != null) ? 1 : -1; 
						}
						if (v.length() > colLengths[c]) colLengths[c] = v.length();
					}
				}
			} else {
				for (int c = 0; c < line.length; c++) {
					cols[c] = line[c].trim();
				}
				treatedLineAsNames = true;
			}	// end of iteration over elements of an entry				
			treatLineAsNames = false;	// Possible only for first line
		}	// end of iteration over entries in csv
		if (progress) System.out.println("");
		
		// Now we generate the SQL to define the table that corresponds to this file:
		String tableName = inf.getName();
		int dot = tableName.lastIndexOf(".");
		if (dot > 1) tableName = tableName.substring(0, dot);
		
		// Each line ends with a comma, an optional comment, and newline, except the first.
		// We actually output these at the start of a new line, to avoid an extra comma at end.
		StringBuilder sb = new StringBuilder();
		sb.append("DROP TABLE IF EXISTS " + tableName + ";\n");
		String sep = " (";
		String comment = "";
		sb.append("CREATE TABLE " + tableName);
		for (int c = 0; c < nCols; c++) {
			sb.append(sep);
			sb.append(comment);
			sb.append("\n");
			sep = ",";
			comment = "";
			sb.append("   " + cols[c]);
			if (canBeInt[c] > 0) {
				if (keys && vals.get(c) != null && !areUniqueIntegers(vals.get(c))) {
					vals.set(c, null);
					if (progress) System.out.println("Col " + c + " (" + cols[c] + ") has unique strings but not integers.");
				}
				BigInteger[] numberTops = numberMax;
				if (minInts[c].compareTo(bigZero) >= 0) numberTops = numberMaxU;
				for (int i = 0; i < numberTops.length; i++) {
					if (maxInts[c].compareTo(numberTops[i]) <= 0) {
						sb.append(" " + numberTypes[i]);
						if (numberTops == numberMaxU) sb.append(" UNSIGNED");
						break;
					}
				}
			}
			else if (canBeFloat[c] > 0) {
				if (keys && vals.get(c) != null && !areUniqueDoubles(vals.get(c))) {
					vals.set(c, null);
					if (progress) System.out.println("Col " + c + " (" + cols[c] + ") has unique strings but not Floats.");
				}
				sb.append(" FLOAT");
			}
			else if (canBeDouble[c] > 0) {
				if (keys && vals.get(c) != null && !areUniqueDoubles(vals.get(c))) {
					vals.set(c, null);
					if (progress) System.out.println("Col " + c + " (" + cols[c] + ") has unique strings but not Doubles.");
				}
				sb.append(" DOUBLE");
			}
			else if (canBeDateTime[c] > 0 || canBeOracleDateTime[c] > 0) sb.append(" DATETIME");
			else if (canBeDate[c] > 0 || canBeOracleDate[c] > 0) sb.append(" DATE");
			else if (canBeTime[c] > 0) sb.append(" TIME");
			else {	// Chars
				String textType = whichText(colLengths[c], utf);
				if (textType==null) textType = "LONGTEXT";	// should never happen
				sb.append(" " + textType);
				comment =  "\t-- max=" + colLengths[c];
			}
			if (!nullable[c]) sb.append(" NOT NULL");
		}
		// Here is where to add UNIQUE KEY!
		if (keys) {
			for (int c = 0; c < nCols; c++) {
				if ((vals.get(c) != null && !vals.get(c).isEmpty())  || 
						(ivals.get(c) != null && !ivals.get(c).isEmpty())) {
					sb.append(sep);
					sb.append(comment);
					sb.append("\n");
					comment = "";
					sb.append("  UNIQUE KEY " + tableName + "_" + cols[c] + " (" + cols[c] + ")");
				}					
			}
		}
		if (comment != "") {
			sb.append(comment);
			sb.append("\n  )");
		}
		else sb.append(")");
		if (utf) sb.append("\n  CHARACTER SET = UTF8");
		sb.append(";\n\n");
		sb.append("LOAD DATA LOCAL INFILE \'" + inf.getName() + "\' INTO TABLE " + tableName + "\n");
		sb.append("   FIELDS TERMINATED BY \'" + quoteIfNeeded(commaC) + "\' ESCAPED BY \'" + quoteIfNeeded(escapeC) + "\'");
		sb.append(" OPTIONALLY ENCLOSED BY \'" + quoteIfNeeded(quoteC) + "\' \n");
		sb.append("   LINES TERMINATED BY \'\\n\'\n");
		if (treatedLineAsNames) sb.append("   IGNORE 1 LINES\n");
		sep = "   (";
		for (int c = 0; c < nCols; c++) {
			sb.append(sep + "@" + cols[c]);
			sep = ",";
		}
		sb.append(")\n");
		sep = " SET\n";
		for (int c = 0; c < nCols; c++) {
			sb.append(sep + "   " + cols[c] + " = ");
			sep = ",\n";
			String expr = "@" + cols[c];
			if (canBeOracleDateTime[c] > 0) expr = "STR_TO_DATE(" + expr + ",\"%d-%b-%Y %H:%i:%s\")";
			else if (canBeOracleDate[c] > 0) expr = "STR_TO_DATE(" + expr + ",\"%d-%b-%Y\")";
			if (nullable[c]) expr = "IF(@" + cols[c] + "=\'\', NULL, " + expr + ")";
			sb.append(expr);
		}
		sb.append(";\n\n");
		
		fw.write(sb.toString());
		if (progress) {
			System.out.println(inFile + ": " + ((treatedLineAsNames) ? lineNo - 1 : lineNo) + " entries");
			System.out.println(" ... " + toTime(System.nanoTime() - startTime));
		}
	}
	
	/** Given a char, returns a String containing that char or a string containing 
	 * two characters, a \ followed by the char if it needs to be quoted (i.e, if it
	 * is itself a \ or ').  If the char is the null character, the empty string is returned.
	 * @param c
	 * @return a String holding the input char or quoting it or empty if \0.
	 */
	private static String quoteIfNeeded(char c) {
		String ans = String.valueOf(c);
		if (c == CSVParser.NULL_CHARACTER) ans = "";
		if (c == '\'' || c == '\\') ans = "\\" + ans;
		return ans;
	}

	/** 		
	  This is a heuristic check to make sure that the first line of the .csv file, if 
		 it's said to contain the names of columns, has reasonable column names.
		 It can only be heuristic, because MySQL allows a very broad range of column names
		 as long as they are included in backquote characters; hence almost any first line could be
		 interpreted that way.
		 Our heuristic assumes that all column names will consist only of the characters [0-9a-zA-Z$_]
		 and will not start with a digit.

	 * @param line an array of purported names
	 * @return true if each name is valid according to our heuristic.
	 */
	private static boolean goodNames(String[] line) {
		// This is a heuristic check to make sure that the first line of the .csv file, if 
		// it's said to contain the names of columns, has reasonable column names.
		// It can only be heuristic, because MySQL allows a very broad range of column names
		// as long as they are included in backquote characters; hence almost any first line could be
		// interpreted that way.
		// Our heuristic assumes that all column names will consist only of the characters [0-9a-zA-Z$_]
		// and will not start with a digit.
		boolean ok = true;
		for (String name: line) {
			String nameTrimmed = name.trim();
			if (!goodNamePattern.matcher(nameTrimmed).matches()) {
				if (ok) System.err.println("1st line of file should list column names, but");
				System.err.println(" \"" + nameTrimmed + "\" is not (heuristically) a good name.");
				ok = false;
			}
		}
		return ok;
	}
	
	private static final Pattern goodNamePattern = Pattern.compile("[a-zA-Z$_][0-9a-zA-Z$_]*");

	/** Issue a warning message if some line has the wrong number of fields. 
	 * @param line the line
	 * @param lineNo line number to report
	 * @param nCols its actual number of columns
	 */
	private static void complainWrongLength(String[] line, int lineNo, int nCols) {
		System.err.println("Line " + lineNo + " has " + line.length + " elements instead of " + nCols + ":");
		for (int i = 0; i < Math.min(maxElementsToPrint, line.length); i++) System.err.println("  " + i + ": " + line[i]);
	}
	
	private final static int maxElementsToPrint = 5;

	/** Prints a help message if the program is called with no files to process
	 * 
	 */
	private static void printHelp() {
		for (String m: helpMsg) System.out.println(m);
	}
	
	private static final String[] helpMsg =
		{"This program reads .csv files specified on the command line and creates a .sql",
		 "file that defines an appropriately structured table for each file and includes",
		 "a LOAD DATA statement that will load the file contents into that table.  Only one",
		 "output file is created for all the specified files. By default, no primary keys or",
		 "indexes are generated and no referential integrity constraints are imposed because",
		 "these cannot be guessed from the contents of the files. The -k option will try to",
		 "create unique keys if applicable.",
		 "",
		 "The following options may be given on the command line:",
		 "  -g No column names are given in the first line of the csv; use generated names",
		 "  -o File name to hold output; default is mysql_load.sql",
		 "  -c comma, given as next argument",
		 "  -q quote, given as next argument",
		 "  -e escape, given as next argument",
		 "  -u text encoding is UTF8; otherwise unspecified, but we assume single-byte characters",
		 "  -k generate UNIQUE KEY constraints for columns with unique values",
		 "  -z integers whose first digit is 0 are taken to be strings",
		 "  -m max number of possibly unique values/key to process if -k [default 100000]",
		 "  -b empty column is NOT treated as NULL (normally \\N), but as value",
		 "  -p print progress reports; each . is " + reportEvery + " rows; non-uniqueness is also reported if -k"};
	
	/*
	 * To deal with MySql BIGINT, we have to accept numbers that are larger than Java's int, hence the
	 * complexity here of using BigInteger. In the extreme of very large numbers, we can use MySql's
	 * DECIMAL (with no fractions) to represent them.
	 */
	static final String[] numberTypes = 
		{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "DECIMAL"};
	static final String[] numberMaxs = 
		{"127", "32767", "8388607", "2147483647", "9223372036854775807",
		"99999999999999999999999999999999999999999999999999999999999999999"};
	static final String[] numberMaxUs = 
		{"255", "65535", "16777215", "4294967295", "18446744073709551615",
		"99999999999999999999999999999999999999999999999999999999999999999"};
	static final String[] numberMins = 
		{"-128", "-32768", "-8388608", "-2147483648", "-9223372036854775808",
		"-99999999999999999999999999999999999999999999999999999999999999999"};
	
	static final BigInteger[] numberMax = new BigInteger[numberTypes.length];
	static final BigInteger[] numberMaxU = new BigInteger[numberTypes.length];
	static final BigInteger[] numberMin = new BigInteger[numberTypes.length];
	static {
		for (int i = 0; i < numberTypes.length; i++) {
			numberMax[i] = new BigInteger(numberMaxs[i]);
			numberMaxU[i] = new BigInteger(numberMaxUs[i]);
			numberMin[i] = new BigInteger(numberMins[i]);
		}
	}
	static final BigInteger bigZero = new BigInteger("0");
	
	/*
	 * We support only "full" MySql floating point numbers as floats/doubles. I.e., no FLOAT(M,D).
	 */
	static final double floatMaxNeg = -3.402823466E+38;
	static final double floatMinNeg = -1.175494351E-38;
	static final double floatMaxPos = 3.402823466E+38;
	static final double floatMinPos = 1.175494351E-38;
	static final double doubleMaxNeg = -1.7976931348623157E+308;
	static final double doubleMinNeg = -2.2250738585072014E-308;
	static final double doubleMaxPos = 1.7976931348623157E+308;
	static final double doubleMinPos = 2.2250738585072014E-308;
	
	static final int NOTFLOAT = 0;
	static final int FLOAT = 1;
	static final int DOUBLE = 2;
	
	/** Determines if its argument can be interpreted as a floating point numeric value.
	 * @param s The String representing the value
	 * @return one of NOTFLOAT, FLOAT, or DOUBLE, if the argument cannot be interpreted as a
	 * floating point value, if it will fit within a FLOAT, or if it will fit within a DOUBLE.
	 */
	static int floatKind(String s) {
		Matcher m = floatPat.matcher(s);
		if (!m.matches()) return NOTFLOAT;
		Double d = new Double(s);
		if (d==0.0d) return FLOAT;
		if (d <= floatMinNeg && d >= floatMaxNeg) return FLOAT;
		if (d >= floatMinPos && d <= floatMaxPos) return FLOAT;
		if (d <= doubleMinNeg && d >= doubleMaxNeg) return DOUBLE;
		if (d >= doubleMinPos && d <= doubleMaxPos) return DOUBLE;
		return NOTFLOAT;
	}
		
	/** Determines if its argument can be interpreted as a BigInteger
	 * @param s The String representing the value 
	 * @return The integer value or null if it cannot be thus interpreted
	 */
	static BigInteger interpretAsBigInt(String s) {
		Matcher m = intPat.matcher(s);
		if (!m.matches()) return null;
		return new BigInteger(s);
	}
	
//	static BigInteger boo = null;
//	static BigInteger foo = interpretAsBigInt("32432947598375923874934742937");
			//new BigInteger("32432947598375923874x934742937");
	
	
	static Pattern intPat = Pattern.compile("(\\+|-)?\\d+");
	static final Pattern intPatNZ = Pattern.compile("(\\+|-)?(0|[1-9]\\d*)");	// intPat that disallows leading 0
	static final Pattern floatPat = Pattern.compile("(\\+|-)?(\\d+(\\.\\d*)|\\d*\\.\\d+)(E(\\+|-)?\\d+)?", Pattern.CASE_INSENSITIVE);
//	static final Pattern doublePat = Pattern.compile("(\\+|-)?(\\d+(\\.\\d*)|\\d*\\.\\d+)(D(\\+|-)?\\d+)?", Pattern.CASE_INSENSITIVE);
	static final String datePatS = "(?<yr>\\d\\d\\d\\d|\\d\\d)(?<a>[-/^])(?<mo>\\d\\d?)\\k<a>(?<da>\\d\\d?)";
//	static final String datePatS = "(\\d\\d\\d\\d|\\d\\d)-(\\d\\d?)-(\\d\\d?)";
	static final String timePatS = "([01]\\d|2[0-3])(?<s>[:.-])([0-5]\\d)\\k<s>([0-5]\\d)(\\.\\d+)?";
	static final String dateTimePatS = datePatS + "[ T]" + timePatS;
	// Oracle's export format example: 05-dec-3438 15:00:00 US/Eastern
	static final String oracleDatePatS = "(?<da>\\d\\d?)-(?<mos>jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)-(?<yr>\\d\\d\\d\\d)";
	static final String oracleTimePatS = "(?<hr>[01]\\d|2[0-3]):(?<mn>[0-5]\\d):(?<se>[0-5]\\d)( (?<tz>[a-zA-Z/]+))?";
	static final String oracleDateTimePatS = oracleDatePatS + " " + oracleTimePatS;
	
	static final Pattern datePat = Pattern.compile(datePatS, Pattern.CASE_INSENSITIVE);
	static final Pattern oracleDatePat = Pattern.compile(oracleDatePatS, Pattern.CASE_INSENSITIVE);
	static final Pattern timePat = Pattern.compile(timePatS);
	static final Pattern dateTimePat = Pattern.compile(dateTimePatS, Pattern.CASE_INSENSITIVE);
	static final Pattern oracleDateTimePat = Pattern.compile(oracleDateTimePatS, Pattern.CASE_INSENSITIVE);
	static final String[] monthNames = {"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"};
	static final int[] monthLengths = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	
	/** Determines whether its input can be interpreted as a date 
	 * @param s the String input
	 * @return true if the input matches the pattern for a SQL-style date and if the year, month and day components are valid
	 */
	static boolean isDate(String s) {
		Matcher m = datePat.matcher(s);
		boolean matched = m.matches();
		if (matched) {
			Integer yr = new Integer(m.group("yr"));
			Integer mo = new Integer(m.group("mo"));
			Integer da = new Integer(m.group("da"));
			// This only accepts 2 or 4-digit years, and just checks that month and day
			// numbers are in range, but does not exclude, e.g., 4/31 or deal with leap years.
			if (((yr >= 0 && yr <= 99) || yr >=1900) &&
					mo >= 0 && mo <= 12 & da >= 0 && da <= 31) return true;
		}
		return false;
	}

	/** Determines whether its input can be interpreted as a time 
	 * @param s the String input
	 * @return true if the input matches the pattern for a SQL-style time and if the hour, minute and second components are valid
	 */
	static boolean isTime(String s) {
		return timePat.matcher(s).matches();
	}
	
	/** Determines whether its input can be interpreted as a datetime
	 * @param s the String input
	 * @return true if the input specifies a valid datetime value
	 */
	static boolean isDateTime(String s) {
		Matcher m = dateTimePat.matcher(s);
		if (m.matches()) {
			Integer yr = new Integer(m.group("yr"));
			Integer mo = new Integer(m.group("mo"));
			Integer da = new Integer(m.group("da"));
			// This only accepts 2 or 4-digit years, and just checks that month and day
			// numbers are in range, but does not exclude, e.g., 4/31 or deal with leap years.
			if (((yr >= 0 && yr <= 99) || yr >=1900) &&
					mo >= 0 && mo <= 12 & da >= 0 && da <= 31) return true;
		}
		return false;
	}

	/** Determines whether its input can be interpreted as a datetime or timestamp, as formatted by some Oracle tools
	 * @param s the String input
	 * @return true if the input can be interpreted as such.
	 */
	static boolean isOracleDateTime(String s) {
		Matcher m = oracleDateTimePat.matcher(s);
		boolean matched = m.matches();
		return matched && okOracleMonthDay(m.group("mos"), m.group("da"));
	}
	
	/** Determines whether its input can be interpreted as a date, as formatted by some Oracle tools
	 * @param s the String input
	 * @return true if the input can be interpreted as such.
	 */
	static boolean isOracleDate(String s) {
		Matcher m = oracleDatePat.matcher(s);
		if (m.matches()) return okOracleMonthDay(m.group("mos"), m.group("da"));
		else return false;
	}

	/** Checks that the combination of month and day strings specify a valid date
	 * @param month String holding the abbreviated name of a month
	 * @param day String holding a string specifying the day of the month
	 * @return true if the month and day form a valid date
	 */
	static boolean okOracleMonthDay(String month, String day) {
		Integer da = new Integer(day);
		for (int i = 0; i < monthNames.length; i++) 
			if (month.equalsIgnoreCase(monthNames[i]) && da <= monthLengths[i])
				return true;
		return false;
	}
	
	/** Given a unique set of strings, check if they form a unique set of integers.
	 * The answer will be "no" if any of the Strings in fact do not specify integers or
	 * if two distinct strings map to the same integer value, such as "1" and "001".
	 * @param set a HashSet of Strings
	 * @return true if the set of integers is also unique
	 */
	static boolean areUniqueIntegers(HashSet<String> set) {
		HashSet<BigInteger> ints = new HashSet<BigInteger>();
		for (String s: set) {
			BigInteger i = new BigInteger(s);
			if (ints.contains(i)) 
				return false;
			ints.add(i);
		}
		return true;
	}
	
	/** Given a unique set of strings, check if they form a unique set of floating-point values.
	 * The answer will be "no" if any of the Strings in fact do not specify floating values or
	 * if two distinct strings map to the same float value, such as ".1" and "0.1" or "1e-1".
	 * @param set a HashSet of Strings
	 * @return true if the set of floating values is also unique
	 */
	static boolean areUniqueDoubles(HashSet<String> set) {
		HashSet<Double> doubles = new HashSet<Double>();
		for (String s: set) {
			Double d = new Double(s);
			if (doubles.contains(d)) 
				return false;
			doubles.add(d);
		}
		return true;
	}
	
	static final String[] textTypes = {"VARCHAR(255)", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"};
	// Which of the following limits apply depends on whether we use UTF8 or LATIN1 encoding:
	static final long[] textMaxU = {84, 84, 21844, 5592402, 1431655765};
	static final long[] textMax = {255, 255, 65535, 16777215, 4294967295L};
	
	/** Determines the smallest text datatype that will hold strings of length len. If utf, then the limits are smaller
	 * because utf8 characters require more bytes to represent. 
	 * @param len maximum length of relevant character strings
	 * @param utf whether the representation will be UTF8
	 * @return the smalled text type that is large enough
	 */
	static String whichText(long len, boolean utf) {
		long[] limits = textMax;
		if (utf) limits = textMaxU;
		for (int i = 0; i < limits.length; i++) {
			if (len <= limits[i]) return textTypes[i];
		}
		return null;
	}
	
	static final long hrPerDay = 24;
	static final long minPerHr = 60;
	static final long secPerMin = 60;
	static final long nsPerSec = 1000000000;

	/** A convenience function to turn a number of elapsed nanoseconds into a human understandable string
	 * showing how many days, hours, minutes and seconds the input represents
	 * @param ns number of nanoseconds
	 * @return the String that shows its interpretation
	 */
	static String toTime(long ns) {
		long sec = ns/nsPerSec;
//		System.out.println(ns + ":" + sec + " sec");
		long min = sec/secPerMin; sec = sec % secPerMin;
//		System.out.println(sec + ":" + min + "min");
		long hr = min/minPerHr; min = min % minPerHr;
//		System.out.println(min + ":" + hr + " hours");
		long day = hr/hrPerDay; hr = hr % hrPerDay;
//		System.out.println(hr + ":" + day + " days");
		StringBuilder sb = new StringBuilder();
		if (day > 0) sb.append(day).append(" days, ");
		if (day > 0 || hr > 0) sb.append(hr).append( " hours, ");
		if (day > 0 || hr > 0 || min > 0) sb.append(min).append(" minutes, ");
		sb.append(sec).append(" seconds.");
		return sb.toString();
	}
	
}

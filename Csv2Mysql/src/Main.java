
/**
 * This class is simply present to allow reference to the Main method of Csv2Mysql without the package prefix.
 * @author psz
 *
 */
public class Main {
//	static final String[] test = {"1", "2", "6", "4", "5", "0", "3"};
//	static RangeTree t = new RangeTree();

	/** The main program is just a wrapper for Csv2Mysql so we need not specify the package.
	 * @param args
	 */
	public static void main(String[] args) {
		edu.mit.csail.medg.csv2mysql.Csv2Mysql.main(args);
	}
}

package Haramy;

import java.awt.Polygon;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;


public class DBAppTest {
	
	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
	public static void main (String [] args) throws DBAppException, IOException, ParseException {
		
		//dbApp.view(strTableName);
		//dbApp.printIndex(strTableName);
		//"view" takes a strTableName and shows it's contents!
		//"printIndex" takes a table name and prints all its indexes stating which column they are for!
		//I hope they help you in testing!
		
		/*
		DBApp dbApp = new DBApp( );
		Hashtable htblColNameValue = new Hashtable( );
		
		String strTableName2 = "Employee";
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("salary", "java.lang.Double");
		htblColNameType.put("manager", "java.lang.Boolean");
		htblColNameType.put("DOB", "java.util.Date");
		dbApp.createTable( strTableName2, "id", htblColNameType );
		
		String strTableName = "Student";
		Hashtable htblColNameType2 = new Hashtable( );
		htblColNameType2.put("id", "java.lang.Integer");
		htblColNameType2.put("name", "java.lang.String");
		htblColNameType2.put("gpa", "java.lang.Double");
		htblColNameType2.put("pol", "java.awt.Polygon");
		dbApp.createTable( strTableName, "pol", htblColNameType2 );
		
		String strTableName3 = "Student2";
		Hashtable htblColNameType3 = new Hashtable( );
		htblColNameType3.put("id", "java.lang.Integer");
		htblColNameType3.put("name", "java.lang.String");
		htblColNameType3.put("gpa", "java.lang.Double");
		dbApp.createTable( strTableName, "id", htblColNameType3 );
		
		
		//randomized data
		for (int i=0; i<10000; i++) {
			Hashtable data = new Hashtable( );
			String [] names = {"raneem", "abdo", "heidi", "yasser"};
			Random rand = new Random();
			int id = rand.nextInt(1000);
			int name = rand.nextInt(4);
			double salary = 5000 + (20000-5000) * rand.nextDouble();
			boolean manager = rand.nextBoolean();
			Date DOB = new Date (-94677120000L + (Math.abs(rand.nextLong())% (70L * 365 * 24 * 60 * 60 * 1000)));
			int x = rand.nextInt(9);
			int y = rand.nextInt(9);
			Polygon pol = new Polygon();
			pol.addPoint(x, y);
			pol.addPoint(x, y);
			pol.addPoint(x, y);
			data.put("pol", pol);
			data.put("id", id);
			data.put("name", names [name]);
			data.put("salary", salary);
			data.put("manager", manager);
			data.put("DOB", DOB);
			dbApp.insertIntoTable( strTableName , data );
			//This counter is to help you know that the code is still inserting and is not stuck
			System.out.println(i);
		}
		
		for (int i=0; i<10000; i++) {
			Hashtable data = new Hashtable( );
			String [] names = {"raneem", "abdo", "heidi", "yasser"};
			Random rand = new Random();
			int id = rand.nextInt(1000);
			int name = rand.nextInt(4);
			double gpa = 0 + (100-0) * rand.nextDouble();
			boolean graduate = rand.nextBoolean();
			Date DOB = new Date (-94677120000L + (Math.abs(rand.nextLong())% (70L * 365 * 24 * 60 * 60 * 1000)));
			int x = rand.nextInt(9);
			int y = rand.nextInt(9);
			Polygon pol = new Polygon();
			pol.addPoint(x, y);
			pol.addPoint(x, y);
			pol.addPoint(x, y);
			data.put("pol", pol);
			data.put("id", id);
			data.put("name", names [name]);
			data.put("gpa", gpa);
			data.put("graduate", graduate);
			data.put("DOB", DOB);
			dbApp.insertIntoTable( strTableName2 , data );
			System.out.println(i);
		}
		
		//non-randomized data for efficient testing with indexes created throughout
		
		//A--
		//operations on table Student
		
		htblColNameValue.put("id", 1);
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		Polygon x = new Polygon();
		x.addPoint(2, 3);
		x.addPoint(6, 7);
		x.addPoint(5, 3);
		htblColNameValue.put("pol", x);
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		
		//dbApp.createRTreeIndex(strTableName, "pol");
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", 3);
		htblColNameValue.put("name", new String("Raneem" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		Polygon y = new Polygon();
		y.addPoint(2, 3);
		y.addPoint(9, 7);
		y.addPoint(5, 3);
		htblColNameValue.put("pol", y);
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", 2);
		htblColNameValue.put("name", new String("Dalia" ) );
		htblColNameValue.put("gpa", new Double( 1.25 ) );
		Polygon z = new Polygon();
		z.addPoint(6, 3);
		z.addPoint(9, 7);
		z.addPoint(5, 10);
		htblColNameValue.put("pol", z);
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("name", new String("dodo" ) );
		dbApp.updateTable(strTableName, "(6,3),(9,7),(5,10)", htblColNameValue);
		
		//dbApp.createBTreeIndex(strTableName2, "id");
		//dbApp.createBTreeIndex(strTableName2, "name");
		//dbApp.createBTreeIndex(strTableName2, "gpa");
		
		SQLTerm[] arrSQLTerms = new SQLTerm[3];
		arrSQLTerms[0] = new SQLTerm();
		arrSQLTerms[0]._strTableName = "Student";
		arrSQLTerms[0]._strColumnName= "name";
		arrSQLTerms[0]._strOperator = "=";
		arrSQLTerms[0]._objValue = "Raneem";
		arrSQLTerms[1] = new SQLTerm();
		arrSQLTerms[1]._strTableName = "Student";
		arrSQLTerms[1]._strColumnName= "gpa";
		arrSQLTerms[1]._strOperator = "<";
		arrSQLTerms[1]._objValue = 1.0;
		arrSQLTerms[2] = new SQLTerm();
		arrSQLTerms[2]._strTableName = "Student";
		arrSQLTerms[2]._strColumnName= "pol";
		arrSQLTerms[2]._strOperator = "=";
		arrSQLTerms[2]._objValue = y;
		String[]strarrOperators = new String[2];
		strarrOperators[0] = "and";
		strarrOperators[1] = "and";
		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);

		while (resultSet.hasNext())
			System.out.println(resultSet.next());
		
		
		//B--
		//operations on table Student2
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", 2);
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		dbApp.insertIntoTable( strTableName3 , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", 6);
		htblColNameValue.put("name", new String("Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		dbApp.insertIntoTable( strTableName3 , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", 7);
		htblColNameValue.put("name", new String("Masr" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		dbApp.insertIntoTable( strTableName3 , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 6 ));
		htblColNameValue.put("name", new String("Noor" ) );
		dbApp.deleteTable( strTableName3 , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 453455 ));
		htblColNameValue.put("name", new String("Raneem" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		dbApp.insertIntoTable( strTableName3 , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 5674567 ));
		htblColNameValue.put("name", new String("Dalia Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.25 ) );
		dbApp.insertIntoTable( strTableName3 , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23498 ));
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		dbApp.insertIntoTable( strTableName3 , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 78452 ));
		htblColNameValue.put("name", new String("Zaky Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.88 ) );
		dbApp.insertIntoTable( strTableName3 , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("name", new String("Noor" ) );
		dbApp.updateTable(strTableName3, "453455", htblColNameValue);
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 2343432 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		dbApp.deleteTable( strTableName3 , htblColNameValue );
		
		SQLTerm[] arrSQLTerms2;
		arrSQLTerms2 = new SQLTerm[2];
		arrSQLTerms2[0]._strTableName = "Student2";
		arrSQLTerms2[0]._strColumnName= "name";
		arrSQLTerms2[0]._strOperator = "=";
		arrSQLTerms2[0]._objValue = "John Noor";
		arrSQLTerms2[1]._strTableName = "Student";
		arrSQLTerms2[1]._strColumnName= "gpa";
		arrSQLTerms2[1]._strOperator = "=";
		arrSQLTerms2[1]._objValue = new Double( 1.5 );
		String[]strarrOperators2 = new String[1];
		strarrOperators2[0] = "OR";
		// select * from Student where name = “John Noor” or gpa = 1.5;
		resultSet = dbApp.selectFromTable(arrSQLTerms2 , strarrOperators2);
		
		while (resultSet.hasNext())
			System.out.println(resultSet.next());
		
		*/
	}

}

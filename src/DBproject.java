import java.io.IOException;

public class DBproject {

	public static void main(String [] args) throws IOException
	{
		
		//*******************Basic Variables*************************
		//
		//Path of first file
		String f1 = args[1];
		//Column to be used as join attribute from f1
		int a1 =  Integer.valueOf(args[3]);;
		//Path of second file
		String f2 = args[5];
		//Column to be used as join attribute from f2
		int a2 = Integer.valueOf(args[7]);;
		//Join algorithm to be used
		String j  = args[9];
		//Available memory
		int m = Integer.valueOf(args[11]);;
		//Directory to use for temporary files
		String t  = args[13];
		//File to store the results
		String o  = args[15];	
		//
		//***********************************************************	
		
		System.out.println("Join algorithm will be executed with the following variables: ");
		System.out.println("   " + args[0]  + "  :  " + args[1]);
		System.out.println("   " + args[2]  + "  :  " + args[3]);
		System.out.println("   " + args[4]  + "  :  " + args[5]);
		System.out.println("   " + args[6]  + "  :  " + args[7]);
		System.out.println("   " + args[8]  +"   :  " + args[9]);
		System.out.println("   " + args[10] +"   :  " + args[11]);
		System.out.println("   " + args[12] +"   :  " + args[13]);
		System.out.println("   " + args[14] +"   :  " + args[15]);
		System.out.println("");
		System.out.println("");
		
		//Check which Join method to use
		if(args[9].equals("NLJ"))
		{
			System.out.println("Executing NLJ algorithm...");
			
			NLJ nlj = new NLJ(f1, a1, f2, a2, j, m, t, o);
			
		}
		else if(args[9].equals("SMJ"))
		{
			System.out.println("Executing SMJ algorithm...");
			SMJ smj = new SMJ(f1, a1, f2, a2, j, m, t, o);
		}
		else
		{
			System.out.println("Wrong Join algorithm...Available join algorithms are: SMJ and NLJ");
			System.exit(0);
		}
		
	}
}

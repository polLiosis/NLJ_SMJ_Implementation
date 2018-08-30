import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Vector;


// Implementation of NLJ algorithm (Nested Loop Join)
public class NLJ {

	private String f1;   // Path for first file
	private int a1;      // The column to use as join attribute from f1
	private String f2;   // Path for second file
	private int a2;      // The column to use as join attribute from f2
	private String j;    // Join Algorithm
	private int m;       // Available memory size. For simplicity we use as memory metric the number of records
	private String t;    // A directory where we create temporary files
	private String o;    // The file to store the results of the join
	
	int outerFileSize = 0;
	int innerFileSize = 0;
	private int num_of_chunks = 0;
	private String chunk[] = null;
	private String block[] = null;
	
	private Vector<Integer> num_of_IOs = new Vector<Integer>();
	private int temp_chunk_size = 0;
	
	private long startTime;
	private long endTime;
	private long totalTime;
	
	private int num_of_joins = 0;
	private int chunks_used = 0;
	
	private String bestForOuter = "";
	
	public NLJ(String f1, int a1 , String f2, int a2, String j, int m, String t, String o ) throws IOException
	{
		this.f1 =  f1;
		this.a1 =  a1;
		this.f2 =  f2;
		this.a2 =  a2;
		this.j  =  j;
		this.m  =  m;
		this.t  =  t;
		this.o  =  o;
		
		this.clearResultFile();
		this.chunk = new String[m-1];
		this.retrieveNumOfRecords();
		
		if(this.bestForOuter.equals("f1"))
		{
			this.startTime = System.nanoTime();
			this.executeJoinAlgorithm(this.f1, this.f2);
			this.endTime = System.nanoTime();
		}
		else if(this.bestForOuter.equals("f2"))
		{
			this.startTime = System.nanoTime();
			this.executeJoinAlgorithm(this.f2, this.f1);
			this.endTime = System.nanoTime();
		}
		
		calculateJoinExecTime();
		printExecutionStats();
	}
	
	// Clear the text file where the results of the join algorithm will be written
	private void clearResultFile() throws IOException
	{
		FileWriter fw = new FileWriter(this.o);
		fw.write("");
	}
	
	// Calculate the execution time of the join algorithm
	private void calculateJoinExecTime()
	{
		this.totalTime = endTime - startTime;
	}
	
	// Retrieve the size of each file (in records) in order to decide which file should be used as outer and inner
	private void retrieveNumOfRecords()
	{
		int total_records_of_f1 = 0;
		int total_records_of_f2 = 0;
		String line = "";
		BufferedReader br = null;
		
        try
        {
        	br = new BufferedReader(new FileReader(f1));
            while ((line = br.readLine()) != null) 
            {
            	total_records_of_f1 = Integer.valueOf(line);
            	break;	
            }
        }catch (FileNotFoundException e) 
        {
            e.printStackTrace();
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        } 
        finally 
        {
            if (br != null) 
            {
                try 
                {
                    br.close();
                } 
                catch (IOException e) 
                {
                    e.printStackTrace();
                }
            }
        }
        
        try
        {
        	br = new BufferedReader(new FileReader(f2));
            while ((line = br.readLine()) != null) 
            {
            	    total_records_of_f2 = Integer.valueOf(line);
            		break;
            }
        }catch (FileNotFoundException e) 
        {
            e.printStackTrace();
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        } 
        finally 
        {
            if (br != null) 
            {
                try 
                {
                    br.close();
                } 
                catch (IOException e) 
                {
                    e.printStackTrace();
                }
            }
        }
        
        this.chooseInnerAndOuterRelation(total_records_of_f1, total_records_of_f2);
	}
	
	// We choose inner and outer relation for the join
	private void chooseInnerAndOuterRelation(int f1_size, int f2_size)
	{
		int f1f2 = f1_size +  (this.num_of_chunks = (int)(Math.ceil(f1_size/(double)(this.m -1)))) * f2_size;
		int f2f1 = f2_size +  (this.num_of_chunks = (int)(Math.ceil(f2_size/(double)(this.m -1)))) * f1_size;
		
		System.out.println("f1JOINf2 expected I/O cost: " + f1f2);
		System.out.println("f2JOINf1 expected I/O cost: " + f2f1);
		
		if ( f1f2 <= f2f1)
		{
			this.bestForOuter = "f1";
		}
		else
		{
			this.bestForOuter = "f2";
		}
	}
	
	// Execute join algorithm
	private void executeJoinAlgorithm(String outer, String inner)
	{
		String line = "";
		BufferedReader br = null;
		boolean first_line = true;
		int counter = 0;
        
        try
        {
        	br = new BufferedReader(new FileReader(outer));
        	while(true)
            {
        		line = br.readLine();			
            	if (first_line)
            	{         		
            		this.outerFileSize = Integer.valueOf(line);
            		first_line = false;
            		num_of_chunks = (int)(Math.ceil(this.outerFileSize/(double)(this.m -1)));
            		System.out.println(num_of_chunks + "  Chunk(s) will be created");        		
            	}
            	else
            	{
            		if(line == null)
            		{
            			this.num_of_IOs.add(this.temp_chunk_size);
            			counter = 0;
                		this.temp_chunk_size = 0;
                		joinWithInnerFile(inner);
                		this.clearChunk();
            			break;
            		}
            		else
            		{
            			if(counter < this.m-1)
                		{
                			this.chunk[counter] = line;
                    		this.temp_chunk_size++;
                			counter++;
                		}
            			else
                		{
                			this.num_of_IOs.add(this.temp_chunk_size);
                			counter = 0;
                    		this.temp_chunk_size = 0;
                    		joinWithInnerFile(inner);
                    		this.clearChunk();
                    		this.chunks_used++;
                    		this.chunk[counter] = line;
                    		this.temp_chunk_size++;
                			counter++;
                		}
            		}
                }
            } 
        }
        catch (FileNotFoundException e) 
        {
            e.printStackTrace();
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        } 
        finally 
        {
            if (br != null) 
            {
                try 
                {
                    br.close();
                } 
                catch (IOException e) 
                {
                    e.printStackTrace();
                }
            }
        }
	}
	
	
	private void joinWithInnerFile(String file)
	{
		String temp [] = null;
		String line = "";
		BufferedReader br = null;
		boolean first_line = true;

	        try
	        {
	        	br = new BufferedReader(new FileReader(file));
	            while ((line = br.readLine()) != null) 
	            {
	            	if (first_line)
	            	{	
	    	            this.num_of_IOs.add(Integer.valueOf(line));
	            		first_line = false;            		
	            	}
	            	else
	            	{
	            		this.block = line.split(",");
	            		for(int i = 0; i < this.chunk.length; i++)
	            		{
	            			if(chunk[i] != null)
	            			{
	            			    temp = null;
	            				temp = chunk[i].split(",");	
	            				if(bestForOuter.equals("f1"))
		            			{
		            				if(!temp[0].equals(""))
		            				{
		            					if (this.block[this.a2].equals(temp[this.a1]))
			    		            	{
			    		            		writeToResults(temp,this.block);
			    		            	}
		            				}
		            			}
		            			else if (bestForOuter.equalsIgnoreCase("f2"))
		            			{
		            				if(!temp[0].equals(""))
		            				{
		            					if (this.block[this.a1].equals(temp[this.a2]))
			    		            	{			
			    		            		writeToResults(temp,this.block);
			    		            	}
		            				}
		            			}
		            			else
		            			{
		            				System.out.println("Error");
		            				System.exit(0);
		            			}
	            			}
	            		}	            		
	            	}
	            }       
	        } 
	        catch (FileNotFoundException e) 
	        {
	            e.printStackTrace();
	        } 
	        catch (IOException e) 
	        {
	            e.printStackTrace();
	        } 
	        finally 
	        {
	            if (br != null) 
	            {
	                try 
	                {
	                    br.close();
	                } 
	                catch (IOException e) 
	                {
	                    e.printStackTrace();
	                }
	            }
	        }
		
	}
	
	//Write results to external file
	private void writeToResults(String a[], String b[]) throws IOException
	{
		FileWriter fw = new FileWriter(this.o, true);
        StringBuilder sb = new StringBuilder();
        
        if(this.bestForOuter.equals("f1"))
        {
        	for(int i = 0; i < a.length; i++)
            {
            		sb.append(a[i]);
            		sb.append(",");
            }
            
            for(int i = 0; i < b.length; i++)
            {
          		if (i != this.a2)
          		{
          			if(i == b.length-1)
          			{
          				sb.append(b[i]);
          			}
          			else
          			{
          			    sb.append(b[i]);
              	     	sb.append(",");
          			}    	
          		}
            }
        }
        else if (this.bestForOuter.equals("f2"))
        {
        	for(int i = 0; i < b.length; i++)
            {
            		sb.append(b[i]);
            		sb.append(",");
            }
        	
        	for(int i = 0; i < a.length; i++)
            {
          		if (i != this.a1)
          		{
          			if(i == a.length-1)
          			{
          				sb.append(a[i]);
          			}
          			else
          			{
          			    sb.append(a[i]);
              	     	sb.append(",");
          			}
          		}
            }
        }
        else
        {
        	System.out.println("Error!!!");
        	System.exit(0);
        }
      
      
      sb.append('\n');
      this.num_of_joins++;
      fw.write(sb.toString());
      fw.close();
        
	}
	
	private void clearChunk()
	{
		for(int i = 0; i < this.chunk.length; i++)
		{
			this.chunk[i] = "";
		}
	}
	
	// Calculate total number of I/O in order to estimate efficiency of the algorithm
	private int calculateTotalIOs()
	{
		int size = 0;
		
		for(int i = 0; i < this.num_of_IOs.size(); i++)
		{
			size = size + this.num_of_IOs.get(i);
		}
		
		return size;
	}
	
	// Print exetution statisctics
	private void printExecutionStats()
	{
		System.out.println();
		System.out.println();
		System.out.println("____________Execution Stats____________");
		System.out.println("Chunks used : " + ++chunks_used);
		System.out.println("IOs : " + this.num_of_IOs);
		System.out.println("# I/Os : " + this.calculateTotalIOs());
		System.out.println("# of joined records : " + this.num_of_joins );
		System.out.println("Execution time of Join Algorithm: " + this.totalTime + " nanoseconds");
		System.out.println("_______________________________________");
				
	}
	
}

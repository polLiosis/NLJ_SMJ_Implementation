import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.util.Scanner;
import java.util.Vector;

public class SMJ {

	private String f1;   // Path for first file
	private int a1;      // The column to use as join attribute from f1
	private String f2;   // Path for second file
	private int a2;      // The column to use as join attribute from f2
	private String j;    // Join Algorithm
	private int m;       // Available memory size. For simplicity we use as memory metric the number of records
	private String t;    // A directory where we create temporary files
	private String o;    // The file to store the results of the join
	
	private int f1_size = 0;
	private int f2_size = 0;
	private int num_of_sublists1 = 0;
	private int num_of_sublists2 = 0;
	
	private String [] buffers;
	private Boolean [] saturated;
	private int num_of_saturated_sublists = 0;
	private int written_recs = 0;
	private int starting_file_size = 0;
	
	private long startTime;
	private long endTime;
	private long totalTime;
	
	private int num_of_joins = 0;
	
	private BufferedReader [] brs = null;
	
	public SMJ(String f1, int a1 , String f2, int a2, String j, int m, String t, String o ) throws IOException
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
		
		this.buffers   = new String[m];
		this.saturated = new Boolean[m];
			
		this.executeBlockBasedSMJAlgorithm();
		this.printExecutionStats();
	}
	
	private void executeBlockBasedSMJAlgorithm() throws IOException
	{
		this.startTime = System.nanoTime();
		this.sortFirstRelation();
		this.sortSecondRelation();
		this.mergeJoin();
		this.endTime = System.nanoTime();
		
		System.out.println("Result file was created!!!");
		
		System.out.println("Deleting temporary files...");
		this.deleteTemporaryFiles(new File(this.t + "/f1"));
	    this.deleteTemporaryFiles(new File(this.t + "/f2"));
		System.out.println("Tempory files were deleted with success!!!");
		
		this.totalTime = this.endTime - this.startTime;
	}
	
	private void sortFirstRelation() throws IOException
	{
		this.initializeSaturatedBuffer();
		this.cleanBuffers();
		this.brs = null;
		this.written_recs = 0;
		
		this.getSizeOfF1();
		this.num_of_sublists1 = (int)(Math.ceil(this.f1_size/(double)(this.m)));
		System.out.println("#Sublists for f1: " + this.num_of_sublists1);
		
		this.create_f1_sublists();
		
		System.out.println("Creating sorting version of f1, the process may take some time.....");
		this.assembleSortedFile(this.num_of_sublists1, "f1" , this.a1 );
		System.out.println("Sorted version of f1 was created with success!!!");
		
		
	}
	
	private void sortSecondRelation() throws IOException
	{
		this.initializeSaturatedBuffer();
		this.cleanBuffers();
		this.brs = null;
		this.written_recs = 0;
		
		this.getSizeOfF2();
		this.num_of_sublists2 = (int)(Math.ceil(this.f2_size/(double)(this.m)));
		System.out.println("#Sublists for f2: " + this.num_of_sublists2);
		
		this.create_f2_sublists();
		
		System.out.println("Creating sorting version of f2, the process may take some time.....");
		this.assembleSortedFile(this.num_of_sublists2, "f2" , this.a2 );
		System.out.println("Sorted version of f2 was created with success!!!");
	}
	
	
	private void assembleSortedFile(int num_of_sublists,String file ,int index) throws IOException
	{
		if (num_of_sublists <= this.m)
		{
			brs = new BufferedReader[num_of_sublists];
			
			//Create bufferedReaders, as many as the sublists for file1. Also read the first Line.
			for(int i = 1; i <= num_of_sublists; i++)
			{
				try
				{
					brs[i-1] = new BufferedReader(new FileReader(this.t + "/"+ file +"/"+ "sublist" + i + ".csv"));
					this.buffers[i-1] = brs[i-1].readLine();
					
					if(buffers[i-1] == null)
					{
						this.saturated[i-1] = true;
						num_of_saturated_sublists++;
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
		            if (brs[i-1] != null) 
		            {
		            }
		        }
			}
			
			int nbuff = 0;
			
			if(file.equals("f1"))
			{
				this.starting_file_size = this.f1_size;
				nbuff = this.num_of_sublists1;
			}
			else if(file.equals("f2"))
			{
				this.starting_file_size = this.f2_size;
				nbuff = this.num_of_sublists2;
			}
			
			while(written_recs < this.starting_file_size)
			{
				//Find min from buffers and write it to file
				String min = this.buffers[0];
				int index_of_min = 0;
				for(int i = 0; i < nbuff; i++)
				{
					String [] temp_min = null;
					String [] temp_current = null;
					
					if(min != null)
					{
						temp_min = min.split(",");
					}
					else
					{
						for (int j = i + 1; j < nbuff; j++)
						{
							if(this.buffers[j] != null)
							{
								min = this.buffers[j];
								index_of_min = j;
								temp_min = min.split(",");
								break;
							}
						}
					}
					
					if(this.buffers[i] != null)
					{
						temp_current = this.buffers[i].split(",");
					}
					else
					{
						temp_current = new String[temp_min.length];
						temp_current[index] = "999999999999";
					}
					
					double a = Double.valueOf(temp_current[index]);
					double b = Double.valueOf(temp_min[index]);
					
					if(a < b)
					{
						min = this.buffers[i];
						index_of_min = i;
					}
					
				}
				
				//Write the min to disk temp file
				FileWriter fw = new FileWriter(this.t + "/"+ file + "/" + "sorted_"+ file + ".csv", true);   
		        StringBuilder sb = new StringBuilder();
				        
				
				sb.append(min + "\n");
				fw.write(sb.toString());
				fw.close();
				this.written_recs++;
				
				if(!this.saturated[index_of_min])
				{
					this.buffers[index_of_min] = brs[index_of_min].readLine();
					
					if(this.buffers[index_of_min] == null)
					{
						this.saturated[index_of_min] = true;
						num_of_saturated_sublists++;
						//System.out.println("Sublist was saturated!!!");
					}
				}
			}
			
		}
		else
	    {
			
		}
		
	
	}
	
	
	private void getSizeOfF1()
	{
		String line = "";
		BufferedReader br = null;

        try
        {

        	br = new BufferedReader(new FileReader(f1));
        	
            while ((line = br.readLine()) != null) 
            {

            	this.f1_size = Integer.valueOf(line);
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
	}
	
	private void getSizeOfF2()
	{
		String line = "";
		BufferedReader br = null;

        try
        {
        	br = new BufferedReader(new FileReader(f2));
        	
            while ((line = br.readLine()) != null) 
            {
            	this.f2_size = Integer.valueOf(line);
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
	}
	
	private void create_f1_sublists()
	{
		String line = "";
		BufferedReader br = null;
		
		boolean first_line = true;
		int counter = 0;
		String [] temp_sublist = new String[m];
		int sublist_counter = 0;

        try
        {

        	br = new BufferedReader(new FileReader(f1));
        	
            while (true) 
            {

            	line = br.readLine();
            	
            	if (first_line)
            	{
            		first_line = false;       		
            	}
            	else
            	{
            		if( line == null)
            		{
            			if(counter > 0)
            			{
            				sublist_counter++;
                		    
                		    String [] t = new String[counter];

                		    
            				for(int i = 0; i < t.length; i++)
            				{
            					t[i] = temp_sublist[i];
            				}
            				
                			this.bubbleSort(t, this.a1);
                			this.saveSublistToDisk(t, sublist_counter, "f1");
                		    
                		    this.bubbleSort(temp_sublist, this.a1);
                			this.saveSublistToDisk(temp_sublist, sublist_counter, "f1");
                			
                			temp_sublist = null;
                			temp_sublist = new String[m];
                			
                			
                			counter = 0;
            			}
            		    
            			
            			
            			break;
            		}
            		else
            		{
            			counter++;
                		
                		if (counter < this.m)
                		{
                			temp_sublist[counter - 1] = line;
                		}
                		else
                		{
                			
                			temp_sublist[counter - 1] = line;
                		    sublist_counter++;
                		    this.bubbleSort(temp_sublist, this.a1);
                			this.saveSublistToDisk(temp_sublist, sublist_counter, "f1");
                			
                			
                			
                			temp_sublist = null;
                			temp_sublist = new String[m];
                			counter = 0;
                			
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
	
	private void create_f2_sublists()
	{
		String line = "";
		BufferedReader br = null;
		
		boolean first_line = true;
		int counter = 0;
		String [] temp_sublist = new String [this.m];
		int sublist_counter = 0;

        try
        {

        	br = new BufferedReader(new FileReader(f2));
        	
            while (true) 
            {
            	
            	line = br.readLine();
            	
            	if (first_line)
            	{
            		first_line = false;       		
            	}
            	else
            	{
            		
            			if(line == null)
            			{
            				if(counter > 0)
            				{
            					sublist_counter++;
                				
                				String [] t = new String[counter];

                				for(int i = 0; i < t.length; i++)
                				{
                					t[i] = temp_sublist[i];
                				}
                				
                    			this.bubbleSort(t, this.a2);
                    			this.saveSublistToDisk(t, sublist_counter, "f2");
                    			
                    			temp_sublist = null;
                    			temp_sublist = new String [this.m];
                    			counter = 0;
            				}
            				
            				break;
            				
            			}
            			else
            			{
            				
            				counter++;
            				
            				
            				if (counter < this.m)
                    		{
                    			temp_sublist[counter - 1] = line; 
                    		}
                    		else
                    		{
            				
            				temp_sublist[counter - 1] = line;
                			sublist_counter++;
                			this.bubbleSort(temp_sublist, this.a2);
                			this.saveSublistToDisk(temp_sublist, sublist_counter, "f2");
                			
                			temp_sublist = null;
                			temp_sublist = new String [this.m];
                			counter = 0;
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
	
	private void saveSublistToDisk(String [] t , int number, String file) throws IOException
	{
		File folder = new File(this.t + "/"+ file);
		if(!folder.exists())
		{
			if(folder.mkdir())
			{
				System.out.println("Directory " + file + " was created with success!!!");
			}
			else
			{
				System.out.println("Problem during the creation of Directory " + file);
			}
			
		}
		
		FileWriter fw = new FileWriter(folder +"/" + "sublist" + number + ".csv", true);
        StringBuilder sb = new StringBuilder();
        
        for(int i = 0; i < t.length; i++)
        {
        	if(t[i] != null)
        	{
        		sb.append(t[i] + "\n");
        	}
      		
        }
      
        fw.write(sb.toString());
        fw.close();
	}
	
	
	
	private void mergeJoin() throws IOException
	{
		Boolean file_exhausted = false;
		
		String line1 = "";
		String line2 = "";
		
		//Open buffer reader for both sorted files 
		BufferedReader brf1 = new BufferedReader(new FileReader(this.t + "/f1/" + "sorted_f1.csv") );
		BufferedReader brf2 = new BufferedReader(new FileReader(this.t + "/f2/" + "sorted_f2.csv") );
		
		//Temp vector in order to not lose records
		Vector<String> temp_list = new Vector<String>();
		
		//Read a record from each file
		
		line1 = brf1.readLine();
		line2 = brf2.readLine();
		
		//As long as one of the list has not been exhausted
		while(!file_exhausted)
		{
			//Split the line you read if possible
			String [] f1_rec = line1.split(",");
			String [] f2_rec = line2.split(",");
			
			//Take the columns you want to compare as integers
			int left  = Integer.valueOf(f1_rec[this.a1]);
			int right = Integer.valueOf(f2_rec[this.a2]);
			
			//If both columns are the same
			if ( left == right )
			{
				//Write the result
				writeToResults(f1_rec, f2_rec);
				
				if(temp_list.size() < this.m-2)
				{
					temp_list.add(line2);
				}
				else
				{
					System.out.println("?");
				}
				
				//If you see that the next record is null then just finish, else save the new values	
				line2 = brf2.readLine();
				if(line2 == null)
				{
					file_exhausted = true;
				}
				else
				{
					f2_rec = line2.split(",");
					right = Integer.valueOf(f2_rec[this.a2]);
				}
				
				
			}
			else if( left < right )
			{
				int prev = left;
				
				//Read record from the left file;
				line1 = brf1.readLine();
				if(line1 == null)
				{
					file_exhausted = true;
					
				}
				else
				{
					f1_rec = line1.split(",");
					left  = Integer.valueOf(f1_rec[this.a1]);
					
				}
				
				if((prev == left) && temp_list.size() > 0)
				{
					for(int i = 0; i < temp_list.size(); i++)
					{
						String [] f2_rec_temp = temp_list.get(i).split(",");
						
						if(f1_rec[this.a1].equals(f2_rec_temp[this.a2]))
						{
							writeToResults(f1_rec, f2_rec_temp);
						}
						
					}				
				}											
				
			}
			else if (left > right)
			{
				//Read record from the right file
				line2 = brf2.readLine();
				if(line2 == null)
				{
					file_exhausted = true;
				}
				else
				{
					f2_rec = line2.split(",");
					right = Integer.valueOf(f2_rec[this.a2]);
					
					temp_list.clear();
					
				}
				
			}
		}
	}
	
	
	private void writeToResults(String a[], String b[]) throws IOException
	{
		FileWriter fw = new FileWriter(this.o, true);
        StringBuilder sb = new StringBuilder();
        
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
      
      sb.append('\n');
      fw.write(sb.toString());
      
      this.num_of_joins++;
      
      fw.close();
        
	}
	
	
	
	private void bubbleSort(String [] t, int index)
	{
		String swap = null;
		
		 for (int i = 0; i < t.length - 1; i++) 
		 {
		      for (int j = 0; j < t.length - i - 1; j++) 
		      {
		    	  
		    	 String [] temp1 = null;
		    	 
		    	 if(t[j] != null)
		    	 {
		    		 temp1 = t[j].split(",");
		    	 }
		    	 
		    	 if(t[j+1] != null)
		    	 {
		    		 String [] temp2 = t[j+1].split(",");
		    		 
		    		 if (Integer.valueOf(temp1[index]) > Integer.valueOf(temp2[index]))
			          {
			             swap   = t[j];
			             t[j]   = t[j+1];
			             t[j+1] = swap;
			          }
		    	 }
		      }
		 }
	}
	
	private void initializeSaturatedBuffer()
	{
		for(int i = 0; i < this.saturated.length; i++)
		{
			this.saturated[i] = false;
		}
	}
	
	private void cleanBuffers()
	{
		for(int i = 0; i < this.buffers.length; i++)
		{
			this.buffers[i] = "";
		}
	}
	
	private boolean sublistsSaturated()
	{
		int counter = 0;
		
		boolean was_saturated = false;
		
		for(int i = 0; i < this.saturated.length; i++)
		{
			if (this.saturated[i] = true)
			{
				counter++;
			}
		}
		
		if(counter == this.saturated.length)
		{
			was_saturated =  true;
		}
		else
		{
			was_saturated =  false;
		}
		
		return was_saturated;
		
	}
	
	private boolean deleteTemporaryFiles(File path)
	{
		
		if(path.exists()) 
		{
		      File[] sub_files = path.listFiles();
		      
		      for(int i=0; i<sub_files.length; i++) 
		      {
		         if(sub_files[i].isDirectory()) 
		         {
		        	 deleteTemporaryFiles(sub_files[i]);
		         }
		         else 
		         {
		           sub_files[i].delete();
		         }
		      }
		    }
		
		    return( path.delete() );
	}
	
	private void clearResultFile() throws IOException
	{
		FileWriter fw = new FileWriter(this.o);
		fw.write("");
	}
	
	private void printExecutionStats()
	{
		System.out.println();
		System.out.println();
		System.out.println("____________Execution Stats____________");
		System.out.println("# of joins : " + this.num_of_joins);
		System.out.println("Execution time of Join Algorithm: " + this.totalTime + " nanoseconds");
		System.out.println("_______________________________________");
	}
	
}

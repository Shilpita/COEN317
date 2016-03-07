

	import java.rmi.*;
	import java.rmi.registry.*;
	import java.rmi.server.*;
	import java.io.DataInputStream;
	import java.util.Scanner;

	public class RaftClient
	{
	    public static void main(String args[])
	    {
	        String name = "Node-" + System.currentTimeMillis();
	        try
	        {
	            int ans;
	            int x = 0;
	            int y = 0;
	            Registry registry = LocateRegistry.getRegistry();
		    RaftRequestInterface stub = (RaftRequestInterface) registry.lookup("ArithmeticModule");
	            
	            Scanner in = new Scanner(System.in);

	            System.out.print("Enter Number 1 : ");
	            x = in.nextInt();

	            System.out.print("Enter Number 2 : ");
	            y = in.nextInt();
	           

	           // int sum = stub.find_sum(x,y);
	           // System.out.println(sum);
	        }
	        catch(Exception e) { 
	        e.printStackTrace();}
	    }
	}    



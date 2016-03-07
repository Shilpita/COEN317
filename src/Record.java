

import java.io.Serializable;

public class Record implements Serializable {
		public int x;
		public int y;
		
		
		public Record(int x1, int y1) {
			// TODO Auto-generated constructor stub
			x= x1;
			y= y1;
		}

		public void printValues(){
		System.out.println("\n Value of x: "+ x +"\n");
		System.out.println("\n Value of y: "+ y +"\n");
		}
		public static void main(String[] args) {
			//Record  record = new Record ();

		   // System.out.println ("record = " + record);

		    //Record record = new Record(x1,y1);
		}
}

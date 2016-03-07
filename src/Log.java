

import java.util.*;

public class Log {
	Vector<LogEntry> log ;
	
    public Log (){
    	this.log   = new Vector<LogEntry>();
    }
	
    public boolean commitLogEntry(String leaderID , LogEntry newLogEntry , LogEntry lastLogEntry){
    	log.addElement(newLogEntry);
		return true;
    }

}


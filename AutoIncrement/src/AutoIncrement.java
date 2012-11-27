import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class AutoIncrement extends EvalFunc<Long>
{
	Long _index; 
	
    public AutoIncrement()
    {    
    	_index = -1L;
    }         

    public Long exec(Tuple input) throws IOException {
        ++ _index;
        return _index;    	
    }        
}

/**
 * Created by IntelliJ IDEA.
 * User: Dima
 * Date: 18.11.12
 * Time: 15:41
 * To change this template use File | Settings | File Templates.
 */
public class CodeResult
{
    private int _CodeID;

    private Integer _TZoneID;

    private  boolean  _IsSuccessed;
    
    private String _Number;

    public CodeResult(String Number, int CodeID, Integer TZoneID, boolean IsSuccessed)
    {
        _CodeID =  CodeID;
        _TZoneID = TZoneID;
        _IsSuccessed = IsSuccessed;
        _Number =     Number;
    }

    public String GetNumber()
    {
        return    _Number;
    }

    public int GetCodeID()
    {
        return    _CodeID;
    }

    public Integer GetTZoneID()
    {
        return    _TZoneID;
    }

    public boolean IsSuccessed()
    {
        return    _IsSuccessed;
    }

}

/**
 * Created by IntelliJ IDEA.
 * User: Dima
 * Date: 17.11.12
 * Time: 19:33
 * To change this template use File | Settings | File Templates.
 */
public class CodesRow
{
    private int m_MinLen;
    private int m_MaxLen;

    private int m_CodeID;
    private Integer m_TZoneID;
    private int m_LeftDel;
    private String m_LeftAdd;

    public CodesRow(int MinLen, int MaxLen, int CodeID, Integer TZoneID, int LeftDel, String LeftAdd)
    {
        m_MinLen = MinLen;
        m_MaxLen = MaxLen;
        m_CodeID = CodeID;
        m_TZoneID = TZoneID;
        m_LeftDel = LeftDel;
        m_LeftAdd = LeftAdd;
    }

    public boolean CheckLength(String Number)
    {
        return m_MinLen <= Number.length() && Number.length() <= m_MaxLen;
    }

    public CodeResult Process(String Number, int CodeID, Integer TZoneID)
    {
        if (m_LeftDel > 0)
        {
            Number = Number.substring(m_LeftDel);
        }
        if (m_LeftAdd != null && !m_LeftAdd.isEmpty())
        {
            Number = m_LeftAdd + Number;
        }
        CodeResult Res = new CodeResult(Number ,m_CodeID, m_TZoneID, TZoneID != null);
        return Res;
    }



}
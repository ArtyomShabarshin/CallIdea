import java.util.List;
import java.sql.*;
import java.util.*;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

public class BAnalysis extends EvalFunc<String>
{
    private final int m_IterationLimit = 1000;

    private final String m_SProcName = "ewr_.p_refresh_codes";

    private final byte m_BaseTZoneMapID = 0;

    private final int m_IncomingRecordTypeID = 1;

    private final Boolean m_IsCaseSensitive = false;

    private String m_UndefNumber = "C";

    private java.util.Map<CodesKey, java.util.HashMap<String, List<CodesRow>>> m_CodesTable;

    private java.util.HashSet<String> m_PrefixSet;

    public BAnalysis() throws SQLException, ClassNotFoundException
    {    	
    	String ConStr = "jdbc:sqlserver://localhost;databaseName=EWRating;integratedSecurity=false;user=sa;password=m367st;";

    	if (!m_IsCaseSensitive)
        {
            m_UndefNumber = m_UndefNumber.toLowerCase();
        }
    	
        LoadTable(ConStr);
    }    
  
    private int LoadTable(String ConStr) throws SQLException, ClassNotFoundException
    {
        int count = 0;
        ResultSet DBDataSet = null;
        Connection Con = null;
        
        try
        {
            m_CodesTable = new java.util.HashMap<CodesKey, HashMap<String, List<CodesRow>>>();
            m_PrefixSet = new java.util.HashSet<String>();

            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
      		Con = DriverManager.getConnection(ConStr);
      		
            Statement Stm = Con.createStatement();
            String Query =  "exec " + m_SProcName;
            DBDataSet = Stm.executeQuery(Query);

            while (DBDataSet.next())
            {
                String Code = DBDataSet.getString("Code");
                if (!m_IsCaseSensitive)
                {
                    Code = Code.toLowerCase();
                }
                m_PrefixSet.add(Code);
                Object LeftDelVal = DBDataSet.getObject("LeftDel");
                final int defaultLeftDel = 0;
                int LeftDel = LeftDelVal == null ? Integer.valueOf(defaultLeftDel) : ((Short) LeftDelVal).intValue();
                String LeftAdd = (String) DBDataSet.getObject("LeftAdd");
                Integer TZoneID = (Integer) DBDataSet.getObject("TZoneID");
                int CodeID = DBDataSet.getInt("CodeID");
                byte TZoneMapID = DBDataSet.getByte("TZoneMapID");
                int ServiceGroupID = DBDataSet.getInt("ServiceGroupID");
                int MinLen = DBDataSet.getInt("MinLen");
                int MaxLen = DBDataSet.getInt("MaxLen");

                CodesKey CurrentKey = new CodesKey(ServiceGroupID, TZoneMapID);
                CodesRow CurrentRow = new CodesRow(MinLen, MaxLen, CodeID, TZoneID, LeftDel, LeftAdd);
                if (!m_CodesTable.containsKey(CurrentKey))
                {
                    m_CodesTable.put(CurrentKey, new  HashMap<String, List<CodesRow>>());
                }
                HashMap<String, List<CodesRow>> Codes = m_CodesTable.get(CurrentKey);
                if (!Codes.containsKey(Code))
                {
                    Codes.put(Code, new ArrayList<CodesRow>());
                }
                Codes.get(Code).add(CurrentRow);

                ++count;
            }//while

        }
        finally
        {
            if (DBDataSet != null)
            {
                DBDataSet.close();
            }
            if (Con != null && !Con.isClosed())
            {               
               Con.close();
            }
        }

        return count;
    }

    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        String number = ((DataByteArray)input.get(0)).toString();
        
        byte RecordTypeID = 0;
        byte TZoneMapID = 1;
        int ServiceGroupID = 0;
        
        try {
			
	        CodeResult res = ProcessNumber(RecordTypeID, TZoneMapID, ServiceGroupID, number);
	        return res.GetNumber();
	        
		} catch (Exception e) {		
			return "error";
		}
        
    }
    
    public CodeResult ProcessNumber(byte RecordTypeID, byte TZoneMapID, int ServiceGroupID, String Number) throws Exception
    {
        final String ErrorMsg = "BAnalysis Error. BAnalysis.ProcessNumber: Number = %s";
        
        Integer TZoneID = Integer.MIN_VALUE;
        int CodeID = Integer.MIN_VALUE;
        CodeResult ProcessRes = new  CodeResult(Number, CodeID, TZoneID, false);

        if (Number == null)
        {
            Number = "";
        }

        Number = Number.trim();

        if (!m_IsCaseSensitive)
        {
            Number = Number.toLowerCase();
        }

        if (Number.length() == 0 && RecordTypeID == m_IncomingRecordTypeID)
        {
            Number = m_UndefNumber;
        }

        for (int i = m_IterationLimit; i > 0; --i)
        {
            CodesRow ResultCode = SinglePass(TZoneMapID, ServiceGroupID, Number);
            if (ResultCode == null)
            {
                if (RecordTypeID == m_IncomingRecordTypeID && Number != m_UndefNumber)
                {
                    Number = m_UndefNumber;
                }
                else
                {
                    String Msg =  String.format(ErrorMsg, Number);
                    throw new Exception(Msg);
                }
            }
            else
            {
                ProcessRes =  ResultCode.Process(Number, CodeID, TZoneID);
                if (ProcessRes.IsSuccessed())
                {
                    return ProcessRes;
                }
            }
        }//for

        return ProcessRes;
    }

    private CodesRow SinglePass(Byte TZoneMapID, int ServiceGroupID, String Number)
    {
        String TargetNumber = Number;

        HashMap<String, List<CodesRow>> Codes;
        HashMap<String, List<CodesRow>> DefaultCodes;

        CodesRow result = null;

        if (TZoneMapID != null)
        {
            CodesKey Key = new CodesKey(ServiceGroupID, TZoneMapID);
            Codes = m_CodesTable.get(Key);         
        }
        else
        {
            Codes = null;
        }

        CodesKey DefaultKey = new CodesKey(ServiceGroupID, m_BaseTZoneMapID);
        DefaultCodes = m_CodesTable.get(DefaultKey);

        if (Codes != null || DefaultCodes != null)
        {
            while (TargetNumber.length() > 0)
            {
                if (m_PrefixSet.contains(TargetNumber))
                {
                    result = FindRow(Number, TargetNumber, Codes);
                    if (result != null) break;

                    result = FindRow(Number, TargetNumber, DefaultCodes);
                    if (result != null) break;
                }

                TargetNumber = TargetNumber.substring(0,TargetNumber.length() - 1);
            }
        }

        return result;
    }

    private CodesRow FindRow(String OriginalNumber, String CheckedNumber, HashMap<String, List<CodesRow>> Codes)
    {
        List<CodesRow> CodesRows;
        CodesRows =   Codes.get(CheckedNumber);
        if (Codes != null && CodesRows != null)
        {
            for (int i = 0; i < CodesRows.size();++i )
            {
                CodesRow CurrrentRow = CodesRows.get(i);
                if (CurrrentRow.CheckLength(OriginalNumber))
                {
                    return CurrrentRow;
                }
            }
        }
        return null;
    }
}
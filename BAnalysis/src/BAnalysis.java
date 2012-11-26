import java.util.List;
import java.sql.*;
import java.util.*;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

public class BAnalysis extends EvalFunc<String>
{
    //РќРѕРјРµСЂ, РїРѕРґРјРµРЅСЏСЋС‰РёР№ Р‘-РЅРѕРјРµСЂ РЅРµ РЅР°Р№РґРµРЅРЅС‹Р№ РІ РЅР°РїСЂР°РІР»РµРЅРёСЏС…
    private String m_UndefNumber = "C";

    //РћРіСЂР°РЅРёС‡РµРЅРёРµ РЅР° РєРѕР»РёС‡РµСЃС‚РІРѕ РёС‚РµСЂР°С†РёР№
    private final int m_IterationLimit = 1000;

    // Р�РјСЏ С…СЂР°РЅРёРјРѕР№ РїСЂРѕС†РµРґСѓСЂС‹, Р·Р°РіСЂСѓР¶Р°СЋС‰Р°СЏ С‚Р°Р±Р»РёС†Сѓ Р‘-Р°РЅР°Р»РёР·Р°
    private final String m_SProcName = "ewr_.p_refresh_codes";

    // Р‘Р°Р·РѕРІР°СЏ TZoneMap
    private final byte m_BaseTZoneMapID = 0;

    // RecordTypeID РґР»СЏ РІС…РѕРґСЏС‰РµРіРѕ Р·РІРѕРЅРєР°
    private final int m_IncomingRecordTypeID = 1;

    // Р§СѓРІСЃС‚РІРёС‚Р»СЊРЅРѕСЃС‚СЊ Рє СЂРµРіРёСЃС‚СЂСѓ РїСЂРё СЃСЂР°РІРЅРµРЅРёРё Р±-РЅРѕРјРµСЂР° Рё РєРѕРґР°
    private final Boolean m_IsCaseSensitive = false;

    // РҐСЌС€-С‚Р°Р±Р»РёС†Р° РєРѕРґРѕРІ. РќР° РІС…РѕРґРµ Code, ServiceGroupID, TZoneMapID, РЅР° РІС‹С…РѕРґРµ СЃРїРёСЃРѕРє СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ РѕС‚Р»РёС‡Р°СЋС‰РёС…СЃСЏ РїРѕ MinLen, MaxLen.
    private java.util.Map<CodesKey, java.util.HashMap<String, List<CodesRow>>> m_CodesTable;

    // РњРЅРѕР¶РµСЃС‚РІРѕ РїСЂРµС„РёРєСЃРѕРІ РґР»СЏ РѕС‚СЃРµС‡РєРё РЅРµРїРѕРґС…РѕРґСЏС‰РёС… РІР°СЂРёР°РЅС‚РѕРІ
    private java.util.HashSet<String> m_PrefixSet;


    // РљРѕРЅСЃС‚СЂСѓРєС‚РѕСЂ
    public BAnalysis(/*String ConStr, Configuration Config*/)    throws   SQLException
    {
    	String ConStr = "jdbc:sqlserver://localhost;databaseName=Billing;integratedSecurity=false;user=sa;password=m367st;";
        //ErrorManager.ErrorProcessor.OnWarning("BAnalysis", 10123);
        if (!m_IsCaseSensitive)
        {
            m_UndefNumber = m_UndefNumber.toLowerCase();
        }
        LoadTable(ConStr);

        //ErrorManager.ErrorProcessor.OnWarning("BAnalysis loaded. " + count + " rows.", 10124);
    }    
  
    ///Р—Р°РіСЂСѓР·РёС‚СЊ codes  
    private int LoadTable(String ConStr)  throws   SQLException
    {
        int count = 0;
        ResultSet DBDataSet = null;
        Connection Con = null;
        try
        {
            m_CodesTable = new java.util.HashMap<CodesKey, HashMap<String, List<CodesRow>>>();
            m_PrefixSet = new java.util.HashSet<String>();
            try
            {
                //Loading the driver...
                Class.forName( "com.microsoft.sqlserver.jdbc.SQLServerDriver" );
            }
            catch( java.lang.ClassNotFoundException e )
            {
                return -1;
            }

            //РЅСѓР¶РЅРѕ РїРѕС…РѕСЂРѕС€РµРјСѓ РїРµСЂРµРґР°РІР°С‚СЊ СЃР°РјРѕ СЃРѕРµРґРёРЅРµРЅРёРµ, С‡С‚РѕР±С‹ РїРѕСЃС‚РѕСЏРЅРЅРѕ РµРіРѕ РЅРµ РѕС‚РєСЂС‹РІР°С‚СЊ
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
      		Con = DriverManager.getConnection(ConStr);
      		
            Statement Stm = Con.createStatement();
//            IDataBaseWrapperCommand Command;
//            Command = dbw.CreateCommand(m_SProcName, System.Data.CommandType.StoredProcedure);
//            IDataBaseWrapperDataSet DBDataSet = Command.FillDataSet();
            String Query =  "exec "   + m_SProcName;
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
                int MinLen = DBDataSet.getByte("MinLen");
                int MaxLen = DBDataSet.getByte("MaxLen");

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
        //РµСЃР»Рё РЅРµ РЅР°Р№РґРµРЅРѕ РїРѕР»Рµ РїРѕ РёРјРµРЅРё  (РїРѕРєР° СЌС‚РѕС‚ РѕР±СЂР°Р±РѕС‚С‡РёРє Р·Р°РєРѕРјРµРЅС‚СЂР°РёР»)
//        catch (SQLException ex)
//        {
//            ex.printStackTrace();
//        }
        //РјРѕР¶РµС‚ РїСЂРёРґС‚Рё DBException РїСЂРё РїРѕР»СѓС‡РµРЅРёРё РґР°РЅРЅС‹С…, РјРѕР¶РµС‚ РїСЂРёР№С‚Рё Reference null РїСЂРё РєРѕРЅРІРµСЂСЃРёРё РїРѕР»РµР№, РєРѕС‚РѕСЂС‹Рµ РЅРµ РґРѕР»Р¶РЅС‹ Р±С‹С‚СЊ null
        catch (Exception ex)
        {
            ex.printStackTrace(); //
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
        byte TZoneMapID = 0;
        int ServiceGroupID = 0;
        
        try {
			ProcessNumber(RecordTypeID, TZoneMapID, ServiceGroupID, number);
		} catch (Exception e) {			
		}
        
        return number;
    }
    
    // РџСЂРёРјРµРЅРµРЅРёРµ РїСЂР°РІРёР» Р‘-Р°РЅР°Р»РёР·Р°
    public CodeResult ProcessNumber(byte RecordTypeID, byte TZoneMapID, int ServiceGroupID, String Number) throws   Exception
    {
        final String ErrorMasg = "BAnalysis Error. BAnalysis.ProcessNumber: Number = %s";
        
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

        int i;
        for (i = m_IterationLimit; i > 0; --i)
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
                    String Msg =  String.format(ErrorMasg, Number);
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

        if (i <= 0)
        {
            String Msg =  String.format(ErrorMasg, Number);
            throw new Exception(Msg);
        }//if
        return ProcessRes;
    }//function

    /// <summary>
    /// Р�С‚РµСЂР°С†РёСЏ РїРѕРёСЃРєР° РїСЂР°РІРёР»Р° Р‘-Р°РЅР°Р»РёР·Р°
    /// </summary>
    /// <param name="TZoneMapID"></param>
    /// <param name="ServiceGroupID"></param>
    /// <param name="Number"></param>
    /// <returns> РџРѕРґС…РѕРґСЏС‰РµРµ РїСЂР°РІРёР»Рѕ </returns>
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
    }//function


   /// Р�С‰РµС‚ РїРѕРґС…РѕРґСЏС‰РёРµ РїСЂР°РІРёР»Р° РґР»СЏ РїСЂРµС„РёРєСЃР°
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

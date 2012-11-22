public  class CodesKey
{
	private int m_ServiceGroupID;
    private byte m_TZoneMapID;

    public CodesKey(int ServiceGroupID, byte TZoneMapID)
    {
        m_ServiceGroupID = ServiceGroupID;
        m_TZoneMapID = TZoneMapID;
    }

    @Override()
    public boolean equals(Object other)
    {
        if(this == other)
            return true;
        if(!(other instanceof CodesKey))
            return false;
        CodesKey that = (CodesKey)other;
        return m_ServiceGroupID == that.m_ServiceGroupID && m_TZoneMapID == that.m_TZoneMapID;
    }

    @Override()
    public int hashCode()
    {
        return m_ServiceGroupID ^ m_TZoneMapID;
    }

}

/* ��������� ���� �� �������� (dbo.TTHuaweiUMTS_Plain) */
raw = LOAD '/user/hadoop/TTHuaweiUMTS_Plain.csv' USING PigStorage(';');

/* �������� ������ ������ ��� ������� */
clean1 = FOREACH raw GENERATE $0 as RecordType, $5 as ANumber, $11 as BNumber;

/* ��������� ������ ��������� ������ */
clean2 = FILTER clean1 BY RecordType == 0;

/* �������� �-������ */
calls = FOREACH clean2 GENERATE ANumber, BNumber;

/* ��������� ���������� */
STORE calls INTO '/user/hadoop/res' USING PigStorage('\t');
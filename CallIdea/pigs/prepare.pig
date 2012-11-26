raw = LOAD '/user/hadoop/cdr/TTHuaweiUMTS_Plain.csv' USING PigStorage(';');

clean1 = FOREACH raw GENERATE $0 as RecordType, $5 as ANumber, $11 as BNumber;
clean2 = FILTER clean1 BY RecordType == 0;
calls = FOREACH clean2 GENERATE ANumber, BAnalysis(BNumber) as BNumber;

anumbers = FOREACH calls GENERATE ANumber as Number;
bnumbers = FOREACH calls GENERATE BNumber as Number;
allnumbers = UNION anumbers, bnumbers;

numbers = DISTINCT allnumbers;

numbers_group = GROUP numbers ALL;
numbers_count = FOREACH numbers_group GENERATE COUNT(numbers);

STORE calls INTO '/user/hadoop/res/calls' USING PigStorage('\t');
STORE numbers_count INTO '/user/hadoop/res/count' USING PigStorage('\t');
STORE numbers INTO '/user/hadoop/res/numbers' USING PigStorage('\t');
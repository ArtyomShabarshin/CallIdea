raw = LOAD '/user/callidea/cdr/TTHuaweiUMTS_Plain.csv' USING PigStorage('\t');

clean = FILTER raw BY $0 == 0; 
calls = FOREACH clean GENERATE $5 as ANumber, BAnalysis($11) as BNumber;

anumbers = FOREACH calls GENERATE ANumber as Number;
bnumbers = FOREACH calls GENERATE BNumber as Number;
numbers = DISTINCT (UNION anumbers, bnumbers);

numbers_index = FOREACH numbers GENERATE Number, AutoIncrement() as NumberIndex;

calls_index_tmp1 = JOIN numbers_index BY Number, calls BY ANumber;
calls_index_tmp2 = JOIN numbers_index BY Number, calls_index_tmp1 BY BNumber;
calls_index = FOREACH calls_index_tmp2 GENERATE $4 as ANumber, $5 as BNumber, $3 as ANumberIndex, $1 as BNumberIndex;

numbers_group = GROUP numbers ALL;
numbers_count = FOREACH numbers_group GENERATE COUNT(numbers);

STORE calls_index INTO '/user/callidea/res/calls' USING PigStorage('\t');
STORE numbers_count INTO '/user/callidea/res/count' USING PigStorage('\t');
STORE numbers_index INTO '/user/callidea/res/numbers' USING PigStorage('\t');
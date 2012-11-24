REGISTER ProcessNumber.jar

/* загружаем файл со звонками (dbo.TTHuaweiUMTS_Plain) */
raw = LOAD '/user/hadoop/cdr/data.csv' USING PigStorage(';');

/* выделяем только нужные нам колонки */
clean1 = FOREACH raw GENERATE $0 as RecordType, $5 as ANumber, $11 as BNumber;

/* оставляем только исходящие звонки */
clean2 = FILTER clean1 BY RecordType == 0;

/* проводим б-анализ */
calls = FOREACH clean2 GENERATE ANumber, BAnalysis(BNumber);

/* получаем список номеров */
anumbers = FOREACH calls GENERATE ANumber as Number;
bnumbers = FOREACH calls GENERATE BNumber as Number;
allnumbers = UNION anumbers, bnumbers;
numbers = DISTINCT allnumbers;

/* получаем количество номеров */
numbers_group = GROUP numbers ALL;
numbers_count = FOREACH numbers_group GENERATE COUNT(numbers);

/* выгружаем результаты */
STORE calls INTO '/user/hadoop/cdr/calls' USING PigStorage('\t');
STORE numbers_count INTO '/user/hadoop/cdr/count' USING PigStorage('\t');
STORE numbers INTO '/user/hadoop/cdr/numbers' USING PigStorage('\t');
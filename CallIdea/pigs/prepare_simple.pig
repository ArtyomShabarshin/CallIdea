/* загружаем файл со звонками (dbo.TTHuaweiUMTS_Plain) */
raw = LOAD '/user/hadoop/TTHuaweiUMTS_Plain.csv' USING PigStorage(';');

/* выделяем только нужные нам колонки */
clean1 = FOREACH raw GENERATE $0 as RecordType, $5 as ANumber, $11 as BNumber;

/* оставляем только исходящие звонки */
clean2 = FILTER clean1 BY RecordType == 0;

/* проводим б-анализ */
calls = FOREACH clean2 GENERATE ANumber, BNumber;

/* выгружаем результаты */
STORE calls INTO '/user/hadoop/res' USING PigStorage('\t');
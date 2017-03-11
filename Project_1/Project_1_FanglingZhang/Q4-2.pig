cus_raw = load '/user/hadoop/input/customers.csv' USING PigStorage(',')
 AS(cusId,name,age,countrycode,salary );
trans_raw = load '/user/hadoop/input/Transaction.csv' USING PigStorage(',')
 AS(transId,cusId,transTotal,transnumItem, transDec );

 cus1= foreach cus_raw generate cusId, name,salary;
 trans1= foreach trans_raw generate transId, cusId,transTotal,transnumItem;
 trans2= group trans1 by cusId;
 trans3= foreach trans2 generate group as cusId, COUNT(trans1) as NumOfTransactions, SUM(trans1.transTotal) as TotalSum, MIN(trans1.transnumItem) as MinItems;
 cus2= join cus1 by cusId, trans3 by cusId using 'replicated'; 
 cus3= foreach cus2 generate $0, name, salary, NumOfTransactions, TotalSum, MinItems;
 STORE cus3 INTO 'pigquery2/query2.csv' using PigStorage(',');



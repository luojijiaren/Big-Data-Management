 cus_raw = load '/user/hadoop/input/customers.csv' USING PigStorage(',')
 AS(cusId,name,age,countrycode,salary );
trans_raw = load '/user/hadoop/input/Transaction.csv' USING PigStorage(',')
 AS(transId,cusId,transTotal,transnumItem, transDec );
 cus1= foreach cus_raw generate cusId, name;
 trans1= foreach trans_raw generate transId, cusId;
 trans2= group trans1 by cusId;
 trans3= foreach trans2 generate group as cusId, COUNT(trans1) as numofTrans;
 trans4= group trans3 all;
 trans5= foreach trans4 generate MIN(trans3.numofTrans) as minTrans;
 trans6= filter trans3 by numofTrans==trans5.minTrans;
cus2= join cus1 by cusId, trans6 by cusId ;
 cus3= foreach cus2 generate name, numofTrans; 
 STORE cus3 into 'query1.csv' using PigStorage(',');




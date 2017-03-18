A = LOAD 'customers' USING PigStorage(',') as (ID: chararray, Name:chararray,
                                                Age:int, cc:int, s:float);
B = FOREACH A GENERATE ID, cc;
C = GROUP B BY cc;
D = FOREACH C GENERATE group as cc, COUNT(B) as num;
E = FILTER D BY num < 2000 OR num > 5000;
F = FOREACH E GENERATE cc;
dump F;
STORE F INTO 'script.out0';

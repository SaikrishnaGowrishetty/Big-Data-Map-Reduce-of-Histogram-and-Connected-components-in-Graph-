E = LOAD '$P' USING PigStorage(',') AS (RI:chararray, BI:chararray, GI:chararray);
R = FOREACH E GENERATE CONCAT('1 ',RI) AS x,1;
B = FOREACH E GENERATE CONCAT('2 ',BI) AS x,1;
G = FOREACH E GENERATE CONCAT('3 ',GI) AS x,1;
Comb = UNION R,B,G;
Grp = group Comb by x;
Res = FOREACH Grp GENERATE group,SUM(Comb.$1);
STORE Res INTO '$O' USING PigStorage ('\t');
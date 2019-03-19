M_MATRIX = LOAD '$M' USING PigStorage(',') AS (i:long , j:long, value_m:double);
N_MATRIX = LOAD '$N' USING PigStorage(',') AS (x:long, y:long, value_n:double);
TEMP_MATRIX = JOIN M_MATRIX BY j FULL OUTER, N_MATRIX BY x;
MATRIX = FOREACH TEMP_MATRIX GENERATE i,y,(value_m * value_n ) AS PRODUCT;
GMATRIX = GROUP MATRIX by (i, y);
FINAL_MATRIX = FOREACH GMATRIX GENERATE group, SUM(MATRIX.PRODUCT);
ORDERED_MATRIX = ORDER FINAL_MATRIX by group;
STORE ORDERED_MATRIX INTO '$O' USING PigStorage (',');
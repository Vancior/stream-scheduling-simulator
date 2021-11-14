param n, integer, >0;
param s, integer, >0;
param source, integer, >0;
param sink, integer, >0;

set I := 1..n;
set J := 1..n;

param f{i in I, j in J}, >= 0;

var x{i in I}, binary;
var y{i in I, j in J}, binary;

s.t. occupy: sum{i in I} x[i] <= s;
s.t. xor1{i in I, j in J}: y[i,j] <= x[i] + x[j];
s.t. xor2{i in I, j in J}: y[i,j] >= x[i] - x[j];
s.t. xor3{i in I, j in J}: y[i,j] >= x[j] - x[i];
s.t. xor4{i in I, j in J}: y[i,j] <= 2 - x[i] - x[j];
s.t. source_place{i in 1..source}: x[i] = 1;
s.t. sink_place{i in (n-sink+1)..n}: x[i] = 0;

minimize obj: sum{i in I, j in J} f[i,j] * y[i,j];

solve;

printf "%d\n", obj;
printf{i in I} "%d ", x[i];
printf "\n";
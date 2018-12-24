#!/bin/bash

awk -F, '
	{
		gsub(/^./,""); 
		gsub(/.$/,""); 
		a[$1]+=$2; 
		b[$1]++;
	}
	
	END {
		for(i in a) print i, 1.0*a[i]/b[i];
	}' input | 

sort -n +1


#!/bin/bash

awk -F, '
	{
		gsub(/^./,""); 
		gsub(/.$/,""); 
		a[$1]++; 
		a[$2]++
	}
	
	END {
		for(i in a) print i, a[i];
	}' input | 

sort -n +1


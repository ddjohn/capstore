#!/bin/bash

FILE=$1

best_route() {
	pig -x local -param FILE=$FILE -param ORIGIN=$1 -param MIDDLE=$2 -param DEST=$3 -param ORIGINDATE=$4 -param MIDDLEDATE=$5 -f g3q2.pig | grep "^("
}

best_route CMI ORD LAX 2008-03-04 2008-03-06
best_route JAX DFW CRP 2008-09-09 2008-09-11
best_route SLC BFL LAX 2008-04-01 2008-04-03
best_route LAX SFO PHX 2008-07-12 2008-07-14
best_route DFW ORD DFW 2008-06-10 2008-06-12
best_route LAX ORD JFK 2008-01-01 2008-01-03

#!/bin/bash

wd=/user/vannerom/MET/decafMET/

cd $wd

source setup_lcg.sh

cd $wd/analysis/

echo $PYTHONPATH

python run.py -i $1

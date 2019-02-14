#!/bin/bash

CLEANER_FOLDER=$HOME/cdcol-cleaner
PYTHON=$HOME/anaconda2/bin/python

echo "$(date) RUNNING CDCOL-CLEANER FOLDER"
source $HOME/.bashrc
cd $CLEANER_FOLDER
$PYTHON run.py
echo "$(date) DONE"
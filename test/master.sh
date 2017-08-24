#!/usr/bin/env bash

git checkout master;
git merge --no-commit --no-ff 0.1.x;
git reset -- test;
rm -rf test/;

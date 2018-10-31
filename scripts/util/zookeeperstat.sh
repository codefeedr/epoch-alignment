#!/usr/bin/env bash

echo stat | nc athens.ewi.tudelft.nl 2181
echo cons | nc athens.ewi.tudelft.nl 2181

echo stat | nc localhost 2181

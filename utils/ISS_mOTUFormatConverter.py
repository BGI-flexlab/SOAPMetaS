#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
    File:	ISS_mOTUFormatConverter.py
    Author:	Shixu He
    Email:	heshixu@genomics.cn
    Date:	2019-08-23
    ------
    (description)
    ------
    Version:
'''

import re
import sys

def getGCFTaxIDDict(markerSpeListFile):
    gcfDict = dict()
    with open(markerSpeListFile, 'rt', encoding='utf-8') as _inf:
        while True:
            lines = _inf.readlines(65535)
            if not lines:
                break
            for line in lines:
                lineEle = line.rstrip().split("\t")
                gcfDict[lineEle[1]] = lineEle[4]
    return gcfDict

def getGCFAbunDict(abundanceFile):
    abunDict = dict()
    with open(abundanceFile, 'rt', encoding='utf-8') as _inf:
        while True:
            lines = _inf.readlines(65535)
            if not lines:
                break
            for line in lines:
                lineEle = re.split(r"\s+", line.rstrip())
                gcf = re.findall(r"GC[AF]_\d+\.\d", lineEle[0])[0]
                abunDict[gcf] = float(lineEle[1])
    return abunDict

def main():
    if (len(sys.argv) != 4):
        print("usage: python3 " + sys.argv[0] + " <SelectedMarkerSpecies.list> <ISS_abundance.txt> <outputFileName>")
        sys.exit(0)
    taxIDDict = getGCFTaxIDDict(sys.argv[1])
    abunDict = getGCFAbunDict(sys.argv[2])
    with open(sys.argv[3], 'wt', encoding='utf-8') as _wf:
        _wf.write("taxaid (species taxID)\tabundance")
        _wf.write("\n")
        for gcf, abun in abunDict.items():
            if gcf not in taxIDDict:
                print("error: GCF accession " + gcf + " has no record in SelectedMarkerSpecies.list file")
                sys.exit(1)
            _wf.write("{0}\t{1}\n".format(taxIDDict.get(gcf), abun))

if __name__ == "__main__":
    main()

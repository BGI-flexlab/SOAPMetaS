#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
    File:	MetaPhlAn2_mpa_pkl_extract.py
    Author:	Shixu He
    Email:	heshixu@genomics.cn
    Date:	2019-10-06
    ------
    (description)
    ------
    Version:
'''

import yaml
import pickle
import json
import re
import sys, os
import bz2
import gzip
import argparse
from Bio import SeqIO
from Bio.SeqUtils import GC

def markerSeqFnaRead():
    geneInfoDict = dict()

    with bz2.open("mpa_v20_m200.fna.bz2", "rt") as bzHandle:
        for record in SeqIO.parse(bzHandle, "fasta"):
            geneInfoDict[record.id] = {"len": len(record), "gc": GC(record.seq), "nname": record.id}
    
    with open("MetaPhlAn2_marker.matrix", "wt") as _ouf:
        count = 1
        for k, v in geneInfoDict.items():
            _ouf.write("{0}\t{1}\t{2}\n".format(count, k, v["len"]))
            count+=1

def dbversion2018(pklFile):
    namelist = dict()
    with open("MetaPhlAn2_mpa_v20_m200_NewName.fna.namelist", "rt") as _nlf:
        while True:
            lines = _nlf.readlines(65535)
            if not lines:
                break
            for line in lines:
                lineEle = line.rstrip().split("\t")
                namelist[lineEle[1]] = lineEle[0]

    with open(pklFile, "rb") as _pklf:
        pklc = pickle.loads(bz2.decompress(_pklf.read()))

    nnameDict = dict()
    for k, v in pklc["markers"].items():
        nnameDict[namelist[k]] = v
    with open("MetaPhlAn2_mpa.markers.list.json", "w") as outfile:
        json.dump(nnameDict, outfile, cls=SetEncoder, indent=4)
    nnameDict = None

    nformTaxon = dict()
    for k, v in pklc["taxonomy"].items():
        nformTaxon[k] = {"genoLen": v}
    with open("MetaPhlAn2_mpa.taxonomy.list.json", "w") as outfile:
        json.dump(nformTaxon, outfile, cls=SetEncoder, indent=4)
    nformTaxon = None

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

def dbversion0000(pklFile):
    with open(pklFile, "rb") as _pklf:
        pklc = pickle.loads(bz2.decompress(_pklf.read()))

    with open("MetaPhlAn2_mpa.markers.list.json", "w") as outfile:
        json.dump(pklc["markers"], outfile, cls=SetEncoder, indent=4)

    nformTaxon = dict()
    for k, v in pklc["taxonomy"].items():
        nformTaxon[k] = {"genoLen": v}
    with open("MetaPhlAn2_mpa.taxonomy.list.json", "w") as outfile:
        json.dump(nformTaxon, outfile, cls=SetEncoder, indent=4)
    nformTaxon = None

def dbversion2019(pklFile):
    with open(pklFile, "rb") as _pklf:
        pklc = pickle.loads(bz2.decompress(_pklf.read()))
    
    taxonDict = dict()
    for k, v in pklc["taxonomy"].items():
        taxonDict[k] = {"taxid": v[0], "genoLen": v[1]}
    
    with open("MetaPhlAn2_mpa.markers.list.201901.json", "wt") as _ouf:
        json.dump(pklc["markers"], _ouf, cls=SetEncoder, indent=4)
    with open("MetaPhlAn2_mpa.taxonomy.list.201901.json", "wt") as _ouf:
        json.dump(taxonDict, _ouf, cls=SetEncoder, indent=4)

if __name__ == "__main__":
    if (len(sys.argv) != 3):
        print("usage: python3 " + sys.argv[0] + " <dbVersion (2018|2019|0000)>  <pkl_file_path>")
        sys.exit(0)
    if sys.argv[1] == "0000":
        dbversion0000(sys.argv[2])
        #markerSeqFnaRead()
    if sys.argv[1] == '2018':
        # "mpa_v20_m200.pkl"
        dbversion2018(sys.argv[2])
    if sys.argv[1] == '2019':
        # "mpa_v294_CHOCOPhlAn_201901.pkl"
        dbversion2019(sys.argv[2])

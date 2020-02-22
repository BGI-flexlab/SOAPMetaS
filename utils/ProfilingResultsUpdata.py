#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
    File:	ProfilingResultsUpdata.py
    Author:	Shixu He
    Email:	heshixu@genomics.cn
    Date:	2019-08-17
    ------
    The first running of CAMI and HMP profiling was done with an old version of Species_genome_gc.list file whilch was based on assembly_summary_refseq.txt file. I have updated the list by introducing assembly_summary_genbank.txt which contains more species.
    More species will be appended with GC content and genome length information, which may increase the profiling accuracy somewhat.
    ------
    Version:
'''

import sys

def genomeDictGenerate(genomeList):
    '''
    genome information dict:
    {
        IGC_species_name1: [genomeLength, genomeGCContent],
        IGC_species_name2: [genomeLength, genomeGCContent],
        ...
    }
    '''
    genomeInfoDict = dict()
    with open(genomeList, 'rt', encoding='utf-8') as _inf:
        while True:
            lines = _inf.readlines(65535)
            if not lines:
                break
            for line in lines:
                lineEle = line.split("\t")
                genomeInfoDict[lineEle[0]] = [lineEle[1], lineEle[2]]
    return genomeInfoDict

def profileListGenerate(abundanceFile, genomeInfoDict = dict()):
    profilingList = []
    totalAbun = 0.0
    with open(abundanceFile, 'rt', encoding='utf-8') as _abun:
        while True:
            lines = _abun.readlines(65535)
            if not lines:
                break
            for line in lines:
                linEle = line.split("\t")
                profilingList.append(linEle)
                if ((float(linEle[4]) == 0.0)) and (linEle[1] in genomeInfoDict):
                    profilingList[-1][4] = float(linEle[3])/float(genomeInfoDict[linEle[1]][0])
                    totalAbun += float(profilingList[-1][4])
    for i in range(len(profilingList)):
        profilingList[i][5] = float(profilingList[i][4])/totalAbun
    return profilingList

def main():
    if (len(sys.argv) != 4):
        print("usage: python3 " + sys.argv[0] + " <original_abundance_file> <Species_genome_gc.list> <output_file_name>")
        sys.exit(0)
    genomeInfoDict = genomeDictGenerate(sys.argv[2])
    profilingList = profileListGenerate(sys.argv[1], genomeInfoDict= genomeInfoDict)
    with open(sys.argv[3], 'wt', buffering=1, encoding='utf-8') as _outf:
        for ele in profilingList:
            _outf.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(ele[0], ele[1], ele[2], ele[3], ele[4], ele[5]))

if __name__ == "__main__":
    main()

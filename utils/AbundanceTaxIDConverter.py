#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
    File:	AbundanceTaxIDConverter.py
    Author:	Shixu He
    Email:	heshixu@genomics.cn
    Date:	2019-08-09
    ------
    (description)
    ------
    Version:
'''

import sys

def markerSpeciesTaxIDDictGenerate(speciesListFile):
    '''
    species list file format: (tab seperated, with header line)
    marker_species_name     GCF_ID  standard_species_name   taxid   species_taxidb
    '''
    markerSpeciesTaxIDDict = dict()
    with open(speciesListFile, 'rt', encoding='utf-8') as _slf:
        _slf.readline()
        while True:
            lines = _slf.readlines(65535)
            if not lines:
                break
            for line in lines:
                lineEle = line.rstrip().split("\t")
                markerSpeciesTaxIDDict[lineEle[0]] = lineEle[4]
    return markerSpeciesTaxIDDict

def profilingResultDictGenerate(profilingFile, proType = "soapmetas"):
    '''
    SOAPMetaS profiling result file format: (tab seperated, without header line)
    0   s__Species_name <raw_read_count>    <GC_recalibrated_reads_count>   <abundance>   <relative_abundance>
    cOMG profiling result format (species rank):
    ID\treads_pairs\treads_proportion\tgene_rel_abundance\tgene_original_abun
    '''
    speciesProfilingDict = dict()
    if (proType.lower() == "soapmetas"):
        with open(profilingFile, 'rt', encoding='utf-8') as _pf:
            while True:
                lines = _pf.readlines(65535)
                if not lines:
                    break
                for line in lines:
                    lineEle = line.strip().split("\t")
                    speciesProfilingDict[lineEle[0]] = float(lineEle[4])
    elif (proType.lower() == "comg"):
        with open(profilingFile, 'rt', encoding='utf-8') as _pf:
            _pf.readline()
            while True:
                lines = _pf.readlines(65535)
                if not lines:
                    break
                for line in lines:
                    lineEle = line.strip().split("\t")
                    speciesProfilingDict["s__" + lineEle[0]] = float(lineEle[4])
    return speciesProfilingDict
    

def profilingConverter(profilingDict = dict(), speTaxIDDict = dict()):
    resultDict = dict()
    for spe, abun in profilingDict.items():
        taxID = speTaxIDDict.get(spe, "-1")
        resultDict[taxID] = resultDict.get(taxID, 0.0) + float(abun)
    return resultDict

def main():
    profilingType = None
    if (len(sys.argv) == 4 ):
        profilingType = "soapmetas"
    elif (len(sys.argv) == 5):
        profilingType = sys.argv[4]
    else:
        print("usage: python3 " + sys.argv[0] + " <abundance file> <Selected_species_taxID_list> <sampleName> ")
        sys.exit(0)
    markerSpeTaxDict = markerSpeciesTaxIDDictGenerate(sys.argv[2])
    profilingDict = profilingResultDictGenerate(sys.argv[1], proType = profilingType)
    resultDict = profilingConverter(profilingDict = profilingDict, speTaxIDDict = markerSpeTaxDict)
    
    if (profilingType.lower() == "soapmetas"):
        with open("SOAPMetas_profiling_motuFormat_" + sys.argv[3] + ".abundance", 'wt', buffering=1, encoding='utf-8') as _outf:
            _outf.write("taxaid (species taxID)\tabundance\n")
            for taxID, abun in resultDict.items():
                _outf.write("{0}\t{1}\n".format(taxID, abun))
    elif (profilingType.lower() == "comg"):
        with open("cOMG_profiling_motuFormat_" + sys.argv[3] + ".abundance", 'wt', buffering=1, encoding='utf-8') as _outf:
            _outf.write("taxaid (species taxID)\tabundance\n")
            for taxID, abun in resultDict.items():
                _outf.write("{0}\t{1}\n".format(taxID, abun))

if __name__ == "__main__":
    main()
    
                

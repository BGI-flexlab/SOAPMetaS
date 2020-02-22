#!/usr/bin/env python3
'''
    File:	StdSpeciesListLink.py
    Author:	Shixu He
    Email:	heshixu@genomics.cn
    Date:	2019-04-28
    ------
    Generate SelectedMarkerSpecies.list and Species_genome_gc.list for IGC_9.9M markers. The script needs NCBI assembly summary file (genbank and refseq) and manually modified IGC9.9M species list (containing all species included in IGC9.9M marker matrix file.)
    ------
    Version: 
    ver03:
    Add support for both genebank and refseq summary files.

    ver02:
    Add support for variant species name. For example: "[Pylum] spename" and "Phlum spename".
    Add suppport for datapath check.

    ver01: 
    Function creation.
'''

import sys, os
import re
import gzip

def stdDictGen(stdListFile_refseq, stdListFile_genbank, dataPath = "ftp://ftp.ncbi.nlm.nih.gov"):
    '''
    operation:
    filt: none representative/reference
    strip: \\s(.+)$

    return:
    dict{
        "na":{
            "phylum1 spe1": {
                "repre": [GCF_No., GCF_No2., ...],
                "phylum1 spe1 strain1_part1": {
                    "repre": [GCF_No., GCF_No2., ...],
                    "phylum1 spe1 strain1_part1 strain1_part2": {
                        "repre": [GCF_No., GCF_No2., ...],
                    }
                },
                "phylum1 spe1 strain2_part1": {
                    "repre": [GCF_No., GCF_No2., ...],
                }
            }
            "phylum1 spe2": {
                ...
            }
            "phylum2 spe3": {
                ...
            }
            ...
        }
        "re":{
            ...
        }
    }
    '''
    outDict = {"re": dict(), "na": dict()}
    outIDDict = dict()
    _stdListFileReader(stdListFile_refseq, spetreeDict=outDict, taxidDict=outIDDict, dataPath=dataPath)
    _stdListFileReader(stdListFile_genbank, spetreeDict=outDict, taxidDict=outIDDict, dataPath=dataPath)
    print("Standard dict generated.")
    return (outDict, outIDDict)

def _stdListFileReader(listFile, spetreeDict = dict(), taxidDict = dict(), dataPath = "ftp://ftp.ncbi.nlm.nih.gov"):
    with open(listFile, "rt") as _ifl:
        while True:
            filePos = _ifl.tell()
            if (_ifl.readline().startswith("#")):
                continue
            else:
                _ifl.seek(filePos)
                break
        while True:
            lines = _ifl.readlines(65535)
            if not lines:
                break
            for line in lines:
                lineCol = line.rstrip().split("\t")
                _stdSpeciesTreeGenerate(lineCol, spetreeDict, taxidDict, dataPath)
                organismName = lineCol[7]
                if (organismName.startswith("[")):
                    lineCol[7] = re.sub(r"\[|\]", "", lineCol[7])
                    _stdSpeciesTreeGenerate(lineCol, spetreeDict, taxidDict, dataPath)

def _stdSpeciesTreeGenerate(lineCol = [], speTreeDict = dict(), taxIDDict = dict(), dataPath = "ftp://ftp.ncbi.nlm.nih.gov"):
    genoPath = re.sub(r"^ftp://ftp.ncbi.nlm.nih.gov", dataPath, lineCol[19])
    if (not os.path.exists(genoPath)):
        # for convenience. If the genome sequence file doesn't exists, we won't be able to calculate the GC content. 
        print(lineCol[0] + " genome directory doesn't exists: " + genoPath)
        return
    stdDict = None
    tmpDict = None
    if (lineCol[4][:2] == "re"):
        stdDict = speTreeDict["re"]
    else:
        stdDict = speTreeDict["na"]
    gcf = lineCol[0]
    lineCol[7] = re.sub(r"\s+\(.+?\)$", "", lineCol[7])
    taxIDDict[gcf] = {"taxid": lineCol[5], "species_taxid": lineCol[6]}
    speEle = re.split(r"\s+", lineCol[7])
    eleCount = len(speEle)
    if eleCount < 3:
        name = " ".join(speEle)
        if (stdDict.setdefault(name, dict()).get("std", False)):
            # If the species name of current item is "standard" binomial nomenclature without "strain" column, we use its "GCF" number as the "standard" and set the "std" tag to True. As this "standard" item without "strain" col might be located after the "standard" bino-nome item with "strain" col, we may check the "std" tag and "strain" col (col[3]).
            if ((not lineCol[8]) and (os.path.exists(genoPath))):
                stdDict.get(name, dict()).setdefault("repre", []).append(gcf)
            return
        stdDict.get(name, dict()).setdefault("repre", []).append(gcf)
        stdDict.get(name)["std"] = True
        return
    tmpDict = stdDict.setdefault(" ".join(speEle[0:2]), dict())
    tmpDict.setdefault("repre", []).append(gcf)
    i = 2
    while i < eleCount:
        name = " ".join(speEle[0:i+1])
        tmpDict = tmpDict.setdefault(name, dict())
        tmpDict.setdefault("repre", []).append(gcf)
        i += 1
    return

def speciesSelection(speciesList, standardDict = dict()):
    '''
    standardDict: returned value of stdDictGen(str) function

    return:
    (ref_species: name in IGC_9.9M_species.ref)
    dict{
        ref_species: {
            "std": standard name
            "gcf: gcf Number
        }
        ref_species2: ...
        ...
    }
    '''
    speciesDict = dict()
    
    with open(speciesList, 'r', encoding='utf-8') as _inf:
        while True:
            lines = _inf.readlines(65535)
            if not lines:
                break
            for line in lines:
                nameList = line.rstrip().split(",")
                for i in range(len(nameList)):
                    if _speciesDictGenerate(re.sub(r"^s__", "", nameList[i]).split("__"), nameList[0], standardDict, speciesDict):
                        break
                print("Species: " + nameList[0] + " selection finished.")
                
    print("Species Dict generated.")
    return speciesDict

def _speciesDictGenerate(nameElement, originalName, standardDict=dict(), speciesDict = dict()):
    tempDict = dict()
    tempNaDict = dict()
    stdDict = standardDict["re"]
    naDict = standardDict["na"]
    eleCount = len(nameElement)
    if eleCount < 2:
        if nameElement[0] in speciesDict:
            speciesDict.setdefault(originalName, {"std": nameElement[0], "gcf": speciesDict[nameElement[0]]["gcf"]})
            return True
        if nameElement[0] in stdDict:
            speciesDict.setdefault(originalName, {"std": nameElement[0], "gcf": stdDict[nameElement[0]]["repre"][0]})
        elif nameElement[0] in naDict:
            speciesDict.setdefault(originalName, {"std": nameElement[0], "gcf": naDict[nameElement[0]]["repre"][0]})
        else:
            return False
        return True
    name = " ".join(nameElement[0:2])
    if name in speciesDict:
        pass
    elif name in stdDict:
        speciesDict.setdefault(originalName, {"std": name, "gcf": stdDict[name]["repre"][0]})
    elif name in naDict:
        speciesDict.setdefault(originalName, {"std": name, "gcf": naDict[name]["repre"][0]})
    else:
        return False
    tempDict = stdDict.get(name, dict())
    tempNaDict = naDict.get(name, dict())
    i = 2
    while i < eleCount:
        name = " ".join(nameElement[0:i+1])
        if name in tempDict:
            speciesDict[originalName]["std"] = name
            speciesDict[originalName]["gcf"] = tempDict[name]["repre"][0]
        elif name in tempNaDict:
            speciesDict[originalName]["std"] = name
            speciesDict[originalName]["gcf"] = tempNaDict[name]["repre"][0]
        else:
            break
        tempDict = tempDict.get(name, dict())
        tempNaDict = tempNaDict.get(name, dict())
        i += 1
    return True

def stdInfoExtract(stdListFile_refseq, stdListFile_genbank, speciesDict = dict(), dbpath = "ftp://ftp.ncbi.nlm.nih.gov"):
    '''
    speciesDict: returned value from markerSelection(file, dict()) function

    return: 
    (ref_spe: species name in IGC_9.9M_species.ref)
    dict: {
        ref_spe: {
            "std": GCF ID, standard species name (shortest name which could be used to extract unique GCF ID from assembly_summary file), path to genome database
            "glen": genome length (plasmid not included, all scaffold/contig included)
            "gc": genome gc content
        }
    }
    '''

    pathDict = dict()
    _pathDictGen(stdListFile_refseq, pathDict=pathDict, dataPath=dbpath)
    _pathDictGen(stdListFile_genbank, pathDict=pathDict, dataPath=dbpath)
    outDict = dict()
    for igcSpeName, dic in speciesDict.items():
        if dic is None:
            continue
        #"tax": "s__" + re.sub(r"\s+", "_", igcSpeName)
        info = _calGenoInfo(pathDict[dic["gcf"]])
        outDict[igcSpeName] = {
            "std": dic["gcf"] + "," + dic["std"] + "," + pathDict[dic["gcf"]],
            "glen": info[0],
            "gc": info[1],
        }
        #print("s__" + re.sub(r"\s+", "_", igcSpeName) + "\t" + str(info[0]) + "\t" + str(info[1]))
    return outDict

def _pathDictGen(listFile, pathDict = dict(), dataPath = "ftp://ftp.ncbi.nlm.nih.gov"):
    with open(listFile, 'r', encoding='utf-8') as _inf:
        while True:
            filePos = _inf.tell()
            if (_inf.readline().startswith("#")):
                continue
            else:
                _inf.seek(filePos)
                break
        while True:
            lines = _inf.readlines(65535)
            if not lines:
                break
            for line in lines:
                ele = line.rstrip().split("\t")
                pathDict[ele[0]] = re.sub(r"^ftp://ftp.ncbi.nlm.nih.gov", dataPath , ele[19])

def _calGenoInfo(genoDir):
    '''
    genoDir: 
    '''
    ret = []
    prefix = genoDir.rstrip("/").split("/")[-1]
    fna = genoDir + "/" + prefix + "_genomic.fna.gz"
    #rep = genoDir + "/" + prefix + "_assembly_report.txt"
    seq = None
    glen = 0
    gclen = 0
    jump = False
    with gzip.open(fna, "rt") as _fna:
        fastaLines = _fna.readlines()
        for line in fastaLines:
            if line[0] == '>':
                if (re.search(r"[Pp]lasmid", line)):
                    jump = True
                else:
                    jump = False
                continue
            if jump: continue
            seq = line.rstrip()
            glen += len(seq)
            gclen += seq.count("G") + seq.count("C")
    ret.append(glen)
    ret.append(gclen/glen)
    return ret

if __name__ == "__main__":
    if (len(sys.argv) != 5):
        print("usage: python3 " + sys.argv[0] + " <assembly_summary_refseq.txt> <assembly_summary_genbank.txt> <manual_IGC9.9M_species_List> <NCBIPubDBPath(ftp://ftp.ncbi.nlm.nih.gov)>")
        print("output1: SelectedMarkerSpecies.list (IGC species name, GCF accession ID, NCBI organism name, NCBI taxid, NCBI species_taxid)")
        print("output2: Species_genome_gc.list (s__species_name, genome length, genome GC percentage, GCF accessID+NCBI orga name+genome seq file path)")
        sys.exit(0)
    
    std, stdID = stdDictGen(sys.argv[1], sys.argv[2], dataPath = sys.argv[4])
    species = speciesSelection(sys.argv[3], std)

    # Test1
    #for k, v in marker.items():
    #    if v == None:
    #        continue
    #    print(k + ":\t" + str(v))

    with open("SelectedMarkerSpecies.list", "wt", encoding="utf-8") as _of:
        _of.write("marker_species_name\tGCF_ID\tstandard_species_name\ttaxid\tspecies_taxid\n")
        for k, v in species.items():
            if v == None:
                continue
            gcfNo = v["gcf"]
            _of.write("{0}\t{1}\t{2}\t{3}\t{4}\n".format(k, gcfNo, v["std"], stdID[gcfNo]["taxid"], stdID[gcfNo]["species_taxid"]))
    
    
    out = stdInfoExtract(sys.argv[1], sys.argv[2], speciesDict = species, dbpath = sys.argv[4])
    with open("Species_genome_gc.list", "wt", buffering=1, encoding="utf-8") as _of:
        for orig, values in out.items():
            _of.write("{0}\t{1}\t{2}\t{3}\n".format(orig, values["glen"], values["gc"], values["std"]))

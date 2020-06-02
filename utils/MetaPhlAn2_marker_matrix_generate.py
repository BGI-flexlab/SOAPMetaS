#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
    File:	MetaPhlAn2_marker_matrix_generate.py
    Author:	Shixu He
    Email:	heshixu@genomics.cn
    Date:	2019-08-28
    ------
    Generate matrix file like IGC_9.9M_marker.matrix for MetaPhlAn2 DB.
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

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

def mpaPickleFileRead(mpaPickleFile, markerListJsonFile, taxonListJsonFile, old_pickle = False, acc2SpeDict = dict(), geneInfoDict = dict()):
    '''
    Combination of mpaMarkerListRead() and taxonListFileRead() function, but generated from mpa_v20_m200.pkl file.
    Generate marker (new name) list file in json format.
    '''
    with open(mpaPickleFile, "rb") as _pklf:
        pklc = pickle.loads(bz2.decompress(_pklf.read()))
    
    # write new name marker
    npkl = {"markers": dict(), "taxonomy": dict()}
    nnameMarker = dict()
    for k, v in pklc["markers"].items():
        nname = geneInfoDict[k]["nname"]
        npkl["markers"][nname] = v
        nnameMarker[nname] = v
        nnameMarker[nname]["oname"] = k
    with open(markerListJsonFile, "w") as outfile:
        json.dump(nnameMarker, outfile, cls=SetEncoder, indent=4)
    nnameMarker = None
    
    nformTaxon = dict()
    npkl["taxonomy"] = pklc["taxonomy"]
    if old_pickle:
        for k, v in pklc["taxonomy"].items():
            nformTaxon[k] = {"genoLen": v}
    else:
        for k, v in pklc["taxonomy"].items():
            nformTaxon[k] = {"genoLen": v[1], "taxid": v[0]}
    with open(taxonListJsonFile, "w") as outfile:
        json.dump(nformTaxon, outfile, cls=SetEncoder, indent=4)
    nformTaxon = None
    
    ofile = bz2.BZ2File('MetaPhlAn2_mpa_v20_m200.pkl', 'w')
    pickle.dump(npkl, ofile, pickle.HIGHEST_PROTOCOL)
    ofile.close()

    markerCladeDict = dict()
    for k, v in pklc["markers"].items():
        clade = v["clade"]
        if not clade.startswith("s__"):
            clade = acc2SpeDict.get(re.sub(r"^t__", "", clade), "s__unclassed")
        markerCladeDict[k] = clade
    
    gcfToGenoLenDict = dict()
    if old_pickle:
        for k, v in pklc["taxonomy"].items():
            gcfToGenoLenDict[re.sub(r"^t__", "", re.split(r"\|", k)[-1])] = int(v)
    else:
        for k, v in pklc["taxonomy"].items():
            gcfToGenoLenDict[re.sub(r"^t__", "", re.split(r"\|", k)[-1])] = int(v[1])

    return gcfToGenoLenDict, markerCladeDict

def mpaMarkerListRead(mpaMakersListFile, acc2SpeDict = dict()):
    '''
    File format:
    gi|483970126|ref|NZ_KB891629.1|:c6456-5752	{'ext': {'GCF_000373585', 'GCF_000355695', 'GCF_000226995'}, 'score': 3.0, 'clade': 's__Streptomyces_sp_KhCrAH_244', 'len': 705, 'taxon': 'k__Bacteria|p__Actinobacteria|c__Actinobacteria|o__Actinomycetales|f__Streptomycetaceae|g__Streptomyces|s__Streptomyces_sp_KhCrAH_244'}

    acc2SpeDict:
    {
        GCF_xxxxxx: s__Species_Name,
        GCA_xxxxxx: s__Species_Name2,
        PRJNxxxxxx: s__SpeciesName3,
        ...
    }

    Returned dict:
    {
        geneID: s__Species_Name,
        geneID2: s__Species_Name2,
        ...
    }
    '''
    markerCladeDict = dict()
    with open(mpaMakersListFile, 'rt', encoding='utf-8') as _mpaf:
        while True:
            lines = _mpaf.readlines(65535)
            if not lines:
                break
            for line in lines:
                spl = re.split(r"\s+", line.rstrip(), maxsplit=1)
                clade = yaml.load(spl[1], Loader = yaml.BaseLoader)["clade"]
                if not clade.startswith("s__"):
                    clade = acc2SpeDict.get(re.sub(r"^t__", "", clade), "s__unclassed")
                markerCladeDict[spl[0]] = clade
    return markerCladeDict

def markerSeqFnaRead(markerGeneFnaFile, newIDOutputFNA, IDcorresFile, norename, nobzip):
    '''
    markerGeneFnaFile: (FASTA sequence format)

    Returned dict:
    {
        geneID: {
            "len": geneLength,
            "gc": geneGCPercentage,
            "nname": customized gene name
        },
        geneID2: {
            ...
        },
        ...
    }
    '''
    geneInfoDict = dict()

    count = 1
    
    if nobzip:
        with open(markerGeneFnaFile, "rt") as handle, bz2.open(newIDOutputFNA, "wt") as newFNA:
            for record in SeqIO.parse(handle, "fasta"):
                nid = "MEPH_Marker_g" + str(count)
                geneInfoDict[record.id] = {"len": len(record), "gc": GC(record.seq), "nname": nid}
                record.id = nid
                SeqIO.write(record, newFNA, "fasta")
                count+=1
    elif norename:
        with bz2.open(markerGeneFnaFile, "rt") as bzHandle, bz2.open(newIDOutputFNA, "wt") as newFNA:
            for record in SeqIO.parse(bzHandle, "fasta"):
                nid = "MEPH_Marker_g" + str(count)
                geneInfoDict[record.id] = {"len": len(record), "gc": GC(record.seq), "nname": nid}
                record.id = nid
                SeqIO.write(record, newFNA, "fasta")
                count+=1
    else:
        with bz2.open(markerGeneFnaFile, "rt") as bzHandle, bz2.open(newIDOutputFNA, "wt") as newFNA:
            for record in SeqIO.parse(bzHandle, "fasta"):
                nid = "MEPH_Marker_g" + str(count)
                geneInfoDict[record.id] = {"len": len(record), "gc": GC(record.seq), "nname": nid}
                record.id = nid
                SeqIO.write(record, newFNA, "fasta")
                count+=1
    with open(IDcorresFile, "wt") as idf:
        for k, v in geneInfoDict.items():
            idf.write("{nid}\t{oid}\n".format(nid=v["nname"], oid=k))

    return geneInfoDict

def species2GenomeRead(spe2genomeFile):
    '''
    spe2genomeFile formrat:
    s__Species_name <count> GCF_xxx1    GCA_xxx2    PRJNxxx
    s__Species_name2 <count> GCF_xxx3    GCF_xxx4    PRJNxxx2
    ...

    returned:
    acc2SpeDict = {
        GCF_xxx1: s__Species_name,
        GCF_xxx2: s__Species_name2,
        PRJNxxx: s__Species_name3,
        ...
    }
    spe2accDict = {
        s__Species_name: [GCF_xxx1, GCA_xxx2, PRJNxxx]
        ...
    }
    '''
    acc2SpeDict = dict()
    spe2accDict = dict()
    with open(spe2genomeFile, 'rt', encoding='utf-8') as _spegno:
        #while True:
            #lastPos = _spegno.tell()
            #_spegno.seek(lastPos)
        while True:
            lines = _spegno.readlines(65535)
            if not lines:
                break
            for line in lines:
                spl = re.split(r"\t", line.rstrip())
                spe2accDict[spl[0]] = spl[2:]
                for gcf in spl[2:]:
                    acc2SpeDict[gcf] = spl[0]
    return acc2SpeDict, spe2accDict

def mappingResultRead(metaphlanMappingResults):
    '''
    mappingresults file format:

    GCA_000001985	441960	no rank=1:root|no rank=131567:cellular organisms|superkingdom=2759:Eukaryota|no rank=33154:Opisthokonta|kingdom=4751:Fungi|subkingdom=451864:Dikarya|phylum=4890:Ascomycota|no rank=716545:saccharomyceta|subphylum=147538:Pezizomycotina|no rank=716546:leotiomyceta|class=147545:Eurotiomycetes|subclass=451871:Eurotiomycetidae|order=5042:Eurotiales|family=28568:Trichocomaceae|genus=5094:Talaromyces|species=37727:Talaromyces marneffei|no rank=441960:Talaromyces marneffei ATCC 18224

    s__zeta_proteobacterium_SCGC_AB_604_O16	1131290	no rank=1:root|no rank=131567:cellular organisms|superkingdom=2:Bacteria|phylum=1224:Proteobacteria|class=580370:Zetaproteobacteria|no rank=1131280:unclassified Zetaproteobacteria|species=1131290:zeta proteobacterium SCGC AB-604-O16

    Returned dict:
    gcfTaxIDDict = {
        GCA_xxxxxx: taxID1,
        GCF_xxxx: taxID2,
        PRJNxxxxx: taxID3,
        ...
    }
    speTaxIDDict = {
        s__SPecies_name: taxID1,
        s__Species_name2: taxID2,
        ...
    }
    '''
    gcfTaxIDDict = dict()
    speTaxIDDict = dict()
    with open(metaphlanMappingResults, 'rt', encoding='utf-8') as _mapf:
        while True:
            lastPos = _mapf.tell()
            if _mapf.readline().startswith("#"):
                continue
            _mapf.seek(lastPos)
            break
        while True:
            lines = _mapf.readlines(65535)
            if not lines:
                break
            for line in lines:
                spl = re.split(r"\s+", line, maxsplit=3)
                if spl[0].startswith("s__"):
                    speTaxIDDict[spl[0]] = spl[1]
                else:
                    gcfTaxIDDict[spl[0]] = spl[1]
    return gcfTaxIDDict, speTaxIDDict

def taxonListFileRead(mpaTaxonListFile):
    '''
    mpaTaxonList format:
    k__Bacteria|p__Proteobacteria|c__Gammaproteobacteria|o__Enterobacteriales|f__Enterobacteriaceae|g__Shigella|s__Shigella_flexneri|t__GCF_000213455	4656186

    Returned dict:
    {
        GCF_000213455: 4656186,
        ...
    }
    '''
    gcfToGenoLenDict = dict()
    with open(mpaTaxonListFile, 'rt', encoding='utf-8') as _taxLF:
        #while True:
            #lastPos = _taxLF.tell()
            #_taxLF.seek(lastPos)
        while True:
            lines = _taxLF.readlines(65535)
            if not lines:
                break
            for line in lines:
                try:
                    spl = re.split(r"\s+", line.rstrip())
                    gcfToGenoLenDict[re.sub(r"^t__", "", re.split(r"\|", spl[0])[-1])] = int(spl[1])
                except Exception as e:
                    print("Error: " + str(e) + " . Current record: " + line)
    return gcfToGenoLenDict


def gcfPathDictUpdate(listFile, pathDict = dict(), dataPath = "ftp://ftp.ncbi.nlm.nih.gov"):
    '''
    pathDict:
    {
        GCF_xxxxx: /path/to/dir,
        GCF_xxxx2: /path/to/dir2,
        ...
    }
    '''
    if listFile == None:
        return
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
                gcf = re.sub(r"\.\d+$", "", ele[0])
                gcfPaired = re.sub(r"\.\d+$", "", ele[17])
                path = re.sub(r"^ftp://ftp.ncbi.nlm.nih.gov", dataPath , ele[19])
                pathDict[gcf] = path
                if gcfPaired not in pathDict:
                    pathDict[gcfPaired] = path

def _calGenoInfo(genoDir):
    '''
    genoDir: 
    '''
    prefix = genoDir.rstrip("/").split("/")[-1]
    fna = genoDir + "/" + prefix + "_genomic.fna.gz"
    #rep = genoDir + "/" + prefix + "_assembly_report.txt"
    glen = 0
    gclen = 0
    gc = 0.0
    with gzip.open(fna, "rt") as _fna:
        for record in SeqIO.parse(_fna, "fasta"):
            if (re.search(r"[Pp]lasmid", record.id)):
                continue
            sequence = record.seq
            glen += len(sequence)
            gclen += sum(sequence.count(x) for x in ['G', 'C', 'g', 'c', 'S', 's'])
    try:
        gc = gclen/float(glen)
    except ZeroDivisionError:
        gc = 0.0
    return glen, gc

def outputGenMatrix(matrixFile, markerCladeDict=dict(), geneInfoDict = dict()):
    count = 1
    with open(matrixFile, 'wt',  encoding='utf-8') as _mf:
        for oid, ginfo in geneInfoDict.items():
            _mf.write("{gid}\t{nid}\t{glen}\t{clade}\t{ggc}\n".format(
                gid = count,
                nid = ginfo["nname"],
                glen = ginfo["len"],
                clade = markerCladeDict[oid],
                ggc = ginfo["gc"]
            ))
            count+=1

def outputSelectedMarker(selMarkerFile, spe2accDict = dict(), gcfTaxIDDict = dict(), speTaxIDDict = dict()):
    with open(selMarkerFile, 'wt', encoding='utf-8') as _smf:
        _smf.write("marker_species_name\tGCF_ID\tstandard_species_name\ttaxid\tspecies_taxid\n")
        for spe, accl in spe2accDict.items():
            _smf.write("{sname}\t{gcf}\t{stdname}\t{taxid}\t{spetaxid}\n".format(
                sname = spe,
                gcf = accl[0],
                stdname = spe,
                taxid = gcfTaxIDDict.get(accl[0], str(-1)),
                spetaxid = speTaxIDDict.get(spe, str(-1))
            ))

def outputSpeGenomeGC(spegenoGCFile, gcfToGenoLenDict = dict(), spe2accDict = dict(), gcfPathDict = dict()):
    with open(spegenoGCFile, 'wt', encoding='utf-8') as _sgf:
        for spe, accl in spe2accDict.items():
            gcf = accl[0]
            genogc = 0.0
            glen = 0
            if gcf in gcfPathDict:
                if os.path.exists(gcfPathDict[gcf]):
                    glen, genogc = _calGenoInfo(gcfPathDict[gcf])
                    lendev = abs(glen-gcfToGenoLenDict[gcf])
                    if lendev > 0:
                        print("Genome {0} length deviation: {1} , {5:.2%}. Fna sequence length: {2}. MetaPhlAn taxonomy length: {3}. Genome file path: {4}".format(gcf, lendev, glen, gcfToGenoLenDict[gcf], gcfPathDict[gcf], lendev/gcfToGenoLenDict[gcf]), file=sys.stderr)
                else:
                    print("Genome {0} doesn't have fna sequence file.".format(gcf), file=sys.stderr)
            else:
                print("Genome {0} not in assembly summary file.".format(gcf), file=sys.stderr)
            _sgf.write("{sname}\t{genolen}\t{genogc}\t{gcf}\n".format(
                sname = spe,
                genolen = gcfToGenoLenDict[gcf],
                genogc = genogc,
                gcf = gcf
            ))

def checkArgv():
    '''Parse the command line parameters.'''
    _parser = argparse.ArgumentParser(description="generate customized IGC_9.9M-like DB of MetaPhlAn2 data.")
    
    _parser.add_argument(
        "--mpa-marker-list",
        required = False,
        default = None,
        help="mpa_v20_m200_markers.list. Extracted from mpa_v20_m200.pkl."
    )

    _parser.add_argument(
        "--mpa-fna-bz2",
        required = False,
        default = None,
        help="mpa_v20_m200.fna.bz2. Gene sequence file in bzip2 compressed file. The file is in mpv_v20_m200.tar"
    )

    _parser.add_argument(
        "--species2genome",
        required = True,
        help = "customized species2genome file contains only defined species."
    )

    _parser.add_argument(
        "--mapping-results",
        required = False,
        default = None,
        help = "species - taxID mapping file for MetaPhlAn2 DB. Related to CAMI evaluation process."
    )

    _parser.add_argument(
        "--mpa-taxon-list",
        required = False,
        help = "mpa_v20_m200_taxonomy.list extracted from mpa_v20_m200.pkl."
    )

    _parser.add_argument(
        "--newid-gene-fna",
        required = False,
        default = "MetaPhlAn2_mpa_v20_m200_NewName.fna.bz2",
        help="Output file name of fna.bz2 sequence file with new gene ID."
    )

    _parser.add_argument(
        "--id-corr-file",
        required = False,
        default = "MetaPhlAn2_mpa_v20_m200_NewName.fna.namelist",
        help="Output file name of correspondent gene ID list."
    )

    _parser.add_argument(
        "--output-matrix",
        required = False,
        default = "MetaPhlAn2_marker.matrix",
        help = "output file name of IGC_9.9M like marker matrix."
    )

    _parser.add_argument(
        "--output-sel-mar",
        required = False,
        default = "MetaPhlAn2_SelectedMarkerSpecies.list",
        help = "output file name of SelectedMarkerSpecies.list of MetaPhlAn2 DB."
    )

    _parser.add_argument(
        "--output-spe-geno",
        required = False,
        default = "MetaPhlAn2_Species_genome_gc.list",
        help = "output file name of Species_genome_gc.list of MetaPhlAn2 DB."
    )

    _parser.add_argument(
        "--ncbi-db-path",
        required = False,
        default = "ftp://ftp.ncbi.nlm.nih.gov",
        help = "Local path of NCBI database. [may be /hwfssz1/pub/database/ftp.ncbi.nih.gov]"
    )

    _parser.add_argument(
        "--assembly-refseq",
        required = False,
        default = None,
        help = "NCBI taxonomy assembly_summary_refseq.txt."
    )

    _parser.add_argument(
        "--assembly-genbank",
        required = False,
        default = None,
        help = "NCBI taxonomy assembly_summary_genbank.txt."
    )

    _parser.add_argument(
        "--mpa-pickle",
        required = False,
        default = None,
        help = "mpa_v20_m200.pkl"
    )

    _parser.add_argument(
        "--marker-json-output",
        required = False,
        default = "MetaPhlAn2_mpa.markers.list.json",
        help = "Output file name of mpa markers list (json format)."
    )

    _parser.add_argument(
        "--taxon-json-output",
        required = False,
        default = "MetaPhlAn2_mpa.taxonomy.list.json",
        help = "Output file name of mpa taxonomy list (json format)."
    )

    _parser.add_argument(
        "--no-rename",
        required = False,
        action = 'store_true',
        help="Whether or not rename reads. if set, read name will be renamed and namelist file will be generated"
    )

    _parser.add_argument(
        "--no-bzip",
        required = False,
        action = 'store_true'
    )

    _parser.add_argument(
        "--old-pickle",
        required = False,
        action = 'store_true',
        help="whether or not the pickle file (eg. mpa_v20_m200.pkl) is old version. In old version, the \"taxonomy\" subdictionary contains both taxID and genome length information. So the data structure is changed."
    )
    
    return _parser.parse_args()

def main():
    args = checkArgv()

    if args.mpa_pickle != None:
        if args.mpa_fna_bz2 == None:
            print(" mpa fna file is necessary for new name generation.")
            sys.exit(1)
        pass
    else:
        if args.mpa_taxon_list == None:
            print(" one of --mpa-pickle and --mpa-taxon-list must be provided.")
            sys.exit(1)

    if args.mpa_fna_bz2 != None:
        geneInfoDict = markerSeqFnaRead(args.mpa_fna_bz2, args.newid_gene_fna, args.id_corr_file, args.no_rename, args.no_bzip)

    acc2SpeDict, spe2accDict = species2GenomeRead(args.species2genome)

    if (args.mpa_fna_bz2 != None) and (args.mpa_pickle != None):
        gcfToGenoLenDict, markerCladeDict = mpaPickleFileRead(args.mpa_pickle, args.marker_json_output, args.taxon_json_output, args.old_pickle, acc2SpeDict = acc2SpeDict, geneInfoDict = geneInfoDict)
    else:
        gcfToGenoLenDict = taxonListFileRead(args.mpa_taxon_list)
        if args.mpa_marker_list != None:
            markerCladeDict = mpaMarkerListRead(args.mpa_marker_list, acc2SpeDict=acc2SpeDict)
    
    if args.mapping_results != None:
        gcfTaxIDDict, speTaxIDDict = mappingResultRead(args.mapping_results)

    if (args.mpa_fna_bz2 != None) and (args.mpa_marker_list != None or args.mpa_pickle != None):
        outputGenMatrix(args.output_matrix, markerCladeDict=markerCladeDict, geneInfoDict=geneInfoDict)

    #if args.mapping_results != None:
    #    outputSelectedMarker(args.output_sel_mar, spe2accDict=spe2accDict, gcfTaxIDDict=gcfTaxIDDict, speTaxIDDict=speTaxIDDict)
    #gcfPathDict = dict()
    #gcfPathDictUpdate(args.assembly_refseq, pathDict=gcfPathDict, dataPath=args.ncbi_db_path)
    #gcfPathDictUpdate(args.assembly_genbank, pathDict=gcfPathDict, dataPath=args.ncbi_db_path)
    #outputSpeGenomeGC(args.output_spe_geno, gcfToGenoLenDict=gcfToGenoLenDict, spe2accDict=spe2accDict, gcfPathDict = gcfPathDict)

if __name__ == "__main__":
    main()

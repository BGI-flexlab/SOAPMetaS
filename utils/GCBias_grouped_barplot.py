# -*- coding: utf-8 -*-
'''
    File:	GCBias_grouped_barplot.py
    Author:	Shixu He
    Email:	heshixu@genomics.cn
    Date:	2019-09-10
    ------
    (description)
    ------
    Version:
'''

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
matplotlib.use('Agg')
import sys


def plotTest():
    labels = ['G1\ndddd', 'G2', 'G3', 'G4', 'G5']
    men_means = [20, 34, 30, 35, 27]
    women_means = [25, 32, 34, 20, 25]

    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, men_means, width, label='Men')
    rects2 = ax.bar(x + width/2, women_means, width, label='Women')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Scores')
    ax.set_title('Scores by group and gender')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    autolabel(rects2, ax)
    autolabel(rects1, ax)
    fig.tight_layout()
    plt.show()

def autolabel(rects, ax):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

def getStdSpeciesInfo(sampleSpeContentFile):
    '''
    SampleSpeciesContent csv file, file format:
    #speciesTaxID,species,GC,DNA,percentage

    Output species info dict:
    {
        taxID: {
            "name": "Species name",
            "GC": <GC content percentage>
            "percent": <DNA percentage>
        }
    }
    '''
    speInfoDict = dict()
    with open(sampleSpeContentFile, 'rt', encoding='utf-8') as _spein:
        #while True:
            #lastPos = _spein.tell()
            #_spein.seek(lastPos)
        _spein.readline()
        while True:
            lines = _spein.readlines(65535)
            if not lines:
                break
            for line in lines:
                lineEle = line.rstrip().split(",")
                speInfoDict[lineEle[0]] = {"name": lineEle[1], "GC": float(lineEle[2]), "percent": float(lineEle[4])}
    return speInfoDict

def readCAMIFormData(camiFormAbunFile, stdSpeInfo = dict()):
    '''
    CAMI format abundance file format:

    # Taxonomic Profiling Output
    @SampleID:SRS014459
    @Version:0.9.3
    @Ranks:superkingdom|phylum|class|order|family|genus|species|strain
    @TaxonomyID:ncbi-taxonomy_2019.08.06
    @__program__:motu
    @@TAXID RANK    TAXPATH TAXPATHSN       PERCENTAGE
    2       superkingdom    2       Bacteria        100.000000
    201174  phylum  2|201174        Bacteria|Actinobacteria 0.149670
    976     phylum  2|976   Bacteria|Bacteroidetes  61.987281
    ...

    Output value dict (Species rank, merely contains species in stdSpeDict):
    {
        taxID1: <PERCENTAGE>,
        taxID2: <PERCENTAGE>,
        ...
    }
    '''
    abunDict = dict()
    with open(camiFormAbunFile, 'rt', encoding='utf-8') as _abun:
        while True:
            lastPos = _abun.tell()
            line = _abun.readline()
            if (line.startswith("#") or line.startswith("@")):
                continue
            _abun.seek(lastPos)
            break
        while True:
            lines = _abun.readlines(65535)
            if not lines:
                break
            for line in lines:
                ele = line.rstrip().split("\t")
                if ele[1] != "species":
                    continue
                
                if ele[0] in stdSpeInfo:
                    abunDict[ele[0]] = float(ele[-1])
                    continue

                c = False
                for tax in ele[2].split("|"):
                    if tax in stdSpeInfo:
                        abunDict[tax] = float(ele[-1])
                        c = True
                        break
                if c: continue

                taxPathSN = ele[3].split("|")
                for k, v in stdSpeInfo.items():
                    if v["name"] in taxPathSN:
                        abunDict[k] = float(ele[-1])
                        break
    return abunDict

def drawColumn(stdSpeInfo = dict(), abunLabel = [], abun1=dict(), abun2=dict(), abun3=dict(), outputPDF="./test.barplot.pdf"):
    vlist1 = []
    vlist2 = []
    vlist3 = []

    vstd = []
    labels = []
    l1norm1 = 0.0
    l1norm2 = 0.0
    l1norm3 = 0.0

    for k, v in stdSpeInfo.items():
        std = float(v["percent"]*100)
        v1 = float(abun1.get(k, 0.0))
        l1norm1 += abs(v1-std)
        v2 = float(abun2.get(k, 0.0))
        l1norm2 += abs(v2-std)
        v3 = float(abun3.get(k, 0.0))
        l1norm3 += abs(v3-std)
        vlist1.append(v1)
        vlist2.append(v2)
        vlist3.append(v3)
        vstd.append(std)
        labels.append("{0}\n(GC {1:.2%})".format(v["name"], float(v["GC"])))
    
    x = np.arange(len(labels))  # the label locations
    width = 0.4  # the width of the bars

    fig, ax = plt.subplots()
    plt.ylim([0, 100])
    rects1 = ax.bar(x - width * 3/4, vlist1, width/2, label=abunLabel[0])
    rects2 = ax.bar(x - width/4, vlist2, width/2, label=abunLabel[1])
    rects3 = ax.bar(x + width/4, vlist3, width/2, label=abunLabel[2])
    rectsstd = ax.bar(x + width * 3/4, vstd, width/2, label="raw DNA percent")

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Percentage (relative abundance) %')
    ax.set_title('GC Bias Recalibration Comparison')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation = 72)
    ax.legend()

    plt.text(1.2, 40, "l1Norm error:\n{label0}: {l1n0:.4}\n{label1}: {l1n1:.4}\n{label2}: {l1n2:.4}".format(label0=abunLabel[0], l1n0=l1norm1/100, label1=abunLabel[1], l1n1=l1norm2/100, label2=abunLabel[2], l1n2=l1norm3/100), fontsize=8)

    fig.tight_layout()
    plt.savefig(outputPDF, quality=100, format="pdf")
        

def main():
    if (len(sys.argv) != 7):
        print("usage: python3 " + sys.argv[0] + " <SampleSpeContentFile csv> <CAMIFORM abun1> <CAMIFORM abun2> <CAMIFORM abun3> <tool_labels> <output.pdf>")
        print("example: python3 GCBias_grouped_barplot.py SampleSpeciesContent_SRR3575975.csv SOAPMetas_SRR3575975_CAMIFORM.profile MetaPhlAn2_SRR3575975_CAMIFORM.profile SOAPMetas_MEPHProfiling_SRR3575975_CAMIFORM.profile \"SOAPMetas_recali,MetaPhlAn2,SOAPMetas_recali_improve\" test.pdf")
        sys.exit(0)

    stdSpeInfo = getStdSpeciesInfo(sys.argv[1])
    abun1 = readCAMIFormData(sys.argv[2], stdSpeInfo=stdSpeInfo)
    abun2 = readCAMIFormData(sys.argv[3], stdSpeInfo=stdSpeInfo)
    abun3 = readCAMIFormData(sys.argv[4], stdSpeInfo=stdSpeInfo)
    abunLabel = sys.argv[5].split(",")
    drawColumn(stdSpeInfo=stdSpeInfo, abunLabel= abunLabel, abun1= abun1, abun2=abun2, abun3= abun3, outputPDF=sys.argv[6])

if __name__ == "__main__":
    main()
    #plotTest()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
    File:	Scatter_plot.py
    Author:	Shixu He
    Email:	heshixu@genomics.cn
    Date:	2019-08-23
    ------
    Plot for SOAPMetaS
    ------
    Version:
'''

#import numpy


import numpy as np
from numpy.polynomial.polynomial import polyfit
from matplotlib.patches import Rectangle
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import math

#from matplotlib import rcParams
#from matplotlib.font_manager import FontProperties
# 解决负号'-'显示为方块的问题
#myfont = FontProperties(fname='/usr/share/fonts/adobe-source-han-serif/SourceHanSerifCN-Regular.otf')
#解决负号'-'显示为方块的问题
#matplotlib.rcParams['axes.unicode_minus']=False
#plt.title('乘客等级分布', fontproperties=myfont)
#plt.ylabel('人数', fontproperties=myfont)
#plt.legend(('头等舱', '二等舱', '三等舱'), loc='best', prop=myfont)



import sys
import re
import argparse

def plotTest():
    # Sample data
    x = -np.random.rand(20)
    y = 4*x + 0.01
    p = list(x)#.extend([0,0,0,0.1235,0.16153,0.8891])
    q = list(y)#.extend([0.1235,0.16153,0.8891,0,0,0])
    p.extend([0,0,0,0.1235,0.16153,0.8891])
    q.extend([0.1235,0.16153,0.8891,0,0,0])
    # Fit with polyfit
    b, m = polyfit(x, y, 1)
    plt.title("TEST plot")
    plt.xlabel("xxxx")
    plt.ylabel("yyy")
    x_max = max(p)
    y_max = max(q)
    y_min = min(q)
    l1norm_all = 0.1234
    l1norm_valid = 0.2345
    plt.text(x_max, y_max, "l1norm(all): {0:.4}".format(l1norm_all), {"ha": "right", "va": "top"})
    plt.text(x_max, y_max-(y_max-y_min)*0.1, "l1norm(valid): {0:.4}".format(l1norm_valid), {"ha": "right", "va": "top"})
    plt.grid(True)
    plt.plot(p, q, '.', color="red")
    plt.plot(x, b + m * x, '-', color="grey")
    #plt.savefig("output.pdf", quality=100, format="pdf")
    plt.show()

def readCAMIFormAbunData(abundanceFile):
    taxIDAbunDict = dict()
    with open(abundanceFile, 'rt', encoding='utf-8') as _abunf:
        while True:
            lastPos = _abunf.tell()
            line = _abunf.readline()
            if line.startswith("@"):
                continue
            if line.startswith("#"):
                continue
            if len(re.split(r"\t", line.rstrip())) < 4:
                continue
            _abunf.seek(lastPos)
            break
        while True:
            lines = _abunf.readlines(65535)
            if not lines:
                break
            for line in lines:
                lineEle = re.split(r"\t", line.rstrip())
                try:
                    if (lineEle[1] == "species"):
                        taxIDAbunDict[lineEle[0]] = float(lineEle[4])
                except IndexError as e:
                    print(e + " . Current line: " + str(lineEle))

    return taxIDAbunDict

def drawLine(title=None, xAbunDict=dict(), xLabel="x", yAbunDict=dict(), yLabel="y", outFile = "SOAPMetas_Scatter_plot_sample.pdf"):
    # all values for draw dots
    #x_values = []
    #y_values = []
    xScaleNum = 1.0
    yScaleNum = 1.0
    l1norm_all = 0
    l1norm_valid = 0
    if (sum(xAbunDict.values()) > 1 ):
        xScaleNum = 100.0
    if (sum(yAbunDict.values()) > 1 ):
        yScaleNum = 100.0

    # common values for plot fit
    x_common = [] 
    y_common = []

    # uncommon values
    x_uncon = []
    y_uncon = []

    taxSet = set(xAbunDict.keys()).union(yAbunDict.keys())
    for tax in taxSet:
        x0 = xAbunDict.get(tax, 0.0)/xScaleNum
        y0 = yAbunDict.get(tax, 0.0)/yScaleNum
        l1norm_all += abs(x0-y0)
        #x_values.append(x0)
        #y_values.append(y0)
        #if (x0>0 and y0>0):
        #    l1norm_valid += abs(x0-y0)
        #    x_common.append(x0)
        #    y_common.append(y0)
        #elif (x0>0 or y0>0):
        #    x_uncon.append(x0)
        #    y_uncon.append(y0)
        #else:
        #    continue
        if (x0>0 and y0>0):
            l1norm_valid += abs(x0-y0)
            x_common.append(math.log10(x0))
            y_common.append(math.log10(y0))
        else:
            x_uncon.append(math.log10(x0) if x0>0 else 0)
            y_uncon.append(math.log10(y0) if y0>0 else 0)
    
    #c_0, c_1 = polyfit(x_common, y_common, 1)
    #x_max = max(max(x_common) if len(x_common) > 0 else 0, max(x_uncon) if len(x_uncon) > 0 else 0)
    #y_min = min(min(y_common) if len(y_common) > 0 else 0, min(y_uncon) if len(y_uncon) > 0 else 0, min(x_common) if len(x_common) > 0 else 0, min(x_uncon) if len(x_uncon) > 0 else 0)
    
    #plt.text(x_max, y_min*0.86, "Proximity (all): {0:.4}".format(2-l1norm_all), {"ha": "right", "va": "top"})
    #plt.text(x_max, y_min*0.9, "Proximity (valid): {0:.4}".format(2-l1norm_valid), {"ha": "right", "va": "top"})

    fig, axes = plt.subplots(1, 1)
    if title != None:
        plt.title(title)
    plt.xlabel(xLabel+ " (log)")
    plt.ylabel(yLabel+ " (log)")

    #fig.text(0.95, 0.85, "l1norm (all): {0:.4}".format(l1norm_all), ha="right")
    #fig.text(0.95, 0.8, "l1norm (>0): {0:.4}".format(l1norm_valid), ha="right")
    axes.grid(True)

    axes.set_xlim(-6, 1)
    axes.set_ylim(-6, 1)
    dots1, = axes.plot(x_uncon, y_uncon, "x", color="gray")
    dots2, = axes.plot(x_common, y_common, 's', color="red")
 
    xc = np.asarray(x_common)
    #plt.plot(xc, c_0 + c_1 * xc, '-', color="tomato")
    line1 = axes.plot(xc, xc, "-", color="black", linewidth=1)

    extra1 = Rectangle((0, 0), 1, 1, fc="w", fill=False, edgecolor='none', linewidth=0)
    extra2 = Rectangle((0, 0), 1, 1, fc="w", fill=False, edgecolor='none', linewidth=0)
    axes.legend([dots1, dots2, extra1, extra2], ("Uncommon", "Common", "l1norm (all): {0:.4}".format(l1norm_all), "l1norm (>0): {0:.4}".format(l1norm_valid)), loc = 1)
    #plt.axis("equal")
    plt.tight_layout()
    plt.savefig(outFile, quality=100, format="pdf")
    #plt.show()

def checkArgv():
    '''Parse the command line parameters.'''
    _parser = argparse.ArgumentParser(description="The script is used for abundance calculation from alignment of bowtie/bwa.")
    
    _parser.add_argument(
        "--title",
        required=False,
        default=None,
        help="Plot title."
    )

    _parser.add_argument(
        "--xlabel",
        required=False,
        default="x label",
        help="Lagend of axis-x."
    )

    _parser.add_argument(
        "--ylabel",
        required=False,
        default="y label",
        help="Lagend of axis-y."
    )
    
    _parser.add_argument(
        "--output",
        required=False,
        default="./SOAPMetas_Scatter_plot_sample.pdf",
        help="Output plot file name."
    )

    _parser.add_argument(
        "--input-abun-x",
        required=True,
        help="CAMI format abundance file, the data is used for x-coordinate."
    )

    _parser.add_argument(
        "--input-abun-y",
        required=True,
        help="CAMI format abundance file, the data is used for y-coordinate."
    )

    _parser.add_argument(
        "--pic-format",
        required=False,
        default="pdf",
        help="Plot format. [pdf, png, jpg, ...]"
    )
    return _parser.parse_args()

def main():
    args = checkArgv()
    #plotTest()
    xInputFile = args.input_abun_x
    yInputFile = args.input_abun_y
    xAbunDict = readCAMIFormAbunData(xInputFile)
    yAbunDict = readCAMIFormAbunData(yInputFile)
    drawLine(title=args.title, xAbunDict = xAbunDict, xLabel=args.xlabel, yAbunDict = yAbunDict, yLabel=args.ylabel, outFile=args.output)


if __name__ == "__main__":
    #plotTest()
    main()

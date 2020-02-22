#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
    File:	Scatter_plot.py
    Author:	Shixu He
    Email:	heshixu@genomics.cn
    Date:	2019-08-23
    ------
    Plot for SOAPMetaS Fig.2
    ------
    Version:
'''

#import numpy


import numpy as np
from numpy.polynomial.polynomial import polyfit
import scipy.stats
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import math

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

def readGeneProfiling(abundanceFile, colN):
    taxIDAbunDict = dict()
    with open(abundanceFile, 'rt', encoding='utf-8') as _abunf:
        if colN == 2:
            while True:
                lines = _abunf.readlines(65535)
                if not lines:
                    break
                for line in lines:
                    lineEle = re.split(r"\t", line.rstrip())
                    taxIDAbunDict[lineEle[0]] = float(lineEle[1])
        elif colN == 5:
            while True:
                lines = _abunf.readlines(65535)
                if not lines:
                    break
                for line in lines:
                    lineEle = re.split(r"\t", line.rstrip())
                    taxIDAbunDict[lineEle[0]] = float(lineEle[4])
        else:
            cn = colN - 1
            while True:
                lines = _abunf.readlines(65535)
                if not lines:
                    break
                for line in lines:
                    lineEle = re.split(r"\t", line.rstrip())
                    taxIDAbunDict[lineEle[0]] = float(lineEle[cn])
    return taxIDAbunDict

def drawLine(title="Sample Plot", xAbunDict=dict(), xLabel="x", yAbunDict=dict(), yLabel="y", pic_format="pdf", outFile = "SOAPMetas_Scatter_plot_sample.pdf", doLog=False):
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
    if doLog:
        for tax in taxSet:
            x0 = xAbunDict.get(tax, 0.0)/xScaleNum
            y0 = yAbunDict.get(tax, 0.0)/yScaleNum
            l1norm_all += abs(x0-y0)
            #x_values.append(x0)
            #y_values.append(y0)
            if (x0>0 and y0>0):
                l1norm_valid += abs(x0-y0)
                x_common.append(math.log10(x0))
                y_common.append(math.log10(y0))
            elif (x0>0 or y0>0):
                x_uncon.append(math.log10(x0) if x0>0 else 0)
                y_uncon.append(math.log10(y0) if y0>0 else 0)
            else:
                continue
    else:
        for tax in taxSet:
            x0 = xAbunDict.get(tax, 0.0)/xScaleNum
            y0 = yAbunDict.get(tax, 0.0)/yScaleNum
            l1norm_all += abs(x0-y0)
            #x_values.append(x0)
            #y_values.append(y0)
            if (x0>0 and y0>0):
                l1norm_valid += abs(x0-y0)
                x_common.append(x0)
                y_common.append(y0)
            elif (x0>0 or y0>0):
                x_uncon.append(x0)
                y_uncon.append(y0)
            else:
                continue
    
    c_0, c_1 = polyfit(x_common, y_common, 1)
    slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(x_common, y_common)
    #x_max = max(max(x_common) if len(x_common) > 0 else 0, max(x_uncon) if len(x_uncon) > 0 else 0)
    #y_min = min(min(y_common) if len(y_common) > 0 else 0, min(y_uncon) if len(y_uncon) > 0 else 0, min(x_common) if len(x_common) > 0 else 0, min(x_uncon) if len(x_uncon) > 0 else 0)
    #plt.text(x_max, y_min*0.86, "Proximity (all): {0:.4}".format(2-l1norm_all), {"ha": "right", "va": "top"})
    #plt.text(x_max, y_min*0.9, "Proximity (valid): {0:.4}".format(2-l1norm_valid), {"ha": "right", "va": "top"})


    fig, axes = plt.subplots(1, 1)
    plt.title(title)
    plt.xlabel(xLabel)
    plt.ylabel(yLabel)

    fig.text(0.90, 0.85, "r^2 = {0:.4}".format(r_value**2), ha="right")
    fig.text(0.90, 0.80, "p = {0:.4}".format(p_value), ha="right")
    fig.text(0.90, 0.75, "slope = {0:.4}".format(slope), ha="right")
    fig.text(0.90, 0.70, "intercept = {0:.4}".format(intercept), ha="right")
    axes.grid(True)

    axes.plot(x_uncon, y_uncon, ".", color="gray")
    axes.plot(x_common, y_common, '.', color="orchid")
    
    xc = np.asarray(x_common)
    plt.plot(xc, intercept + slope * xc, '-', color="tomato")
    #axes.plot(xc, xc, "-", color="tomato")
    #plt.axis("equal")
    fig.tight_layout()
    plt.savefig(outFile, quality=100, format=pic_format)
    #plt.show()

def checkArgv():
    '''Parse the command line parameters.'''
    _parser = argparse.ArgumentParser(description="The script is used for abundance calculation from alignment of bowtie/bwa.")
    
    _parser.add_argument(
        "--title",
        required=False,
        default="Sample Plot",
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
        help="Output plot file name. (pdf format)"
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
        default="pdf"
    )

    _parser.add_argument(
        "--log-norm",
        action="store_true",
        help="whether calculate the log value"
    )

    _parser.add_argument(
        "--abun-col",
        type=int,
        default=2,
        help="The abundance column in .abundance file. 2 for SOAPMetaS default output, 5 for SOAOPMetaS detailed output"
    )


    return _parser.parse_args()

def main():
    args = checkArgv()
    #plotTest()
    xInputFile = args.input_abun_x
    yInputFile = args.input_abun_y
    xAbunDict = readGeneProfiling(xInputFile, args.abun_col)
    yAbunDict = readGeneProfiling(yInputFile, args.abun_col)
    drawLine(title=args.title, xAbunDict = xAbunDict, xLabel=args.xlabel, yAbunDict = yAbunDict, yLabel=args.ylabel, pic_format=args.pic_format, outFile=args.output, doLog =args.log_norm)


if __name__ == "__main__":
    #plotTest()
    main()

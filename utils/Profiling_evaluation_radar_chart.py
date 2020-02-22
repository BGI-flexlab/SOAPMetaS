#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: hsx (heshixu@genomics.cn)
# Created Time: Mon 2019-11-11 10:37:16 CST

#module#
import sys
import re
import argparse

import numpy as np

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.path import Path
from matplotlib.spines import Spine
from matplotlib.projections.polar import PolarAxes
from matplotlib.projections import register_projection

L1NORM_SCALE=200

def radar_factory(num_vars, frame='circle'):
    """Create a radar chart with `num_vars` axes.

    This function creates a RadarAxes projection and registers it.

    Parameters
    ----------
    num_vars : int
        Number of variables for radar chart.
    frame : {'circle' | 'polygon'}
        Shape of frame surrounding axes.

    """
    # calculate evenly-spaced axis angles
    theta = np.linspace(0, 2*np.pi, num_vars, endpoint=False)

    def draw_poly_patch(self):
        # rotate theta such that the first axis is at the top
        verts = unit_poly_verts(theta + np.pi / 2)
        return plt.Polygon(verts, closed=True, edgecolor='k')

    def draw_circle_patch(self):
        # unit circle centered on (0.5, 0.5)
        return plt.Circle((0.5, 0.5), 0.5)

    patch_dict = {'polygon': draw_poly_patch, 'circle': draw_circle_patch}
    if frame not in patch_dict:
        raise ValueError('unknown value for `frame`: %s' % frame)

    class RadarAxes(PolarAxes):

        name = 'radar'
        # use 1 line segment to connect specified points
        RESOLUTION = 1
        # define draw_frame method
        draw_patch = patch_dict[frame]

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # rotate plot such that the first axis is at the top
            self.set_theta_zero_location('N')

        def fill(self, *args, closed=True, **kwargs):
            """Override fill so that line is closed by default"""
            return super().fill(closed=closed, *args, **kwargs)

        def plot(self, *args, **kwargs):
            """Override plot so that line is closed by default"""
            lines = super().plot(*args, **kwargs)
            for line in lines:
                self._close_line(line)

        def _close_line(self, line):
            x, y = line.get_data()
            # FIXME: markers at x[0], y[0] get doubled-up
            if x[0] != x[-1]:
                x = np.concatenate((x, [x[0]]))
                y = np.concatenate((y, [y[0]]))
                line.set_data(x, y)

        def set_varlabels(self, labels):
            self.set_thetagrids(np.degrees(theta), labels)

        def _gen_axes_patch(self):
            return self.draw_patch()

        def _gen_axes_spines(self):
            if frame == 'circle':
                return super()._gen_axes_spines()
            # The following is a hack to get the spines (i.e. the axes frame)
            # to draw correctly for a polygon frame.

            # spine_type must be 'left', 'right', 'top', 'bottom', or `circle`.
            spine_type = 'circle'
            verts = unit_poly_verts(theta + np.pi / 2)
            # close off polygon by repeating first vertex
            verts.append(verts[0])
            path = Path(verts)

            spine = Spine(self, spine_type, path)
            spine.set_transform(self.transAxes)
            return {'polar': spine}

    register_projection(RadarAxes)
    return theta


def unit_poly_verts(theta):
    """Return vertices of polygon for subplot axes.

    This polygon is circumscribed by a unit circle centered at (0.5, 0.5)
    """
    x0, y0, r = [0.5] * 3
    verts = [(r*np.cos(t) + x0, r*np.sin(t) + y0) for t in theta]
    return verts

def dataMatrixGenerate(inputFiles, standard, taxon):
    '''
    Output data matrix:
    [
        (taxon, {
            "percise": [tool1_precise, tool2_precise, ...],
            "recall": [tool1_recall, tool2_recall, ...],
            "proximity": [tool1_proximity, tool2_proximity, ...]
        })
    ]
    '''
    data = []
    
    # profileDict: { taxon1: percent1. taxon2: percent2, ...}
    stdProfileDict = _readProfile(standard, taxon)
    preciseList = []
    recallList = []
    proximityList =[]
    for inf in inputFiles:
        print("read input file: " + inf, file=sys.stdout)
        profileDict = _readProfile(inf, taxon)

        precise, recall = classificationEvaluation(set(stdProfileDict.keys()), set(profileDict.keys()))
        l1norm = speciesL1NormStats(stdProfileDict, profileDict)
        proximity = 1-l1norm/L1NORM_SCALE

        preciseList.append(precise)
        recallList.append(recall)
        proximityList.append(proximity)
    
    data.append( (taxon, {"precise": preciseList, "recall": recallList, "proximity": proximityList}) )

    return data


def speciesL1NormStats(valueDictA, valueDictB):
    nameSet = set(valueDictA.keys()).union(set(valueDictB.keys()))
    _L1Norm = 0
    for _spe in nameSet:
        _L1Norm += abs(valueDictA.get(_spe, 0) - valueDictB.get(_spe, 0))
    return _L1Norm

def classificationEvaluation(setReal=set(), setPredicted=set()):
    _eleNumReal = len(setReal)
    _eleNumPredicted = len(setPredicted)
    if (_eleNumPredicted == 0) or (_eleNumReal == 0):
        return (0, 0)
    _truePosi = len(setReal.intersection(setPredicted))
    _precision = _truePosi/_eleNumPredicted
    _recall = _truePosi/_eleNumReal
    return (_precision, _recall)

def _readProfile(profile, taxon):
    '''
    output dict:
        { taxon1: percent1. taxon2: percent2, ...}
    '''
    discard = ["#", "@"]
    profileDict = dict()
    with open(profile, "rt") as _inp:
        for line in _inp:
            if line[0] in discard:
                continue
            ele = line.rstrip().split("\t")
            if len(ele) < 2:
                print(line)
                continue
            if ele[1] != taxon:
                continue
            profileDict[ele[0]] = float(ele[4])
    return profileDict

def example_data():
    # The following data is from the Denver Aerosol Sources and Health study.
    # See doi:10.1016/j.atmosenv.2008.12.017
    #
    # The data are pollution source profile estimates for five modeled
    # pollution sources (e.g., cars, wood-burning, etc) that emit 7-9 chemical
    # species. The radar charts are experimented with here to see if we can
    # nicely visualize how the modeled source profiles change across four
    # scenarios:
    #  1) No gas-phase species present, just seven particulate counts on
    #     Sulfate
    #     Nitrate
    #     Elemental Carbon (EC)
    #     Organic Carbon fraction 1 (OC)
    #     Organic Carbon fraction 2 (OC2)
    #     Organic Carbon fraction 3 (OC3)
    #     Pyrolized Organic Carbon (OP)
    #  2)Inclusion of gas-phase specie carbon monoxide (CO)
    #  3)Inclusion of gas-phase specie ozone (O3).
    #  4)Inclusion of both gas-phase species is present...
    data = [
        ['Sulfate', 'Nitrate', 'EC', 'OC1', 'OC2', 'OC3', 'OP', 'CO', 'O3'],
        ('Basecase', [
            [0.88, 0.01, 0.03, 0.03, 0.00, 0.06, 0.01, 0.00, 0.00],
            [0.07, 0.95, 0.04, 0.05, 0.00, 0.02, 0.01, 0.00, 0.00],
            [0.01, 0.02, 0.85, 0.19, 0.05, 0.10, 0.00, 0.00, 0.00],
            [0.02, 0.01, 0.07, 0.01, 0.21, 0.12, 0.98, 0.00, 0.00],
            [0.01, 0.01, 0.02, 0.71, 0.74, 0.70, 0.00, 0.00, 0.00]]),
        ('With CO', [
            [0.88, 0.02, 0.02, 0.02, 0.00, 0.05, 0.00, 0.05, 0.00],
            [0.08, 0.94, 0.04, 0.02, 0.00, 0.01, 0.12, 0.04, 0.00],
            [0.01, 0.01, 0.79, 0.10, 0.00, 0.05, 0.00, 0.31, 0.00],
            [0.00, 0.02, 0.03, 0.38, 0.31, 0.31, 0.00, 0.59, 0.00],
            [0.02, 0.02, 0.11, 0.47, 0.69, 0.58, 0.88, 0.00, 0.00]]),
        ('With O3', [
            [0.89, 0.01, 0.07, 0.00, 0.00, 0.05, 0.00, 0.00, 0.03],
            [0.07, 0.95, 0.05, 0.04, 0.00, 0.02, 0.12, 0.00, 0.00],
            [0.01, 0.02, 0.86, 0.27, 0.16, 0.19, 0.00, 0.00, 0.00],
            [0.01, 0.03, 0.00, 0.32, 0.29, 0.27, 0.00, 0.00, 0.95],
            [0.02, 0.00, 0.03, 0.37, 0.56, 0.47, 0.87, 0.00, 0.00]]),
        ('CO & O3', [
            [0.87, 0.01, 0.08, 0.00, 0.00, 0.04, 0.00, 0.00, 0.01],
            [0.09, 0.95, 0.02, 0.03, 0.00, 0.01, 0.13, 0.06, 0.00],
            [0.01, 0.02, 0.71, 0.24, 0.13, 0.16, 0.00, 0.50, 0.00],
            [0.01, 0.03, 0.00, 0.28, 0.24, 0.23, 0.00, 0.44, 0.88],
            [0.02, 0.00, 0.18, 0.45, 0.64, 0.55, 0.86, 0.00, 0.16]])
    ]
    return data

def getTaxonName(c):
    if c == "t":
        print("No support for strain level now, we change to spacies level", file = sys.stderr)
        return "species"
    taxonName = {
        "k": "superkindom",
        "p": "phylum",
        "c": "class",
        "o": "order",
        "f": "family",
        "g": "genus",
        "s": "species"
        }
    return taxonName[c]

def checkArg():
    parser=argparse.ArgumentParser(description="Radar plot for precision/recall/proximity(1-l1norm) evaluation of CAMI format profiling result.",usage="python3 %(prog)s [options] -v [vertex label list] -i [input profile list]",add_help="TRUE")

    parser.add_argument("-v", "--vertex",
        required = True,
        help = "Labels for each vertices on radar plot. comma seperated. the number of vertices must be equal to number of input profiling file. Notice that the order of vertices must be the same as input files."
        )

    parser.add_argument("-t", "--title",
        required = False,
        default = "Radar plot of precise, recall, proximity of different tool",
        help = "Plot title."
        )

    parser.add_argument("-i", "--input",
        required = True,
        help = "input CAMI format profiling result files. comma seperated. the number of input file must be equal to number of vertices. Notice that the order of input files must be the same as vertices."
        )

    parser.add_argument("--taxon", 
        required = False,
        default = "s",
        choices = ["k", "p", "c", "o", "f", "g", "s", "t"],
        help = "taxonomy level for estimation."
        )
    
    parser.add_argument("-s", "--standard",
        required = True,
        help = "gold standard profiling file."
        )

    parser.add_argument("-o", "--output",
        required = False,
        default = "Radar_precise_recall_estimation",
        help = "output file name (prefix)."
        )

    parser.add_argument("-f", "--format",
        required = False,
        default = "pdf",
        choices = ["pdf", "png"],
        help = "output image format."
        )

    return parser.parse_args()

if __name__ == '__main__':
    args = checkArg()
    output = args.output
    outFormat = args.format
    outFileName = output + "." + outFormat
    vertices = re.sub(r"\s+", "", args.vertex).split(",")
    inputs = re.sub(r"\s+", "", args.input).split(",")
    std = args.standard
    title = args.title
    taxon = getTaxonName(args.taxon)

    if len(inputs)<3:
        print("Radar plot needs at least three input file !", file=sys.stderr)
        sys.exit(1)

    if not len(vertices)==len(inputs):
        print("input profiling file number are not equal to vertex number. please check your arguments", file=sys.stderr)
        sys.exit(1)

    N = len(inputs)
    theta = radar_factory(N, frame='polygon')

    #data = example_data()
    #spoke_labels = data.pop(0)

    data = dataMatrixGenerate(inputs, std, taxon)
    spoke_labels = vertices
    print(spoke_labels)
    print(data)

    fig, axes = plt.subplots(figsize=(6, 6), nrows=1, ncols=1,
                             subplot_kw=dict(projection='radar'))
    fig.subplots_adjust(wspace=0.25, hspace=0.20, top=0.85, bottom=0.05)

    colors = ['b', 'r', 'g'] #, 'm', 'y']
    legend_labels = ("precise", "recall", "proximity")
    # Plot the four cases from the example data on separate axes
    #for ax, (subTitle, case_data) in zip(axes.flatten(), data): # !!note: we set nrow and ncols to 1, so here the axes is scalar
    for ax, (subTitle, case_data) in zip([axes], data):
        ax.set_rgrids([x*0.2 for x in range(1, 6, 1)])
        ax.set_title(subTitle, weight='bold', size='medium', position=(0.5, 1.1),
                     horizontalalignment='center', verticalalignment='center')
        for k, color in zip(legend_labels, colors):
            ax.plot(theta, case_data[k], color=color, scalex=False, scaley=False)
            #ax.fill(theta, case_data[k], facecolor=color, alpha=0.25)
        ax.set_varlabels(spoke_labels)

    # add legend relative to top-left plot
    #ax = axes[0, 0]
    ax = axes # !! note: same as line 331
    legend = ax.legend(legend_labels, loc=(0.9, .95),
                       labelspacing=0.1, fontsize='small')

    #fig.text(0.5, 0.965, title,
    #         horizontalalignment='center', color='black', weight='bold',
    #         size='large')

    #plt.show()
    plt.savefig(outFileName, quality=100, format=outFormat)


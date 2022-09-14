import ROOT
import array
from ROOT import gROOT
from array import array
import math
import CMS_lumi, tdrstyle
import argparse

def resolution(histo):
	nBins = histo.GetNbinsX()
	x, y = array( 'd' ), array( 'd' )
	xerr, yerr = array( 'd' ), array( 'd' )
	for i in range(histo.GetNbinsX()):
		histo_Y = histo.ProjectionY("histo_Y",i,i,"e")
		xaxis = histo.GetXaxis()
		x.append(xaxis.GetBinCenter(i+1))
		xerr.append(xaxis.GetBinWidth(i+1)/2)
		y.append(histo_Y.GetStdDev())
		yerr.append(histo_Y.GetStdDevError())

	return(ROOT.TGraphErrors(nBins,x,y,xerr,yerr))

# Define input arguments
parser = argparse.ArgumentParser(description='')
parser.add_argument('-i', '--inFileName', type=str, default='', help='Input json file')
args = parser.parse_args()

inFileName = args.inFileName

inFile = ROOT.TFile.Open(inFileName,"READ")

h_uperp_raw_vs_pv = inFile.Get("uperp_raw_vs_pv")
h_uperp_pf_vs_pv = inFile.Get("uperp_pf_vs_pv")
h_uperp_puppi_vs_pv = inFile.Get("uperp_puppi_vs_pv")

resolution_raw = resolution(h_uperp_raw_vs_pv)
resolution_pf = resolution(h_uperp_pf_vs_pv)
resolution_puppi = resolution(h_uperp_puppi_vs_pv)

# Drawing
tdrstyle.setTDRStyle()

#change the CMS_lumi variables (see CMS_lumi.py)
CMS_lumi.lumi_13TeV = "7.6 fb^{-1}"
CMS_lumi.writeExtraText = 1
CMS_lumi.extraText = "Preliminary"
CMS_lumi.lumi_sqrtS = "13 TeV" # used with iPeriod = 0, e.g. for simulation-only plots (default is an empty string)

iPos = 11

iPeriod = 4

H_ref = 650 
W_ref = 800 
W = W_ref
H  = H_ref

# references for T, B, L, R
T = 0.08*H_ref
B = 0.12*H_ref 
L = 0.12*W_ref
R = 0.04*W_ref

canvas = ROOT.TCanvas("c2","c2",50,50,W,H)
canvas.SetFillColor(0)
canvas.SetBorderMode(0)
canvas.SetFrameFillStyle(0)
canvas.SetFrameBorderMode(0)
canvas.SetLeftMargin( L/W )
canvas.SetRightMargin( R/W )
canvas.SetTopMargin( T/H )
canvas.SetBottomMargin( 1.1 * B/H )
canvas.SetTickx(0)
canvas.SetTicky(0)

ROOT.gStyle.SetLegendBorderSize(0)
ROOT.gStyle.SetOptStat(0)

xAxis = resolution_raw.GetXaxis()
xAxis.SetNdivisions(6,5,0)

yAxis = resolution_raw.GetYaxis()
yAxis.SetNdivisions(6,5,0)
yAxis.SetTitleOffset(1)

resolution_raw.Draw("APZ")
resolution_raw.SetMarkerStyle(20)
resolution_raw.GetXaxis().SetTitle("Number of primary vertices")
resolution_raw.GetYaxis().SetTitle("#sigma(u_{#perp}  ) (GeV)")
resolution_raw.SetMinimum(10)
resolution_raw.SetMaximum(50)

resolution_pf.Draw("PZ")
resolution_pf.SetLineColor(ROOT.kBlue)
resolution_pf.SetMarkerColor(ROOT.kBlue)
resolution_pf.SetMarkerStyle(21)

resolution_puppi.Draw("PZ")
resolution_puppi.SetLineColor(ROOT.kRed)
resolution_puppi.SetMarkerColor(ROOT.kRed)
resolution_puppi.SetMarkerStyle(22)

#draw the lumi text on the canvas
CMS_lumi.CMS_lumi(canvas, iPeriod, iPos)

canvas.cd()
canvas.Update()
canvas.RedrawAxis()
frame = canvas.GetFrame()
frame.Draw()

leg = ROOT.TLegend(.35,.7,.55,.9)
leg.SetBorderSize(0)
leg.SetFillColor(0)
leg.SetFillStyle(0)
leg.SetTextFont(42)
leg.SetTextSize(0.04)
leg.SetHeader("#gamma+jets (2016G)","C");
leg.AddEntry(resolution_puppi,"Puppi p_{T}^{miss}","lep")
leg.AddEntry(resolution_pf,"PF p_{T}^{miss}","lep")
leg.AddEntry(resolution_raw,"Raw p_{T}^{miss}","lep")
leg.Draw()

canvas.SaveAs("plots/resolution_uperp_pv.png")
canvas.SaveAs("plots/resolution_uperp_pv.pdf")

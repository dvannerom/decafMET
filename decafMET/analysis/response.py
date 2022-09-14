import ROOT
import array
from ROOT import gROOT
from array import array
import math
import CMS_lumi, tdrstyle
import argparse

#def response(histo):
#	nBins = histo.GetNbinsX()
#	x, y = array( 'd' ), array( 'd' )
#	xerr, yerr = array( 'd' ), array( 'd' )
#	for i in range(histo.GetNbinsX()):
#		xaxis = histo.GetXaxis()
#		x.append(xaxis.GetBinCenter(i+1))
#		xerr.append(xaxis.GetBinWidth(i+1)/2)
#		yaxis = histo.GetYaxis()
#		y.append(histo.GetBinContent(i+1,1)*yaxis.GetBinCenter(1))
#		sumEvents = histo.GetBinContent(i+1,1)
#		sumEventsY2 = histo.GetBinContent(i+1,1)*math.pow(yaxis.GetBinCenter(1),2)
#		for j in range(histo.GetNbinsY()-1):
#			y[i] += histo.GetBinContent(i+1,j+2)*yaxis.GetBinCenter(j+2)
#			sumEvents += histo.GetBinContent(i+1,j+2)
#			sumEventsY2 += histo.GetBinContent(i+1,j+2)*math.pow(yaxis.GetBinCenter(j+2),2)
#		
#		if sumEvents > 0:
#			sigma_num = sumEventsY2/math.pow(y[i],2)
#			y[i] = (y[i]/(sumEvents*x[i]))
#			sigma_den1 = 1./sumEvents
#			sigma_den2 = math.pow(xerr[i]/x[i],2)
#			yerr.append(y[i]*math.sqrt(sigma_num+sigma_den1+sigma_den2))
#		else:
#			y[i] = 0
#			yerr.append(0)
#
#	return(ROOT.TGraphErrors(nBins,x,y,xerr,yerr))

def response(histo):
	nBins = histo.GetNbinsX()
	x, y = array( 'd' ), array( 'd' )
	xerr, yerr = array( 'd' ), array( 'd' )
	for i in range(histo.GetNbinsX()):
		xaxis = histo.GetXaxis()
		x.append(xaxis.GetBinCenter(i+1))
		xerr.append(xaxis.GetBinWidth(i+1)/2)
		yaxis = histo.GetYaxis()
		histo_xSlice = ROOT.TH1F("", "", yaxis.GetNbins(), yaxis.GetBinLowEdge(1), yaxis.GetBinUpEdge(yaxis.GetNbins()))
		for j in range(histo.GetNbinsY()-1):
			histo_xSlice.SetBinContent(j+1,histo.GetBinContent(i+1,j+1))
			histo_xSlice.SetBinError(j+1,histo.GetBinError(i+1,j+1))
		y.append(histo_xSlice.GetMean())
		yerr.append(histo_xSlice.GetMeanError())
	return(ROOT.TGraphErrors(nBins,x,y,xerr,yerr))

# Define input arguments
parser = argparse.ArgumentParser(description='Plot the selected quantity for the selected particle')
parser.add_argument('-i', '--inputFile', type=str, default='', help='Input data file')
args = parser.parse_args()

inputFile = args.inputFile

inFile = ROOT.TFile.Open(inputFile,"READ")

#h_upar_raw_vs_qt = inFile.Get("neg_upar_raw_vs_qt")
#h_upar_pf_vs_qt = inFile.Get("neg_upar_pf_vs_qt")
#h_upar_puppi_vs_qt = inFile.Get("neg_upar_puppi_vs_qt")
#
#response_raw = response(h_upar_raw_vs_qt)
#response_pf = response(h_upar_pf_vs_qt)
#response_puppi = response(h_upar_puppi_vs_qt)

h_response_raw_vs_qt = inFile.Get("response_raw_vs_qt")
h_response_pf_vs_qt = inFile.Get("response_pf_vs_qt")
h_response_puppi_vs_qt = inFile.Get("response_puppi_vs_qt")

response_raw = response(h_response_raw_vs_qt)
response_pf = response(h_response_pf_vs_qt)
response_puppi = response(h_response_puppi_vs_qt)

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

xAxis = response_raw.GetXaxis()
xAxis.SetNdivisions(6,5,0)

yAxis = response_raw.GetYaxis()
yAxis.SetNdivisions(6,5,0)
yAxis.SetTitleOffset(1)

response_raw.Draw("APZ")
response_raw.SetMarkerStyle(20)
response_raw.SetMinimum(0.7)
response_raw.SetMaximum(1.25)
response_raw.GetXaxis().SetTitle("q_{T} (GeV)")
response_raw.GetYaxis().SetTitle("-<u_{#parallel}>/q_{T}")

response_pf.Draw("PZ")
response_pf.SetLineColor(ROOT.kBlue)
response_pf.SetMarkerColor(ROOT.kBlue)
response_pf.SetMarkerStyle(21)

response_puppi.Draw("PZ")
response_puppi.SetLineColor(ROOT.kRed)
response_puppi.SetMarkerColor(ROOT.kRed)
response_puppi.SetMarkerStyle(22)

#draw the lumi text on the canvas
CMS_lumi.CMS_lumi(canvas, iPeriod, iPos)

canvas.cd()
canvas.Update()
canvas.RedrawAxis()
frame = canvas.GetFrame()
frame.Draw()

leg = ROOT.TLegend(.75,.7,.92,.9)
leg.SetBorderSize(0)
leg.SetFillColor(0)
leg.SetFillStyle(0)
leg.SetTextFont(42)
leg.SetTextSize(0.04)
leg.SetHeader("#gamma+jets (2016G)","C");
leg.AddEntry(response_puppi,"Puppi p_{T}^{miss}","lep")
leg.AddEntry(response_pf,"PF p_{T}^{miss}","lep")
leg.AddEntry(response_raw,"Raw p_{T}^{miss}","lep")
leg.Draw()

canvas.SaveAs("plots/response.png")
canvas.SaveAs("plots/response.pdf")

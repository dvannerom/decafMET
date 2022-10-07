import ROOT
import array
from ROOT import gROOT
from array import array
import math
import CMS_lumi, tdrstyle
import argparse

def response(histo):
	nBinsX = histo.GetNbinsX()
	x, y = array( 'd' ), array( 'd' )
	xerr, yerr = array( 'd' ), array( 'd' )
	for i in range(nBinsX):
		xaxis = histo.GetXaxis()
		x.append(xaxis.GetBinCenter(i+1))
		xerr.append(xaxis.GetBinWidth(i+1)/2)
		yaxis = histo.GetYaxis()
		histo_xSlice = histo.ProjectionY("h_projectionY",i+1,i+1,"e")
		y.append(histo_xSlice.GetMean())
		yerr.append(histo_xSlice.GetMeanError())
	return(ROOT.TGraphErrors(nBinsX,x,y,xerr,yerr))

if __name__ == '__main__':
	# Define input arguments
	parser = argparse.ArgumentParser(description='Plot the selected quantity for the selected particle')
	parser.add_argument('-i', '--inputFile', type=str, default='', help='Input data file')
	parser.add_argument('-d', '--dataset', type=str, default='gamma', help='gamma or dy')
	parser.add_argument('-r', '--run', type=str, default='2022D', help='Data taking run')
	args = parser.parse_args()
	
	dataset = args.dataset
	inputFile = args.inputFile
	run = args.run
	
	inFile = ROOT.TFile.Open(inputFile,"READ")
	
	#h_response_raw_vs_qt = inFile.Get("response_raw_vs_qt")
	#h_response_pf_vs_qt = inFile.Get("response_pf_vs_qt")
	#h_response_puppi_vs_qt = inFile.Get("response_puppi_vs_qt")
	h_response_raw_vs_qt = inFile.Get("response_raw_vs_qt")
	h_response_pf_vs_qt = inFile.Get("response_pf_JECCorr_vs_qt")
	h_response_puppi_vs_qt = inFile.Get("response_puppi_JECCorr_vs_qt")
	
	response_raw = response(h_response_raw_vs_qt)
	response_pf = response(h_response_pf_vs_qt)
	response_puppi = response(h_response_puppi_vs_qt)

#	h_response_raw_vs_pv = inFile.Get("response_raw_vs_pv")
#	h_response_pf_vs_pv = inFile.Get("response_pf_vs_pv")
#	h_response_puppi_vs_pv = inFile.Get("response_puppi_vs_pv")
#	
#	response_raw = response(h_response_raw_vs_pv)
#	response_pf = response(h_response_pf_vs_pv)
#	response_puppi = response(h_response_puppi_vs_pv)
	
	# Drawing
	tdrstyle.setTDRStyle()
	
	#change the CMS_lumi variables (see CMS_lumi.py)
#	CMS_lumi.lumi_13TeV = "1.82 fb^{-1}"
	CMS_lumi.lumi_13TeV = "4.38 fb^{-1}"
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
	#canvas.SetTickx(0)
	#canvas.SetTicky(0)
	
	ROOT.gStyle.SetLegendBorderSize(0)
	ROOT.gStyle.SetOptStat(0)
	
	xAxis = response_raw.GetXaxis()
	xAxis.SetNdivisions(6,5,0)
	
	yAxis = response_raw.GetYaxis()
	yAxis.SetNdivisions(6,5,0)
	yAxis.SetTitleOffset(1)
	
	response_raw.Draw("APZ")
	response_raw.SetMarkerStyle(20)
	#response_raw.SetMinimum(0.7)
	response_raw.SetMinimum(0)
	response_raw.SetMaximum(1.45)
	response_raw.GetXaxis().SetTitle("q_{T} (GeV)")
	response_raw.GetYaxis().SetTitle("-<u_{#parallel}/q_{T}>")
	
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
	
	leg = ROOT.TLegend(.73,.7,.9,.9)
	leg.SetBorderSize(0)
	leg.SetFillColor(0)
	leg.SetFillStyle(0)
	leg.SetTextFont(42)
	leg.SetTextSize(0.04)
	#if dataset == "gamma": leg.SetHeader("#gamma+jets ("+run+")","C");
	#elif dataset == "dy": leg.SetHeader("Drell-Yan ("+run+")","C");
	#if dataset == "gamma": leg.SetHeader("#gamma+jets","C");
	#elif dataset == "dy": leg.SetHeader("Drell-Yan","C");
	leg.AddEntry(response_puppi,"Puppi p_{T}^{miss}","lep")
	leg.AddEntry(response_pf,"PF p_{T}^{miss}","lep")
	leg.AddEntry(response_raw,"Raw p_{T}^{miss}","lep")
	leg.Draw()
	
	canvas.SaveAs("plots/response.png")
	canvas.SaveAs("plots/response.pdf")

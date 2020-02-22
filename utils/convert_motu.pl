#!/usr/bin/env perl

#use lib "../";

use strict;
use warnings;
use Data::Dumper;
use Utils;

my ($filename_tooloutput, $dirWithNCBItaxDump, $sampleIDname) = @ARGV;
die "usage: perl $0 <original tool result file> <NCBI dump> [sampleID]
  tool result file: the original result of the original tool run.
  
  NCBI dump: must point to the directory that holds the unpacked dump 
             of the NCBI taxonomy, or more precise: the two files
             nodes.dmp and names.dmp
  
  sampleID: optional. Set a sample name.\n" if ((@ARGV < 2) || (@ARGV	 > 3));
$sampleIDname = "unknown" if (not defined $sampleIDname);

my %frequencies = ();
my $sum = 0;
open (IN, $filename_tooloutput) || die "cannot read orginal input file '$filename_tooloutput': $!\n";
	while (my $line = <IN>) {
		next if ($line =~ m/^#/);
		next if ($line =~ m/^taxaid\s+/);
		my ($taxid, $frequency) = split(m/\t|\n/, $line);
		$frequencies{$taxid} = $frequency if ($frequency != 0);
		$sum += $frequency;
	}
close (IN);

my %NCBItaxonomy = %{Utils::read_taxonomytree($dirWithNCBItaxDump."/nodes.dmp")};
my %NCBImerged = %{Utils::read_taxonomyMerged($dirWithNCBItaxDump."/merged.dmp")};
foreach my $taxid (keys(%frequencies)) {
	next if ($taxid < 0);
	if (not exists $NCBItaxonomy{$taxid}) {
		if (not exists $NCBImerged{$taxid}) {
			print "error: cannot find a taxonomy ID '$taxid'.\n";
		} else {
			my $newTaxid = $NCBImerged{$taxid};
			$frequencies{$newTaxid} += $frequencies{$taxid};
			delete $frequencies{$taxid};
		}
	}
}

my $taxonomydate = "unknown";
if (-e $dirWithNCBItaxDump."/taxdump.tar.gz.date") {
	$taxonomydate = qx(cat $dirWithNCBItaxDump/taxdump.tar.gz.date);
	chomp $taxonomydate;
}

my %tree = ();
my %NCBInames = %{Utils::read_taxonomyNames($dirWithNCBItaxDump."/names.dmp")};
foreach my $taxid (sort keys(%frequencies)) {
	my $abundance = 0;
	$abundance = $frequencies{$taxid} / $sum if ($sum > 0);
	my @lineage = ();
	if ($taxid == -1) {
		push @lineage, {taxid => -1, name => 'unassigned', rank => 'unassigned'};
	} else {
		@lineage = @{Utils::getLineage($taxid, \%NCBItaxonomy)};
		Utils::addNamesToLineage(\@lineage, \%NCBInames);
	}
	Utils::addNCBILineage(\%tree, \@lineage, $abundance*100);
}
Utils::pruneUnwantedRanks(\%tree);

print Utils::generateOutput("motu", $sampleIDname, \%tree, $taxonomydate);

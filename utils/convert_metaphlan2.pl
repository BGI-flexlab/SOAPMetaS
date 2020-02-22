#!/usr/bin/env perl

#use lib "../";

use strict;
use warnings;
use Data::Dumper;
use Utils;

my ($filename_taxids, $filename_metaphlanoutput, $dirWithNCBItaxDump, $sampleIDname) = @ARGV;
die "usage: perl $0 <mapping-file> <metaphlan2 result file> <NCBI dump> [sampleID]
  mapping-file: the file holding the one to one mappings from GreenGene to NCBI,
				which should have been created by 'buildIDlists.pl'.

  metaphlan2 result file: the original result of a metaphlan2 run.
  
  NCBI dump: must point to the directory that holds the unpacked dump 
			 of the NCBI taxonomy, or more precise: the two files
			 nodes.dmp and names.dmp

  sampleID: optional. Set a sample name.\n" if ((@ARGV < 3) || (@ARGV	 > 4));
$sampleIDname = "unknown" if (not defined $sampleIDname);

my %markerIDs = ();
my $taxonomydate = undef;
open (IN, $filename_taxids) || die "cannot read file with species2genomes mappings '$filename_taxids': $!\n";
	while (my $line = <IN>) {
		if ($line =~ m/#NCBI taxonomy downloaded at '(.+?)'/) {
			$taxonomydate = $1;
			next;
		}
		my ($id, $taxid, $lineageString) = split(m/\t|\n/, $line);
		$markerIDs{$id} = lineage_string2object($lineageString);
	}
close (IN);

my %metaphlanTree = ('children', undef);
open (IN, $filename_metaphlanoutput) || die;
	while (my $line = <IN>) {
		next if ($line =~ m/^#/);
		next if ($line !~ m/^k\_\_/); #there might pop up a line holding the "Identifier" of the sample, but it looks like there is no comment symbol. Thus, I relay on the fact that every lineage should start with the superkingdom, i.e. ^k__
		my ($lineageString, $abundance) = split(m/\t|\n/, $line);
		my @lineage = split(m/\|/, $lineageString);
		addLineage(\%metaphlanTree, \@lineage, $abundance);
	}
close (IN);

assignRealAbundance(\%metaphlanTree); #handle cases where a strain like "t__Ruminococcus_gnavus_unclassified" with its abundance points in reality to the according species, i.e. "s__Ruminococcus_gnavus". That might happen on all taxonomic ranks
my @abundanceLeaves = @{printLeafAbundances(\%metaphlanTree)}; #collect all those leaves and maybe branches of the GreenGene tree that holds a "realAbd"

my %NCBItaxonomy = %{Utils::read_taxonomytree($dirWithNCBItaxDump."/nodes.dmp")};
my %NCBInames = ();
my %NCBImerged = %{Utils::read_taxonomyMerged($dirWithNCBItaxDump."/merged.dmp")};
my %NCBItax = ('children', undef, 'rank', 'wholeTree', 'name', 'NCBItaxonomy');
my $idMismappings = -1;
my @missingTaxa = ();
foreach my $taxon (sort @abundanceLeaves) {
	if (exists $markerIDs{$taxon->{name}}) {
		my $taxid = $markerIDs{$taxon->{name}}->[scalar(@{$markerIDs{$taxon->{name}}})-1]->{taxid};
		if (not defined checkTaxid($taxid, \%NCBItaxonomy, \%NCBImerged)) {
			$NCBItax{children}->{-3}->{children}->{$taxid} = {abundance => $taxon->{abundance}, rank => 'unknownTaxid', name => $taxon->{lineage}};
		} else {
			Utils::addNCBILineage(\%NCBItax, $markerIDs{$taxon->{name}}, $taxon->{abundance});
		}
	} else {
		#species is unclassified, thus we find some "genus" which is not in the known IDs. Genus name is identical to species first word, thus we look for all species containing genus name and look for the deepest common taxid
		my @lineages = ();
		my ($genusName) = ($taxon->{name} =~ m/^g\_\_(.+?)$/);
		if (defined $genusName) {
			foreach my $ID (keys(%markerIDs)) {
				if ($ID =~ m/^s\_\_$genusName/) {
					#problems occure if it's a virus living in that species. Thus, we add only those lineages that contain the rank "genus"
					my $candidate = $markerIDs{$ID};
					my $candidateSuperkingdom = getRank($candidate, "superkingdom");
					if ((defined $candidateSuperkingdom) && ((($taxon->{lineage} =~ m/k\_\_Bacteria/) && ($candidateSuperkingdom == 2)) || (($taxon->{lineage} =~ m/k\_\_Viruses/) && ($candidateSuperkingdom == 10239)))) {
						push @lineages, $candidate;
					}
				}
			}
		}
				
		if (@lineages > 0) {
			my @commonLineage = @{Utils::getCommonLineage(\@lineages)};
			my $taxid = $commonLineage[$#commonLineage]->{taxid};
			if (not defined checkTaxid($taxid, \%NCBItaxonomy, \%NCBImerged)) {
				$NCBItax{children}->{-3}->{children}->{$taxid} = {abundance => $taxon->{abundance}, rank => 'unknownTaxid', name => $taxon->{lineage}};
			} else {
				Utils::addNCBILineage(\%NCBItax, \@commonLineage, $taxon->{abundance});
			}
		} else {
			next;
			#eine weiter Quelle um Namen den NCBI taxIDs zuzuordnen würde sich an Peter Hofmanns Idee orientieren und die textuellen Namen aus der names.dmp mit den hier auftauchenden Begriffen vergleichen. Dann könnte man die Lineage aufbauen und gucken, ob der Rang stimmt. Gabe neue Abhängikeiten zu names.dmp und nodes.dmp
			%NCBItaxonomy = %{Utils::read_taxonomytree($dirWithNCBItaxDump."/nodes.dmp")} if (scalar(keys(%NCBItaxonomy)) == 0);
			my @guessedLineage = @{Utils::guessByName($taxon, \%NCBItaxonomy, $dirWithNCBItaxDump."/names.dmp")};
			if (@guessedLineage > 0) {
				%NCBInames = %{Utils::read_taxonomyNames($dirWithNCBItaxDump."/names.dmp")} if (scalar(keys(%NCBInames)) == 0);
				Utils::addNamesToLineage(\@guessedLineage, \%NCBInames);
				my $taxid = $guessedLineage[$#guessedLineage]->{taxid};
				if (not defined checkTaxid($taxid, \%NCBItaxonomy, \%NCBImerged)) {
					$NCBItax{children}->{-3}->{children}->{$taxid} = {abundance => $taxon->{abundance}, rank => 'unknownTaxid', name => $taxon->{lineage}};
				} else {
					Utils::addNCBILineage(\%NCBItax, \@guessedLineage, $taxon->{abundance});
				}
			} else {
				print STDERR "warnings: could not find a NCBI taxid for '".$taxon->{name}."'. Abundance is added to class 'unassigned'.\n";
				if (not exists $NCBItax{children}->{-1}) {
					$NCBItax{children}->{-1} = {rank => 'unassigned', name => 'unassigned'};
				}
				$NCBItax{children}->{-1}->{children}->{--$idMismappings} = {abundance => $taxon->{abundance}, rank => 'noMappingFound', name => $taxon->{lineage}};
				push @missingTaxa, $taxon;
			}
		}
	}
}

markStrains(\%NCBItax);
Utils::pruneUnwantedRanks(\%NCBItax);

print Utils::generateOutput("metaphlan2", $sampleIDname, \%NCBItax, $taxonomydate);

sub checkTaxid {
	my ($taxid, $taxononmy, $merged) = @_;
	
	if (not exists $taxononmy->{$taxid}) {
		if (not exists $merged->{$taxid}) {
			print STDERR "a) no match for '$taxid'\n";
			$taxid = undef;
		} else {
			$taxid = $merged->{$taxid};
			if (not exists $taxononmy->{$taxid}) {
				print STDERR "b) no match for '$taxid'\n";
				$taxid = undef;
			}
		}
	}

	return $taxid;
}

sub markStrains {
	#the NCBI taxonomy does not seem to know the rank 'strain'. We want to call a rank below a 'species' and with rank name 'no rank' a 'strain', thus we traverse the tree and rename those ranks
	my ($tree, $belowSpecies) = @_;
	
	$belowSpecies = 0 if (not defined $belowSpecies);
	$belowSpecies = ($belowSpecies || ($tree->{rank} eq 'species'));
	foreach my $taxid (keys(%{$tree->{children}})) {
		if (exists $tree->{children}->{$taxid}->{children}) {
			markStrains($tree->{children}->{$taxid}, $belowSpecies);
		} else {
			if ($belowSpecies && ($tree->{children}->{$taxid}->{rank} eq 'no rank')) {
				$tree->{children}->{$taxid}->{rank} = 'strain';
			}
		}
	}
	
}

sub getOverallAbundance {
	my ($tree) = @_;
	
	my $abundance = 0;
	$abundance += $tree->{abundance} if (exists $tree->{abundance});
	foreach my $childName (keys(%{$tree->{children}})) {
		$abundance += getOverallAbundance($tree->{children}->{$childName});
	}
	
	return $abundance;
}

sub printLeafAbundances {
	my ($tree, $lineage) = @_;

	$lineage = "" if (not defined $lineage);
	my @leaves = ();
	foreach my $childName (keys(%{$tree->{children}})) {
		if ((exists $tree->{children}->{$childName}->{realAbd}) && ($tree->{children}->{$childName}->{realAbd} != 0)) {
			my $taxName = $childName;
			$taxName =~ s/^t\_\_//;
			push @leaves, {name => $taxName, abundance => $tree->{children}->{$childName}->{realAbd}, lineage => $lineage."|".$childName};
		}
		if (exists $tree->{children}->{$childName}->{children}) {
			push @leaves, @{printLeafAbundances($tree->{children}->{$childName}, $lineage."|".$childName)};
		}
	}
	
	return \@leaves;
}

sub assignRealAbundance {
	my ($tree) = @_;
	
	$tree->{realAbd} = 0;
	foreach my $childName (keys(%{$tree->{children}})) {
		if ($childName =~ m/\_unclassified$/) {
			$tree->{realAbd} += $tree->{children}->{$childName}->{abundance};
			$tree->{children}->{$childName}->{realAbd} = 0;
		} else {
			$tree->{children}->{$childName}->{realAbd} = $tree->{children}->{$childName}->{abundance};
		}
		assignRealAbundance($tree->{children}->{$childName}) if (exists $tree->{children}->{$childName}->{children});
		
	}
}

sub addLineage {
	my ($tree, $lineage, $abundance) = @_;
	
	if (scalar (@{$lineage}) == 1) {
		$tree->{children}->{$lineage->[0]} = {abundance => $abundance};
	} else {
		if (not exists $tree->{children}->{$lineage->[0]}) {
			$tree->{children}->{$lineage->[0]} = {children => undef};
		}
		my @subLineage = @{$lineage};
		shift @subLineage;
		addLineage($tree->{children}->{$lineage->[0]}, \@subLineage, $abundance);
	}
}

sub lineage_string2object {
	my ($lineagestring) = @_;
	chomp $lineagestring;
	my @lineage = ();
	foreach my $part (split(m/\|/, $lineagestring)) {
		my ($rank, $taxid, $name) = ($part =~ m/^(.+?)=(\d+):?(.*?)$/);
		my $hash = {rank => $rank, taxid => $taxid};
		$hash->{name} = $name if ((defined $name) && ($name ne ''));
		push @lineage, $hash;
	}
	return \@lineage;
}

sub getRank {
	my ($lineage, $rankname) = @_;
	
	foreach my $rank (@{$lineage}) {
		if (lc($rank->{rank}) eq lc($rankname)) {
			return $rank->{taxid};
		}
	}
	
	return undef;
}

#!/usr/bin/env perl

#use lib "../";
#use lib "/biobox/lib/";

use strict;
use warnings;
use Data::Dumper;
use XML::XPath;
use XML::XPath::XMLParser;
use Utils;

#files which holds taxonomy information and will be downloaded to the local machine
my @filesToDownload = (
	'ftp://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt',
	'ftp://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt',
	'ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz',
);
my $MAXIDSPERFILE = 1000; #NCBI batch entrez allows fetching of several IDs, but is limited to an unknown number. Thousand per run seems to be working.
my ($filename_species2genomes, $WORKDIR) = @ARGV; #the first argument for this script is the file from metaphlan2 which holds the species genome table, second is the path to a working directory
$WORKDIR = 'workdir/' if (not defined $WORKDIR); #where downloaded files shall be stored
die "usage: perl $0 <species2genomes.txt> <working directory>\n" if (@ARGV != 2);


# all required downloads are performed and necessary files will be extracted from archives
$WORKDIR = qx(readlink -m $WORKDIR); chomp $WORKDIR;
qx(mkdir -p $WORKDIR) if (not -d $WORKDIR);
my $downloadSubDir = $WORKDIR."/Downloads/";
qx(mkdir -p $downloadSubDir) if (not -d $downloadSubDir);
foreach my $file (@filesToDownload) {
	my @parts = split(m|/|, $file);
	my $filename = $parts[$#parts];
	if (not -e $downloadSubDir.$filename) {
		print STDERR "downloading '".$filename."': ...";
		qx(wget -q -O $downloadSubDir$filename $file);
		qx(date +"%Y.%m.%d" > $downloadSubDir/$filename.date);
		print STDERR " done.\n";
	}
	if (($filename eq 'taxdump.tar.gz') && (not -e $WORKDIR."/nodes.dmp")) {
		print STDERR "extracting 'nodes.dmp' from '".$filename."': ...";
		qx(cd $WORKDIR && tar xzvf $downloadSubDir/$filename nodes.dmp);
		print STDERR " done.\n";
	}
	if (($filename eq 'taxdump.tar.gz') && (not -e $WORKDIR."/names.dmp")) {
		print STDERR "extracting 'names.dmp' from '".$filename."': ...";
		qx(cd $WORKDIR && tar xzvf $downloadSubDir/$filename names.dmp);
		print STDERR " done.\n";
	}
	if (($filename eq 'taxdump.tar.gz') && (not -e $WORKDIR."/merged.dmp")) {
		print STDERR "extracting 'merged.dmp' from '".$filename."': ...";
		qx(cd $WORKDIR && tar xzvf $downloadSubDir/$filename merged.dmp);
		print STDERR " done.\n";
	}
}

##Phase 1 grep all accession number from the species2genomes file of metaphlan and try to obtain according taxids from various sources, i.e. ftp downloads from NCBI for current accessions, produce id lists for Batch Entrez manual download for outdated accessions plus successive parsing of the retrieved XML files, look up in a manual.txt file for the few remaining accessions which must be found somewhere in the internet
my %ids = %{findspeciesaccessions($filename_species2genomes, $WORKDIR, $downloadSubDir)};

## Phase 2: given the taxIDs we now need to get the "lineage" from the NCBI taxonomy
my %taxonomy = %{Utils::read_taxonomytree($WORKDIR.'/nodes.dmp')};
my %NCBImerged = %{Utils::read_taxonomyMerged($WORKDIR."/merged.dmp")};
my %genomes = ();
foreach my $type (sort keys(%ids)) {
	foreach my $id (keys(%{$ids{$type}})) {
		if (not defined $ids{$type}->{$id}) {
			#die "there are undefined IDs of type '$type' for ID '$id' after phase 1. You should consider deleting the xml files and download fresh ones via NCBI batch entrez!\n";
            print STDERR "Undefined IDs of type \"$type\" for ID $id after phase 1.\n";
            delete $ids{$type}->{$id};
            next;
		}
		my $taxid = $ids{$type}->{$id};
		if (not exists $taxonomy{$taxid}) {
			if (not exists $NCBImerged{$taxid}) {
				die "no match for '$taxid'";
			} else {
				$taxid = $NCBImerged{$taxid};
				if (not exists $taxonomy{$taxid}) {
					die "no match for '$taxid'";
				}
			}
		}
		my $lineage = Utils::getLineage($taxid, \%taxonomy);
		$genomes{$id} = $lineage;
	}
}

## Phase 3: now, every genomes has its full lineage. Parse species2genomes.txt again and find lineages also for the GreenGene like species names
my %species = ();
open (IN, $filename_species2genomes) || die;
	while (my $line = <IN>) {
		my ($species, $numGenomes, @genomes) = split(m/\t|\n/, $line);
		my %collectedTaxIDs = ();
		foreach my $genome (@genomes) {
            if ((not exists $genomes{$genome}) || (not defined $genomes{$genome})) {
                next;
            }
			my @lineage = @{$genomes{$genome}};
			my $speciesTaxid = undef;
			foreach my $rank (@lineage) {
				if ($rank->{rank} eq 'species') {
					$speciesTaxid = $rank->{taxid};
					last;
				}
			}
			$collectedTaxIDs{$speciesTaxid}++;
		}
        if (scalar(keys(%collectedTaxIDs)) == 0) {
            next;
        } elsif (scalar(keys(%collectedTaxIDs)) != 1) {
			#currently (13.11.2015) there are 34 "species" in the species2genomes file, which have genomes that acutally point to different species tax-ids. Thus, we here search for the longest common part in the according lineages.
			my @lineages = ();
			foreach my $taxid (keys(%collectedTaxIDs)) {
				push @lineages, Utils::getLineage($taxid, \%taxonomy);
			}
			$species{$species} = Utils::getCommonLineage(\@lineages);
			print STDERR "species '$species' is represented by $numGenomes genomes, which point to ".scalar(keys(%collectedTaxIDs))." DIFFERENT species tax-ids. Lowest common taxid is selected instead.\n";
		} else {
			my @taxids = keys(%collectedTaxIDs);
			$species{$species} = Utils::getLineage($taxids[0], \%taxonomy);
		}
	}
close (IN);

## last Phase: present collected results as a text file
my %taxonomyNames = %{Utils::read_taxonomyNames($WORKDIR.'/names.dmp')};
my $date = qx(cat $downloadSubDir/taxdump.tar.gz.date); chomp $date;
print "#NCBI taxonomy downloaded at '".$date."'\n";
foreach my $id (sort keys(%genomes)) {
	my @lineage = @{$genomes{$id}};
	Utils::addNamesToLineage(\@lineage, \%taxonomyNames);
	print $id."\t".$lineage[$#lineage]->{taxid}."\t".Utils::printLineage(\@lineage)."\n";
}
foreach my $id (sort keys(%species)) {
	my @lineage = @{$species{$id}};
	Utils::addNamesToLineage(\@lineage, \%taxonomyNames);
	print $id."\t".$lineage[$#lineage]->{taxid}."\t".Utils::printLineage(\@lineage)."\n";
}

sub findspeciesaccessions {
	my ($filename_species2genomes, $WORKDIR, $downloadSubDir) = @_;
	
	my %ids = ();
	open (IN, $filename_species2genomes) || die;
		while (my $line = <IN>) {
			my @parts = split(m/\t|\n/, $line);
			foreach my $id (splice(@parts, 2)) {
				if ($id =~ m/^GCF|A_/) {
					$ids{Assembly}->{$id} = undef;
				} elsif ($id =~ m/^PRJNA/) {
					$ids{bioproject}->{$id} = undef;
				} elsif ($id =~ m/XXX/) {
					$ids{exception}->{$id} = undef;
				} else {
					print Dumper $id;
					die;
				}
			}
		}
	close (IN);

	#~ # assembly_accession	bioproject	biosample	wgs_master	refseq_category	taxid	species_taxid	organism_name	infraspecific_name	isolate	version_status	assembly_level	release_type	genome_rep	seq_rel_date	asm_name	submitter	gbrs_paired_asm	paired_asm_comp	ftp_path
	#~ GCF_000001215.2	PRJNA164		AABU00000000.1	na	7227	7227	Drosophila melanogaster			replaced	Chromosome	Major	Full	2007/10/22	Release 5		GCA_000001215.2	different	na
	open (IN, $downloadSubDir."assembly_summary_refseq.txt") || die;
		while (my $line = <IN>) {
			my @parts = split(m/\n|\t/, $line);
			$parts[0] =~ s/\..*?$//;
			if ($#parts > 1) {
				if (exists $ids{Assembly}->{$parts[0]}) {
					$ids{Assembly}->{$parts[0]} = $parts[5];
				}
				if (exists $ids{bioproject}->{$parts[1]}) {
					$ids{bioproject}->{$parts[1]} = $parts[5];
				}
			}
		}
	close (IN);

	#~ # assembly_accession	bioproject	biosample	wgs_master	refseq_category	taxid	species_taxid	organism_name	infraspecific_name	isolate	version_status	assembly_level	release_type	genome_rep	seq_rel_date	asm_name	submitter	gbrs_paired_asm	paired_asm_comp	ftp_path
	#~ GCA_000001215.2	PRJNA13812		AABU00000000.1	na	7227	7227	Drosophila melanogaster			replaced	Chromosome	Major	Full	2007/10/22	Release 5		GCF_000001215.2	different	na
	open (IN, $downloadSubDir."assembly_summary_genbank.txt") || die;
		while (my $line = <IN>) {
			my @parts = split(m/\n|\t/, $line);
			$parts[0] =~ s/\..*?$//;
			if ($#parts > 1) {
				if (exists $ids{Assembly}->{$parts[0]}) {
					$ids{Assembly}->{$parts[0]} = $parts[5];
				}
				if (exists $ids{bioproject}->{$parts[1]}) {
					$ids{bioproject}->{$parts[1]} = $parts[5];
				}
			}
		}
	close (IN);

	open (IN, $WORKDIR."/manual.txt") || die "cannot open file '$WORKDIR/manual.txt': $!";
		while (my $line = <IN>) {
			my ($accession, $taxid) = split(m/\t|\n/, $line);
			foreach my $type (keys(%ids)) {
				if ((exists $ids{$type}->{$accession}) && (not defined $ids{$type}->{$accession})) {
					$ids{$type}->{$accession} = $taxid;
				}
			}
		}
	close (IN);
	
	foreach my $type (sort keys(%ids)) {
		my $batchFile = $WORKDIR."/".$type."_result.xml";
		if (-e $batchFile) {
			my $tmpXML = "$WORKDIR/tmp.xml";
			qx(echo "<root>" > $tmpXML && cat $batchFile >> $tmpXML && echo "</root>" >> $tmpXML);
			my $ncbiBatchResults = XML::XPath->new(filename => $tmpXML);
			if ($type eq 'bioproject') {
				foreach my $xml_document ($ncbiBatchResults->find('/root/DocumentSummary')->get_nodelist) {
					my $accession = undef;
					my $taxid = undef;
					foreach my $xml ($xml_document->find('Project/ProjectID/ArchiveID')->get_nodelist) {
						$accession = $xml->getAttribute('accession');
					}
					foreach my $xml ($xml_document->find('Project/ProjectType/ProjectTypeSubmission/Target/Organism')->get_nodelist) {
						$taxid = $xml->getAttribute('taxID');
					}
					if ((exists $ids{$type}->{$accession}) && (not defined $ids{$type}->{$accession})) {
						$ids{$type}->{$accession} = $taxid;
					}
				}
			} elsif ($type eq 'Assembly') {
				foreach my $xml_document ($ncbiBatchResults->find('/root/DocumentSummary')->get_nodelist) {
					my $accession = undef;
					my $taxid = undef;
                    my $accession_synomy = undef;
					foreach my $xml ($xml_document->find('AssemblyAccession')->get_nodelist) {
						($accession) = (XML::XPath::XMLParser::as_string($xml) =~ m/>(.+?)\.?\d*\</);
					}
                    foreach my $xml ($xml_document->find('Synonym/Genbank')->get_nodelist) {
                        ($accession_synomy) = (XML::XPath::XMLParser::as_string($xml) =~ m/>(.+?)\.?\d*\</);
                    }
					foreach my $xml ($xml_document->find('Taxid')->get_nodelist) {
						($taxid) = (XML::XPath::XMLParser::as_string($xml) =~ m/>(.+?)</);
					}
					if ((exists $ids{$type}->{$accession}) && (not defined $ids{$type}->{$accession})) {
						$ids{$type}->{$accession} = $taxid;
					} elsif ((exists $ids{$type}->{$accession_synomy}) && (not defined $ids{$type}->{$accession_synomy})) {
 						$ids{$type}->{$accession_synomy} = $taxid;
                   }
				}
			}
			unlink $tmpXML;
		} else {
			my %unknowns = 	%{checkForUnknownIDs(\%ids)};
			if ($unknowns{$type} > 0) {
				print STDERR $type."\n";
				my $unknown = 0;
				my $filecount = 1;
				open (OUT, "> ".$WORKDIR."/unknownids_".$type."_".($filecount++).".txt") || die;
				foreach my $id (keys(%{$ids{$type}})) {
					if (not defined $ids{$type}->{$id}) {
						$unknown++;
						print OUT $id."\n";
						if ($unknown > $MAXIDSPERFILE) {
							$unknown = 0;
							close OUT;
							open (OUT, "> ".$WORKDIR."/unknownids_".$type."_".($filecount++).".txt") || die;
						}
					}
				}
				close (OUT);
				print STDERR "  unknown: ".$unknown." / ".scalar(keys(%{$ids{$type}}))."\n";
				
				print STDERR "Not all taxa could be resolved to NCBI taxonomy IDs.
There are three ways to inform this program about taxon mappings:
1) by downloading current NCBI files via FTP (already done automatically)
2) by providing manual mappings via the file 'manual.txt'
3) by using the NCBI batch entrez system in an semi-automatic way
   http://www.ncbi.nlm.nih.gov/sites/batchentrez:
   Files holding the unknown IDs have been created. You must upload those to the
   Batch Entrez system, select all fetched entries and download them as XML 
   file. Merge XML files, if multiple downloads are necessary, due to a limit of
   fetchable IDs per run.
   Save this file in the '$WORKDIR' directory and name it 'Assembly_result.xml' or
   'bioproject_result.xml'. This will mostly cover outdated taxa.
Re-run this program after you added new information in one of the mentions ways.\n";
				exit 2;
			}
		}
	}

	return \%ids;
}

sub checkForUnknownIDs {
	my ($refHash_ids) = @_;
	
	my %unknowns = ();
	foreach my $type (sort keys(%{$refHash_ids})) {
		$unknowns{$type}=0;
		foreach my $id (keys(%{$refHash_ids->{$type}})) {
			if (not defined $refHash_ids->{$type}->{$id}) {
				$unknowns{$type}++;
				$unknowns{all}++;
			}
		}
	}
	
	return \%unknowns;
}

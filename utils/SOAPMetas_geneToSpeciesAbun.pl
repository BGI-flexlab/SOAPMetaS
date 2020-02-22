#! /usr/bin/perl

=head1 Program: get_taxonomic_profile.pl

=head1 Version: V1.0

=head1 Updated: Nov 18 2015

=head1 Description: This program use to get taxonomic profile from gene profile and taxonomic annotation

=head1 
            
    Usage: perl get_taxonomic_profile.pl [options]

    Options: 
    -p  <str>   corr.[first col: gene; second col:phylum name]
    -g  <str>   corr.[first col: gene; second col:genus name]
    -s  <str>   corr.[first col: gene; second col:species name]
    -f  <str>   gene profile file
    -o  <str>   output file prefix
    -t  <str>   sampleTag
    gene is the same as the fist col of gene profile
    Contact: xiehailiang@genomics.org.cn
   
=head1 
         
=cut

use strict;
use warnings;

use Getopt::Long;

#initialize some parameters
our ($corr_p, $corr_g, $corr_s, $profile_gene, $outdir);  # input
our (%profile_d);                                         # output
our (%anno, @sample, %profile);                           # anno sample profile
our $sampleTag;
our (@s, $i);                                             # else

GetOptions( 
    "p=s" => \$corr_p,
    "g=s" => \$corr_g,
    "s=s" => \$corr_s,
    "f=s" => \$profile_gene,
    "o=s" => \$outdir,
    "t=s" => \$sampleTag,
);
#get the introduction information
die `pod2text $0` if ((!$corr_p && !$corr_g && !$corr_s) || !$profile_gene);

#$outdir ||= ".";
#$outdir =~ s/\/$//;
$sampleTag ||= "0";

my $pwd = $ENV{'PWD'};

#$outdir = "$pwd/$outdir" if ($outdir !~ /^\//);

$corr_p = "$pwd/$corr_p" if ($corr_p && $corr_p !~ /^\//);
$corr_g = "$pwd/$corr_g" if ($corr_g && $corr_g !~ /^\//);
$corr_s = "$pwd/$corr_s" if ($corr_s && $corr_s !~ /^\//);

$profile_gene = "$pwd/$profile_gene" if ($profile_gene !~ /^\//);

#`mkdir -p $outdir` unless (-e $outdir);

#profile result file
$profile_d{p} = $outdir.'_phylum.profile';
$profile_d{g} = $outdir.'_genus.profile';
$profile_d{s} = $outdir.'_species.profile';

#anno
if ($corr_p) {
    open GS, "less $corr_p |" or die "can't read $corr_p:$!\n";
    while (<GS>) {
        chomp;
        @s = split /\t/;
        $anno{p}{$s[0]} = $s[1];

    }
    close GS;
}
if ($corr_g) {
    open GS, "less $corr_g |" or die "can't read $corr_g:$!\n";
    while (<GS>) {
        chomp;
        @s = split /\t/;
        $anno{g}{$s[0]} = $s[1];

    }
    close GS;
}
if ($corr_s) {
    open GS, "less $corr_s |" or die "can't read $corr_s:$!\n";
    while (<GS>) {
        chomp;
        @s = split /\t/;
        $anno{s}{$s[1]} = $s[3];

    }
    close GS;
}

#gene profile
open GP, $profile_gene or die "can't read $profile_gene:$!\n";
#$_ = <GP>;
#chomp;
#@sample = split(/\t/, $_);
@sample = ("sampleTag", "ID", "reads_count", "recalibrated_reads_count", "abundance", "rel_abundance");

while (<GP>) {
    chomp;
    @s = split /\t/;
    if (exists $anno{p}{$s[1]}) {
        for ($i = 2; $i <= $#s; ++$i) {  
            $profile{p}{$anno{p}{$s[1]}}{$i} += $s[$i];  
        }
    }
    if (exists $anno{g}{$s[1]}) {
        for ($i = 2; $i <= $#s; ++$i) {  
            $profile{g}{$anno{g}{$s[1]}}{$i} += $s[$i];  
        }
    }
    if (exists $anno{s}{$s[1]}) {
        for ($i = 2; $i <= $#s; ++$i) {  
            $profile{s}{$anno{s}{$s[1]}}{$i} += $s[$i];  
        }
    }
}
close GP;


#profile
if ($corr_p) {
    open PP, ">$profile_d{p}" or die "can't write $profile_d{p}:$!\n";
    #print PP join("\t", @sample), "\n";

    foreach my $anno (sort keys %{$profile{p}}) {
        my $anno_nospace = $anno;
        $anno_nospace =~ s/\s+/\_\_/g;
        print PP "$sampleTag\tp__$anno_nospace";
        for ($i = 2; $i <= $#sample; ++$i) {
            print PP "\t$profile{p}{$anno}{$i}";
        }
        print PP "\n";
    }
    close PP;
}
if ($corr_g) {
    open PP, ">$profile_d{g}" or die "can't write $profile_d{g}:$!\n";
    #print PP join("\t", @sample), "\n";

    foreach my $anno (sort keys %{$profile{g}}) {
        my $anno_nospace = $anno;
        $anno_nospace =~ s/\s+/\_\_/g;
        print PP "$sampleTag\tg__$anno_nospace";
        for ($i = 2; $i <= $#sample; ++$i) {
            print PP "\t$profile{g}{$anno}{$i}";
        }
        print PP "\n";
    }
    close PP;
}
if ($corr_s) {
    open PP, ">$profile_d{s}" or die "can't write $profile_d{s}:$!\n";
    #print PP join("\t", @sample), "\n";

    foreach my $anno (sort keys %{$profile{s}}) {
        my $anno_nospace = $anno;
        #$anno_nospace =~ s/\s+/\_\_/g;
        print PP "$sampleTag\t$anno_nospace";
        for ($i = 2; $i <= $#sample; ++$i) {
            print PP "\t$profile{s}{$anno}{$i}";
        }
        print PP "\n";
    }
    close PP;
}

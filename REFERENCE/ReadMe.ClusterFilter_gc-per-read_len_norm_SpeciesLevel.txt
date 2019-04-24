The enclosed Perl script is provided only for illustration of the methods presented in the publication. 

The script performs filtration of bacterial hits with genomic islands and provides GC- and genome-length recalibrated read counts for each reported bacterial hit.
The GC normalization is performed based on a regression model created as described in the manuscript.
The regression coefficients are hard coded in the script on line 340.

All currently available bacterial reference genome sequences should be downloaded from NCBI and put into one FASTA file indexed for alignment.
Two additional files (described as arguments -l and -s below) should be prepared to provide genome length and GC content of the sequences in this FASTA file.

The following command line should be used to run the script:

perl ClusterFilter_gc-per-read_len_norm_SpeciesLevel.pl -i <input SAM file> -r <Report file> -l <genome length and GC file> -s <strain level genome length file> -o <output file>


Description of the arguments:

-i SAM file is a standard alignment file containing metagenomic reads mapped to a collection of bacterial references (the FASTA file mentioned above).


-r Report file containing genome names for all mapped reads. It should have the following format:

<genome name>	<genome ID>	<raw read count>

The read count can be left blank. Genome name = Genus species.


-l Genome length and GC file should have the following format:

<genome name>	<genome length, bp>	<genome GC as a fraction, e.g., 0.65>

Note: If a genome has multiple chromosomes they should all be reported combined. The genome length should be the total of all chromosomes and the GC content should be set to the average GC of all chromosomes.


-s Strain level genome length. In this case individual chromosomes are not combined. Format:

<genome id>	<genome length, bp>



Output:

#<Genus species>	<Num_reads>	<GC-norm_reads>		<GC-norm_reads_per_Mb_of_ref>

Followed by genomic island filtration statistics for uncertain cases.


 

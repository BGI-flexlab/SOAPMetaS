

use Getopt::Std;
#use Try::Tiny;
use TryCatch;
use Statistics::R;

getopt ('irslo'); #i input sam, r report file, l genome length and GC, s strain length, o outfile.txt 


open (FD_IN, "$opt_i") || die "Error opening $opt_i";
open (FD_R, "$opt_r") || die "Error opening $opt_r";
open (FD_L, "$opt_l") || die "Error opening $opt_l";
open (FD_S, "$opt_s") || die "Error opening $opt_s";

open (FD_OUT, ">$opt_o") || die "Error opening $opt_o";

@w = ();

%hits = ();
%reads = ();
%genomes = ();
%str_genomes = ();
%Species = ();

while($line = <FD_R>) 
{
    chomp $line;
    @w = split(/\t/, $line);
    @w2 = split(/ /, $w[0]); #complete name 
    
    $hits{$w[1]}[0] = "$w2[0] $w2[1]"; # {id} = Genus species
    $hits{$w[1]}[1] = $w[0]; #hit name
    
    $genomes{"$w2[0] $w2[1]"}[0] = 1;
    $str_genomes{$w[1]}[0] = 1; #$w[1] is gi
}

while($line = <FD_L>) 
{
    chomp $line;
    @w = split(/\t/, $line);
    if(exists $genomes{$w[0]})    #$w[0] genus species
    {
        $genomes{$w[0]}[0] = $w[1]; #genome length
        $genomes{$w[0]}[1] = $w[2]; #genome gc
    }
}


while($line = <FD_S>) 
{
    chomp $line;
    @w = split(/\t/, $line);
    if(exists $str_genomes{$w[0]})    #$w[0] gi
    {
        $str_genomes{$w[0]} = $w[2]; #genome length
    }
}




while($line = <FD_IN>) 
{
    if($line =~ /^@/)
    {
        next;
    }
    else
    {
	@w = split(/\t/, $line);
	if($w[1] eq "4"){next;}
	#print "checking $w[2]\n";
        if(exists $hits{$w[2]})
        {
	    #print $line;
            $start = $w[3];
	    #print "$start\n";
            push @{$hits{$w[2]}[2]}, $start;
	    
	    #Get GC
	    $seq = $w[9];
	    $as = 0;
	    $ts = 0;
	    $cs = 0;
	    $gs = 0;
	    $aline = lc $seq;
	    $as += ($aline =~ tr/a//);

	    $tline = lc $seq;
	    $ts += ($tline =~ tr/t//);

	    $cline = lc $seq;
	    $cs += ($cline =~ tr/c//);

	    $gline = lc $seq;
	    $gs += ($gline =~ tr/g//);
	    
	    $gc = sprintf("%.0f",($gs + $cs)*100/($as + $ts + $gs + $cs));
	    #push @{$hits{$w[2]}[2]}, $gc;
	    #print "${$hits{$w[2]}[2]}[-1]\n";
	    
	    push @{$reads{$w[0]}[0]}, $w[2]; #gi <-- this is used to trace to wich strains the hits occured (discard the read if it does not pass the test)
	    push @{$reads{$w[0]}[1]}, $hits{$w[2]}[0]; #genus species
	    $reads{$w[0]}[2] = $gc; 
        }
        
        
    }
}



foreach $key (keys %hits)
{
    %deltas = ();
    @starts = sort {$a <=> $b} @{$hits{$key}[2]};
    $size = @starts;
    if($size < 5){delete $hits{$key}; next;}
    
    if($key eq "*"){delete $hits{$key}; next;}
    
        
    $mu_H0 = $str_genomes{$key}/$size;  #<--- Genomes have "genus species" as key, BUT hits have gi as key
    #$var_H0 = ($size**2 - 1)/12; # variance for discrete uniformm distribution
    
    #print "muH0: $mu_H0\n";
	
    #calculate sum of squares for var
    $SS = 0;
    $Sum = 0;
    #@delt = ();
    $Lt10Count = 0;
    open (FD_T, ">temp.txt") || die "Error opening temp.txt";
    for($i=1;$i<$size;$i++)
    {
        $delta = $starts[$i] - $starts[$i-1];# + 1;
	
	if($delta < 10){$Lt10Count++;}
	
	$deltas{$delta}++;
	$Sum += $delta;
	#push @delt, $delta;
	print FD_T "$delta\n";
    }
    close(FD_T);
    
    $mu = sprintf("%.0f",$Sum/$size);
    
    for($i=1;$i<$size;$i++)
    {
        $delta = $starts[$i] - $starts[$i-1];# + 1;
	$SS += ($delta - $mu)**2;
    }
    
   #Delta counts
   $DeltaCounts = "";
    foreach $key(sort {$a <=> $b} keys %deltas)
    {
	$DeltaCounts .= "$key  $deltas{$key}\n";	
    }
    
    if($size > 5000){$hits{$key}[3] = "na"; next;}
    
    my $R = Statistics::R->new();

    try{$R->run(q`rm(list=ls(all=TRUE))`);}
    catch($e){print "During initialization, the following error was handled:\n $e"; next;};

    #$R->set( 'gr', \@delt ); <------ TOO BUGGY AND SLOW
    $R->run(q`gr = read.table("temp.txt", sep = "\t", header=FALSE,  check.names=FALSE, stringsAsFactors=FALSE)`);
    $R->set('H0_mu', $mu_H0);
    
    try{$R->run(q`t = t.test(gr,mu=H0_mu,na.action=na.omit)`);}
    catch($e){print "The following error was handled:\n $e"; next;};
    #$R->run(q`t = t.test(gr,mu=H0_mu)`);
    
    $pval = $R->get('t$p.value');
    $stat = $R->get('t$statistic');
   

    #$R->stopR();
    
    
    
    $var = $SS/($size - 1);

 
    $tstat = "Num reads: $size  mu_H0: $mu_H0  Delta_mu: $mu  Delta_var: $var  T-test statistic: $stat   T-test p-vlaue: $pval\n";
    
    print "$key  $tstat";
    
    #if($Num_10k_windows < 5){delete $hits{$key}; next;}
    #Filter hits
    
    if($pval < 0.01){delete $hits{$key}; next;}
    
    
        ##### Genome size estimate###############################
    
    $count_Dist = 0;
    $Sum_Dist = 0;
    for($j=1;$j<$size;$j++)
    {
	for($k=1;$k<$size;$k++)
	{
	    if($k > $j) # upper triangle of the matrix (n choose 2 combinations)
	    {
		if($starts[$k] >= $starts[$j])
		{
		    $st = $starts[$j];
		    $end = $starts[$k];
		}
		else
		{
		    $st = $starts[$k];
		    $end = $starts[$j];
		}
		$dist = $end - $st + 1;
		
		if($dist > $str_genomes{$key}/2)
		{
		    $dist = $str_genomes{$key} - $end + $st + 1;
		}
	    }
	    #print "st: $st   end: $end   dist: $dist\n";
	    $Sum_Dist += $dist;
	    $count_Dist++;
	}
    }
    
    $MeanDist = sprintf("%.2f", $Sum_Dist/$count_Dist);
    
    $Glb = sprintf("%.0f", ($size/($size + 2*($size**0.5)))*4*$MeanDist);
    $Gub = sprintf("%.0f", ($size/($size - 2*($size**0.5)))*4*$MeanDist);
    
    print "Mean distance: $MeanDist\n";
    
    if($Glb < $str_genomes{$key} && $str_genomes{$key} < $Gub)
    {
	print "Genome size is within estimate.  Genome size: $str_genomes{$key}  Est.G lower CB: $Glb  upper CB: $Gub\n";
    }
    else
    {
	print "Genome size is NOT within estimate.  Genome size: $str_genomes{$key}  Est.G lower CB: $Glb  upper CB: $Gub\n";
    }
    
    ########################################################
    
    
    
    if($Gub < 0.1*$str_genomes{$key}){delete $hits{$key}; next;}
    
    if($size <= 200 && $Lt10Count > 0.75*$size){delete $hits{$key}; next;} #islands
    
    if($Gub >= $str_genomes{$key}*0.1 && $Gub < 0.5*$str_genomes{$key})
    {
	$hits{$key}[3] = "The upper bound of the genome size estimate is 10 to 50% of the actual size.\nGenome size: $str_genomes{$key}  Est.G lower CB: $Glb  upper CB: $Gub\n"; 
	$hits{$key}[3] .= $tstat;
	$hits{$key}[3] .= "Read_Deltas\tCount\n";
	$hits{$key}[3] .= $DeltaCounts; 
    }
    else
    {
	$hits{$key}[3] = "na";
    }

}



foreach $key (keys %reads)
{
    #See if read mapped to a single species and its strain(s) was not deleted during window filtration above
    #Single species?
    $cur_sp = @{$reads{$key}[1]}[0];
    foreach $sp (@{$reads{$key}[1]})
    {
        if($sp ne $cur_sp)
	{
	    #print "read $key mapped to $cur_sp and $sp and will be discarded\n";
	    goto skip;
	}
    }
    
    #at least one strain was not deleted?
    $good = 0;
    $questionable = 0;
    foreach $str (@{$reads{$key}[0]})
    {
	#print "$key:  $str\n";
        if(exists $hits{$str})
	{
	    $good = 1;
	    if($hits{$str}[3] ne "na"){$questionable = 1;}
	    #print "$str\n";
	}
    }
    
    if($good == 0)
    {
	#print "read $key has no unfiltered hits and will be discarded\n";
	goto skip;
    }
    
    push @{$Species{$cur_sp}[0]}, $reads{$key}[2]; #add read gc
    
    #print "$cur_sp    !$Species{$cur_sp}[1]!\n";
    
    if($Species{$cur_sp}[1] eq "" && $questionable == 0){$Species{$cur_sp}[1] = "NQ";}
    
    if($Species{$cur_sp}[1] eq "Q" && $questionable == 0){$Species{$cur_sp}[1] = "NQ";}
    
    if($Species{$cur_sp}[1] eq "" && $questionable == 1){$Species{$cur_sp}[1] = "Q";}
    
    # if already NQ leave as non-questionable
    
skip:
}



print FD_OUT "#Species\tNum_reads\tGC-norm_reads\tGC-norm_reads_per_Mb_of_ref\n";

foreach $key (keys %Species)
{
    
    $gc_norm_reads = 0;
 
    $Genome_gc = $genomes{$key}[1];
    
    if($Genome_gc == 0){next;}
    
    $rcount = @{$Species{$key}[0]};
    if($rcount < 5){delete $Species{$key}; next;}
       
    foreach $GC (@{$Species{$key}[0]}) 
    {
	$k = 0.812093*exp(-0.5*(($GC-49.34331)/8.886807)**2) + 6.829778 + 0.2642576*$GC - 0.005291173*$GC**2 + 0.00003188492*$GC**3 - 2.502158*log($Genome_gc);
	
	#print "$Genome_gc  $GC   $k\n";
	
	if($k < 0.1){$k = 0.1;}
	
	$gc_norm_reads += 1/$k;
    }
	
    $gc_norm_reads = sprintf("%.0f",$gc_norm_reads);
    
    $Species{$key}[2] = $gc_norm_reads;
    
    #get GC-normalized reads per MB of reference
    $G_len = $genomes{$key}[0];
    $gc_norm_reads_per_MB_ref = sprintf("%.0f", $gc_norm_reads * 1000000 / $G_len);
    
    $Species{$key}[3] = $gc_norm_reads_per_MB_ref;
    
    #print FD_OUT "$hits{$key}[0]\t$key\t$hits{$key}[1]\t$Num_10k_windows\t$gc_norm_reads\t$gc_norm_reads_per_MB_ref\n";
    
}

foreach $key (sort { $Species{$b}[3] <=> $Species{$a}[3] } keys %Species)
{
    if($Species{$key}[1] eq "NQ")
    {
	$NumReads = @{$Species{$key}[0]};
	print FD_OUT "$key\t$NumReads\t$Species{$key}[2]\t$Species{$key}[3]\n";
	
    }    
}





print FD_OUT "\n\n\nUncertain Species (including each hit):\n\n";
foreach $key (sort { $Species{$b}[3] <=> $Species{$a}[3] } keys %Species)
{
    if($Species{$key}[1] eq "Q")
    {
	$NumReads = @{$Species{$key}[0]};
	print FD_OUT "$key\t$NumReads\t$Species{$key}[2]\t$Species{$key}[3]\n";
    
    
	print FD_OUT "\nHits for this species:\n";
	foreach $hit(keys %hits)
	{
	    if($hits{$hit}[0] eq $key)
	    {
	        print FD_OUT "$hits{$hit}[1]\n$hits{$hit}[3]\n\n";
	    }
	}
    }
}



close(FD_IN);
close(FD_R);
close(FD_L);
close(FD_OUT);

exec("rm temp.txt");

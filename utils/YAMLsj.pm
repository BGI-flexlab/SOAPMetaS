#!/usr/bin/env perl

use strict;
use warnings;

package YAMLsj;

use Data::Dumper;

my $verbose = 0;
#~ my ($inputfile) = @ARGV;
#~ print Dumper parseYAML($inputfile);

sub parseYAML {
	my ($input) = @_;
	
	open (IN, $input) || die "cannot read yaml file '$input': $!";
		#split into documents
		my @documents = ();
		my @doc = ();
		while (my $line = <IN>) {
			chomp $line;
			
			$line = removeComment($line);
			
			if (($line =~ m/^---/) || ($line =~ m/^\.\.\./)) {
				if (@doc > 0) {
					my @h = @doc;
					push @documents, \@h;
					@doc = ();
				}
			} else {
				push @doc, $line;
			}
		}
		if (@doc > 0) {
			my @h = @doc;
			push @documents, \@h;
			@doc = ();
		}		
	close (IN);
	
	foreach my $document (@documents) {
		print STDERR "=== new document ===\n" if ($verbose);
		$document = (parseDocument($document))->[0];
	}
	
	return \@documents;
}

sub removeComment {
	my ($line) = @_;
	
	my %quotes = ();
	my $resultLine = "";
	for (my $i = 0; $i < length($line); $i++) {
		my $char = substr($line, $i, 1);
		if ($char eq '"' || $char eq "'" || $char eq '`') {
			if (not exists $quotes{$char}) {
				$quotes{$char} = 'open';
			} else {
				delete $quotes{$char};
			}
		} elsif ($char eq '#') {
			if (scalar(keys(%quotes)) == 0) {
				return $resultLine;
			}
		}
		$resultLine .= $char;
	}
	
	return $resultLine;
}

sub parseDocument {
	my ($lines) = @_;

	my @stack = ();
	my $indent = -1;
	foreach my $line (@{$lines}) {
		next if ($line =~ m/^\s*$/);
		my ($line_indent, $rest) = ($line =~ m/^([\s|\-]*)(.+?)$/);
		my ($key, $value) = ($rest =~ m/^(\S+)\s*:\s*(.*?)$/);
		
		if (length($line_indent) > $indent) {
			print STDERR "push: $line\n" if ($verbose);
			if (defined $key) {
				push @stack, {
					$key => {'#value' => $value}, 
					'#indent' => length($line_indent),
					'#lastKey' => $key,
				};
			} else {
				$stack[$#stack]->{$stack[$#stack]->{'#lastKey'}}->{'#tag'} = $stack[$#stack]->{$stack[$#stack]->{'#lastKey'}}->{'#value'} if ($stack[$#stack]->{$stack[$#stack]->{'#lastKey'}}->{'#value'} ne '');
				$stack[$#stack]->{$stack[$#stack]->{'#lastKey'}}->{'#value'} = $rest;
			}
		} elsif ((length($line_indent) < $indent) || ($line_indent =~ m/\-/)) {
			print STDERR "pull: $line\n" if ($verbose);

			for (my $i = @stack-1; $i >= 1; $i--) {
				if ($stack[$i]->{'#indent'} > length($line_indent)) {
					$stack[$i-1]->{$stack[$i-1]->{'#lastKey'}}->{'#children'} = [] if (not exists $stack[$i-1]->{$stack[$i-1]->{'#lastKey'}}->{'#children'});
					my $index = @{$stack[$i-1]->{$stack[$i-1]->{'#lastKey'}}->{'#children'}};
					$index-- if (($index > 0) && (not exists $stack[$i-1]->{$stack[$i-1]->{'#lastKey'}}->{'#children'}->[$index-1]->{$stack[$i]->{'#lastKey'}}));
					foreach my $key (keys(%{$stack[$i]})) {
						next if ($key =~ m/^#/);
						$stack[$i-1]->{$stack[$i-1]->{'#lastKey'}}->{'#children'}->[$index]->{$key} = $stack[$i]->{$key};
					}
					pop @stack;
				} elsif ($stack[$i]->{'#indent'} == length($line_indent)) {
					$stack[$#stack-1]->{$stack[$#stack-1]->{'#lastKey'}}->{'#children'} = [] if (not exists $stack[$#stack-1]->{$stack[$#stack-1]->{'#lastKey'}}->{'#children'});
					my $index = @{$stack[$#stack-1]->{$stack[$#stack-1]->{'#lastKey'}}->{'#children'}};
					$index-- if (($index > 0) && (not exists $stack[$#stack-1]->{$stack[$#stack-1]->{'#lastKey'}}->{'#children'}->[$index-1]->{$key}));
					foreach my $key (keys(%{$stack[$#stack]})) {
						next if ($key =~ m/^#/);
						$stack[$#stack-1]->{$stack[$#stack-1]->{'#lastKey'}}->{'#children'}->[$index]->{$key} = $stack[$#stack]->{$key};
					}
					pop @stack;
					push @stack, {
						$key => {'#value' => $value}, 
						'#indent' => length($line_indent),
						'#lastKey' => $key,
					};
				}
			}

			if (defined $key) {
				$stack[$#stack]->{$key} = {'#value' => $value};
				$stack[$#stack]->{'#lastKey'} = $key;
			}
		} elsif (length($line_indent) == $indent) {
			print STDERR "equa: $line\n" if ($verbose);
			if (defined $key) {
				$stack[$#stack]->{$key} = {'#value' => $value};
				$stack[$#stack]->{'#indent'} = length($line_indent);
				$stack[$#stack]->{'#lastKey'} = $key;
			} else {
				$stack[$#stack]->{$stack[$#stack]->{'#lastKey'}}->{'#value'} .= "\n".$rest;
			}

		} else {
			die "cannot occure\n";
		}
		$indent = length($line_indent);
	}
	
	if (@stack > 1) {
		for (my $i = @stack-1; $i >= 1; $i--) {
			$stack[$i-1]->{$stack[$i-1]->{'#lastKey'}}->{'#children'} = [] if (not exists $stack[$i-1]->{$stack[$i-1]->{'#lastKey'}}->{'#children'});
			my $index = @{$stack[$i-1]->{$stack[$i-1]->{'#lastKey'}}->{'#children'}};
			$index-- if (($index > 0) && (not exists $stack[$i-1]->{$stack[$i-1]->{'#lastKey'}}->{'#children'}->[$index-1]->{$stack[$i]->{'#lastKey'}}));
			foreach my $key (keys(%{$stack[$i]})) {
				next if ($key =~ m/^#/);
				$stack[$i-1]->{$stack[$i-1]->{'#lastKey'}}->{'#children'}->[$index]->{$key} = $stack[$i]->{$key};
			}
			pop @stack;
		}
	}
	
	delete $stack[0]->{'#indent'};
	delete $stack[0]->{'#lastKey'};
	cleanStack(\@stack);
	
	return \@stack;
}

sub cleanStack {
	my ($stack) = @_;
	
	foreach my $hash (@{$stack}) {
		foreach my $key (keys(%{$hash})) {
			if (exists $hash->{$key}->{'#children'}) {
				if (exists $hash->{$key}->{'#value'} && $hash->{$key}->{'#value'} ne '') {
					$hash->{$key}->{'#tag'} = $hash->{$key}->{'#value'};
				}
				delete $hash->{$key}->{'#value'};
				cleanStack($hash->{$key}->{'#children'});
			}
		}
	}
}

1;

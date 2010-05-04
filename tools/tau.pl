#!/usr/bin/perl

=head1 NAME

new_tau.pl

=head1 SYNOPSIS

new_tau.pl [-v] ranking ...

=head1 DESCRIPTION

Compute Kendall's tau correlations between all rankings given on the
command line.

This differs from Ellen's F<new_kendall.pl> script in that it uses the
Tau-C variant which explicitly handles tied scores correctly.  Ellen's
script assumes a ranking without ties, and the inputs may not be
stably sorted, so ties may not have been broken consistently.  This is
probably not a concern when you have 100,000 permutations, but for
small sets it's better to be more careful.

Implementation is based on "Kendall's tau : Procedure" found at
L<http://franz.stat.wisc.edu/~rossini/courses/intro-nonpar/text/Kendall_s_tex2html_image_mark_tex2html_wrap_inline4141_Procedure.html>
which is part of
"Nonparametric Statistical Methods: Supplemental Text", by Dr. A. J. Rossini

=head1 FILES

Ranking files are assumed to contain lines of the form

=over

run score

=back

If a line is read in the second or later rankings, which contains a
runtag not in the first ranking, it is discarded.  If you want to do
correlations between rankings of different subsets of runs, use
F<loo_tau.pl>.

=head1 HISTORY

Adapted from my F<tau.pl> and Ellen's F<new_kendall.pl>.

=cut

use strict;
use Getopt::Long;
use Pod::Usage;

my $verbose = 0;
GetOptions("verbose!" => \$verbose)
    or die pod2usage(2);

die pod2usage(2) unless scalar(@ARGV) > 1;

my @rank;
my @rankname;
my %runs;

#
# Read in rankings, and collect a table of scores for each.
# Also, count number of tied scores in each ranking.
#
my $i = 0;
my @genrun = ('A' .. 'Z', 'a' .. 'z', map(chr, (33 .. 64)) );
my $nruns;

while (my $ranking = shift) {
    $rankname[$i] = $ranking;
    my $runcount;
    
    open(IN, $ranking) or die "Can't read $ranking: $1\n";
    while (<IN>) {
	chomp;
	my ($run, $score) = split;
	next if ($run =~ /^$/);
	    
	if ($i == 0) {
	    $runs{$run} = 1;
	    $nruns++;
	} else {
	    next unless exists $runs{$run};
	    $runcount++;
	}

	if ($score =~ /^$/) {
	    # only one thing on the line, assume it's a score and
	    # make up a run name
	    $score = $run;
	    $run = $genrun[$.];
	}

	$rank[$i]{$run} = $score;
    }
    close(IN);
    
    if ($i > 0 and $runcount != $nruns) {
	die "Number of runs in $ranking ($runcount) does not equal $nruns\n";
    }
    
    print "$i: $ranking\n";

    $i++;
}

#
# Compute the tau correlation between each ranking.
#
my $numrank = $i;
my ($sum, $min, $max, $mean) = (0, 2, 0, 0);
my ($max_pair, $min_pair);

for $i (0 .. $numrank - 2) {
    for my $j ($i + 1 .. $numrank - 1) {
	my $t = tau($i, $j);
	print "tau between rankings $i and $j is $t\n";
	$sum += $t;

	if (abs($t) > abs($max)) {
	    $max = $t;
	    $max_pair = "$i.$j";
	}
        if (abs($t) < abs($min)) {
            $min = $t;
            $min_pair  = "$i.$j";
        }
    }
}
$mean = $sum / (($numrank * ($numrank - 1)) / 2);
print "mean tau is $mean (min, $min for $min_pair; max, $max for $max_pair)\n";

sub tau {
    my ($i, $j) = @_;
    my ($con, $dis, $tie_i, $tie_j, $n, @pairs);
    my (@runs, %runstat);

    # Build a map between these two rankings
    # This map is a list of score pairs in order of the first ranking

    for my $run (sort { $rank[$i]{$b} <=> $rank[$i]{$a} }
		 keys %{ $rank[$i] }) {
	push @pairs, [ $rank[$i]{$run}, $rank[$j]{$run} ];
	# print "$run $rank[$i]{$run} $rank[$j]{$run}\n" if $verbose;
	push @runs, $run;
    }

    while (my $pair = shift @pairs) {
	$n++;
	for my $otherpair (@pairs) {
	    if (($pair->[0] == $otherpair->[0]) and
	        ($pair->[1] != $otherpair->[1])) {
		$tie_i++;

	    } elsif (($pair->[1] == $otherpair->[1]) and
		    ($pair->[0] != $otherpair->[0])) {
		$tie_j++;

	    } elsif ($pair->[1] < $otherpair->[1]) {
		$dis++;
		$runstat{$runs[$n - 1]}++;

	    } elsif ($pair->[1] > $otherpair->[1]) {
		$con++;
	    }
	}
	print "$pair->[0]\t$pair->[1]\t$con\t$dis\t$tie_i\t$tie_j\n" if $verbose;
    }

    my $sum = $con + $dis;
    my $t = ($con - $dis) / sqrt(($sum + $tie_i) * ($sum + $tie_j));
#    my $t = 2 * ($con - $dis) / ($n * ($n - 1));

    if ($verbose) {
	for my $run (sort { $runstat{$a} <=> $runstat{$b} } keys %runstat) {
	    print "$run $runstat{$run}\n";
	}
    }


    return $t;
}

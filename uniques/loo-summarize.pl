#!/usr/bin/perl

=head1 NAME

loo-summarize.pl - summarize the output of loo-eval.pl

=head1 SYNOPSIS

loo-summarize.pl

=head1 DESCRIPTION

loo-eval.pl produces a set of eval.RUNTAG files.  This script compares
the evaluation output files with the official outputs and reports
differences for different measures.

=cut

use strict;

my $root = "/trec/trec14/terabyte";
my $runs_table = "$root/../reports/runs_table";
my $pool_runs_desc = "$root/results/mastermerge.adhoc/merge.top-2.100";

my %runs;
my %groups;
my %pool_runs;
my @measures = qw/map bpref/;

open(RUNS, $runs_table) or die "Can't read $runs_table: $!\n";
while (<RUNS>) {
    chomp;
    my ($run, $pid, undef, $track, undef, undef, $task) = split /:/;
    next unless $track eq "terabyte";
    next unless $task eq "adhoc";

    $runs{$run} = 1;
    $groups{$pid}{$run} = 1;
}
close(RUNS);

open(POOL_RUNS, $pool_runs_desc) or die "Can't read $pool_runs_desc: $!\n";
while (<POOL_RUNS>) {
    next if /^#/;  
    next if /^$/;
    next unless /sortmerge/;
    chomp;

    my ($prog, $track, $depth, $run, $run2) = split;
    $pool_runs{$run} = $depth;
    if ($prog =~ /sortmerge_first/) {
	$pool_runs{$run2} = $depth;
    }
}
close(POOL_RUNS);


my %eval;
my %loo;

printf "%13s", " ";
for my $meas (@measures) {
    printf " %20s%12s", $meas, " ";
}
printf "\n%13s", "Run ";
for my $meas (@measures) {
    printf " %7s %7s %7s %7s", "Orig", "LOO", "Diff", "%Diff";
}
print "\n";

for my $grp (sort keys %groups) {
    for my $run (keys %{ $groups{$grp} }) {
	next unless -e "eval.$run";
	
	open(EVAL, "$root/results/$run/mail.file") 
	    or die "Can't read official evaluation for $run: $!\n";
	while (<EVAL>) {
	    chomp;
	    my ($meas, $topic, $val) = split;
	    $eval{$topic}{$meas} = $val;
	}
	close(EVAL);

	open(LOO, "eval.$run") or die "Can't find LOO eval for $run: $!\n";
	while (<LOO>) {
	    chomp;
	    my ($meas, $topic, $val) = split;
	    $loo{$topic}{$meas} = $val;
	}
	close(LOO);

	if (exists $pool_runs{$run}) {
	    printf "%13s", "$run*";
	} else {
	    printf "%12s ", "$run";
	}       
	
	
	for my $meas (@measures) {
	    my $diff = $loo{all}{$meas} - $eval{all}{$meas};
	    my $diffpct = $diff / $eval{all}{$meas};
	    printf " %7.4f %7.4f %7.4f %7.4f",
		$eval{all}{$meas}, $loo{all}{$meas}, $diff, $diffpct;
	}
	print "\n";
    }
}
	

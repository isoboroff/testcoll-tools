#!/usr/bin/perl

=head1 NAME

annotate-qrels.pl - Count (unique) relevant documents contributed to
pools by each group

=head1 SYNOPSIS

overlap-count.pl

=head1 DESCRIPTION

Re-construct the pool, and output an annotated qrels which has the
number of groups finding each document, and the names of those groups,
appended to each line.

This annotated qrels is used by the LOO scripts.

Note that we try to be careful to pool just like sortmerge{,_first}.pl
does.

=cut

use strict;

my $root = "/trec/trec14/terabyte";
my $pool_runs_desc = "$root/results/mastermerge.adhoc/merge.top-2.100";
my $full_qrels = "$root/eval/qrels.adhoc";

my $runs_table = "$root/../reports/runs_table";

my @topics = (751 .. 800);

my %pool_runs;
my @groups;
my %grp;
my %qrel;

my $verbose = 1;

#
# Read the runs table and collect runs and groups
#
open (RUNS_TABLE, "$runs_table") or die "Can't read $runs_table: $!\n";
while (<RUNS_TABLE>) {
    chomp;
    my ($run, $pid, undef, $track, undef, undef, $task) = split /:/;
    next unless $track eq "terabyte";
    next unless $task eq "adhoc" or $task eq "efficiency";

    push @groups, $pid unless exists $grp{$pid};
    $grp{$pid}{$run} = 1;
    $grp{$run} = $pid;
}
close(RUNS_TABLE);

@groups = sort @groups;

#
# Read pool_runs to get pool construction details
#
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

#
# Build the pool in memory, keeping track of which groups submitted
# each document.

my %pool;
my %sims;
for my $topic (@topics) {
    for my $run (sort keys %pool_runs) {
	my $run_grp = $grp{$run};
	%sims = ();

	# Each tXXX file is already sorted by score
	open(TFILE, "$root/results/$run/t$topic") 
	    or die "Can't read $root/results/$run/t$topic: $!\n";
	while (<TFILE>) {
	    my ($top, undef, $docid, $rank, $sim, $tag) = split;
	    $sims{$docid} = $sim;
	}
	
	my $poolrank = 0;
	for my $docid (sort sort_by_sim keys %sims) {
	    $pool{$topic}{$docid}{$run_grp}++;
	    $poolrank++;
	    last if $poolrank >= $pool_runs{$run};
	}
    }
}
		
	    
#
# Read qrels
#
open(QRELS, $full_qrels) or die "Can't read $full_qrels: $!\n";
while (<QRELS>) {
    chomp;
    my ($topic, undef, $docid, $rel) = split;
    my $contributors = $pool{$topic}{$docid};
    my $cont_count = scalar(keys %$contributors);

    print "$topic 0 $docid $rel $cont_count ", 
	join(" ", sort keys %{ $pool{$topic}{$docid} }), "\n";
}
close(QRELS);

sub sort_by_sim {
    $sims{$b} <=> $sims{$a}
	or
    $b cmp $a;
}


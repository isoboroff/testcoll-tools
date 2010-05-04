#!/usr/bin/perl

=head1 NAME

gloo-eval.pl - Evaluate runs without their GROUP'S unique qrels contributions

=head1 SYNOPSIS

gloo-eval.pl

=head1 DESCRIPTION

Evaluate each run using a qrels that is missing the unique documents
contributed by that run's GROUP.  This measures the impact of that
group on the qrels.

=head1 FILES

This script creates a set of qrels and evaluation output for each group
included in the pools.  You want to make a work directory to run this
script.

=cut

use strict;

my $root = "/trec/trec14/terabyte";
my $ann_qrels = "$root/eval/annotated-qrels";

my $trec_eval = "/usr/local/bin/trec_eval -M1000 -q";

my $runs_table = "$root/../reports/runs_table";

my @topics = (751 .. 800);

my %pool_runs;
my %grp;
my %qrel;

#
# Read the runs table and collect runs and groups
#
open (RUNS_TABLE, "$runs_table") or die "Can't read $runs_table: $!\n";
while (<RUNS_TABLE>) {
    chomp;
    my ($run, $pid, undef, $track, undef, undef, $task) = split /:/;
    next unless $track eq "terabyte";
    next unless $task eq "adhoc" or $task eq "efficiency";

    print "$pid $run\n";
    $grp{$pid}{$run} = $task;
}
close(RUNS_TABLE);

for my $leaveout (keys %grp) {
    print "$leaveout...\n";
    open(QRELS, $ann_qrels) or die "Can't read $ann_qrels: $!\n";

    my $qout = "qrels.without-$leaveout";
    open(OUT, ">$qout") or die "Can't write $qout: $!\n";

    print "   building qrels\n";
    while (<QRELS>) {
	my ($topic, undef, $docid, $rel, $num_grp, $first_grp) = split;
	next if ($num_grp == 1 and $first_grp eq $leaveout);
	print OUT "$topic 0 $docid $rel\n";
    }
    close QRELS;
    close OUT;

    for my $run (keys %{ $grp{$leaveout} }) {
	print "$run...\n";

	print "   sorting runfile,\n";
	my $runtmp = `mktemp -q /tmp/eval-tb.XXXXXX`;
	chomp $runtmp;
	die "Couldn't create temporary run file $runtmp\n" if ($? >> 8) != 0;

	qx(gzip -dc $root/results/$run/input.gz | sort -s -k 1,1n > $runtmp);
	die "Couldn't resort run file $run/input.gz\n" if ($? >> 8) != 0;

	print "   evaluating $trec_eval $qout $runtmp > eval.$run\n";
	qx($trec_eval $qout $runtmp > eval.$run);
	die "Can't run trec_eval on $run\n" if ($? >> 8) != 0;

	qx(rm $runtmp);
	warn "Couldn't remove $runtmp\n" if ($? >> 8) != 0;
    }
    print "\n";
}



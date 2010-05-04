#!/usr/bin/perl

# Evaluate the sensitivity of a task evaluated with trec_eval.

# Given the set of runs, first read in the per-topic score for
# each question and each run.  Then randomly generate two sets of
# questions of size SIZE for SIZE 5, 10, ...T.  Find the score for all
# pairs of run for each question set and count the number of times the
# two question sets rank the system pair in different orders.

# Argument is a file containing a list of runs

use strict;
use Getopt::Long;

my $usage = "bootstrap-stab.pl [ -r root ] [ -e eval_file ] <runlist>\n";

my $rootdir = "/trec/trec13/terabyte";
my $eval_file = "mail.file";
my $all_runs = '';  # default, just use uncommented runs

GetOptions("rootdir=s" => \$rootdir,
	   "evalfile=s" => \$eval_file,
	   "all!" => \$all_runs,
	  ) or die $usage;

print STDERR "$all_runs is all_runs\n";

my $resultsdir = "$rootdir/results";

my @topics;
my $num_bins = 21;

my @runs = ();
my $num_runs;
my $num_qs;
my $max_eval_qs;
my %scores;
my %ave;

my @measures = qw(map P10 bpref R-prec);

my $runlist = shift or die $usage;

if ( (! -e $runlist) || (! open RUNS, "<$runlist") ) {
    die "Can't find/open runs list file `$runlist': $!\n";
}
while (my $line = <RUNS>) {
    chomp $line;
    next if ($line =~ /^\s*$/);
    if ($all_runs) {
	$line =~ s/^# *//;
    }
    next if ($line =~ /^#/);

    my ($tag) = split " ", $line;
    push @runs, $tag;
}

close RUNS || die "Can't close runs_list: $!\n";

# get per-topic scores for all runs

my %topics;
for my $tag (@runs) {
    my $scorefile = "$rootdir/results/$tag/$eval_file";
    if ( (! -e $scorefile) || (! open SCORES, "<$scorefile") ) {
        die "Can't find/open scorefile for run $tag in $scorefile\n";
    }

    print STDERR "Reading scores for $tag\n";
    while (my $sline = <SCORES>) {
        chomp $sline;
	my ($meas, $topic, $val) = split " ", $sline;

	if ($topic ne "all") {
	    $topics{$topic} = 1;
	}

	$scores{$tag}{$meas}[$topic] = $val;
    }
    close SCORES || die "Can't close scorefile for run $tag: $!\n";
}

@topics = keys %topics;
$num_runs = scalar(@runs);
$num_qs = scalar(@topics);
$max_eval_qs = $num_qs;

my @qset_sizes = ();
for (my $qset_size = 5; $qset_size <= $max_eval_qs; $qset_size += 5) {
    push @qset_sizes, $qset_size;
}
if ($qset_sizes[$#qset_sizes] < $max_eval_qs) {
    push @qset_sizes, $max_eval_qs;
}

print STDERR "Running boot stability for ", join(",", @qset_sizes), "\n";

srand (1);

# generate question sets
for my $qset_size (@qset_sizes) {
    my $total_comps = 0;
    my %swaps = ();
    my %noswaps = ();
    
    print STDERR "Qset $qset_size";

    # repeat the whole process 500 time for each qset size
    for my $trial (1 .. 500) { 

	my @questions;
	my @score;
	my @scoreA;
	my @scoreB;
	my @diff;
	my @qset;

	print STDERR "." if $trial%10 == 0;

	# randomly select two sets of $qset_size questions with replacement
	for my $i (0 .. 1) {
	    for my $sel (0 .. ($qset_size-1)) {
		my $choice = int(rand $num_qs);
		$qset[$i][$sel] = $topics[$choice];
	    }
	}

        # evaluate all runs on both question sets
	for my $r (0 .. ($num_runs - 1)) {
	    for my $m (@measures) {
		$score[$r][0]{$m} = ave_score(\@{$qset[0]}, $m, 
					      $qset_size, $runs[$r]);
		$score[$r][1]{$m} = ave_score(\@{$qset[1]}, $m, 
					      $qset_size, $runs[$r]);
	    }
	}

	# look at all run score differences
        for (my $r1 = 0; $r1<($num_runs-1); $r1++) {
            for (my $r2=$r1+1; $r2<$num_runs; $r2++) {
		for my $m (@measures) {
		    $scoreA[0] = $score[$r1][0]{$m};
		    $scoreA[1] = $score[$r1][1]{$m};
		    $scoreB[0] = $score[$r2][0]{$m};
		    $scoreB[1] = $score[$r2][1]{$m};

		    $diff[0] = $scoreA[0] - $scoreB[0];
		    $diff[1] = $scoreA[1] - $scoreB[1];

		    $total_comps++;
		    my $bin = get_bin(abs($diff[0]));
		    if ($diff[0]*$diff[1] < 0) {
			$swaps{$m}[$bin]++;
		    }
		    else {
			$noswaps{$m}[$bin]++;
		    }
		}
	    }
        }
    }

    print STDERR "\n";

    for my $m (@measures) {
	for (my $b=0; $b<$num_bins; $b++) {
	    if ($swaps{$m}[$b]+$noswaps{$m}[$b] > 0) {
		printf "$b\t$qset_size\t$m\t%.2f\n",
		    $swaps{$m}[$b]/($swaps{$m}[$b]+$noswaps{$m}[$b]);
	    }
	    else {
		print "$b\t$qset_size\t$m\t0\n";
	    }
	}
    }

}



# given a floating point difference between 0 and 1, return an
# integer bin number
sub get_bin {
    my ($diff) = @_;

    if    ($diff < .01) { return 0; }
    elsif ($diff < .02) { return 1; }
    elsif ($diff < .03) { return 2; }
    elsif ($diff < .04) { return 3; }
    elsif ($diff < .05) { return 4; }
    elsif ($diff < .06) { return 5; }
    elsif ($diff < .07) { return 6; }
    elsif ($diff < .08) { return 7; }
    elsif ($diff < .09) { return 8; }
    elsif ($diff < .10) { return 9; }
    elsif ($diff < .11) { return 10; }
    elsif ($diff < .12) { return 11; }
    elsif ($diff < .13) { return 12; }
    elsif ($diff < .14) { return 13; }
    elsif ($diff < .15) { return 14; }
    elsif ($diff < .16) { return 15; }
    elsif ($diff < .17) { return 16; }
    elsif ($diff < .18) { return 17; }
    elsif ($diff < .19) { return 18; }
    elsif ($diff < .20) { return 19; }
    elsif ($diff >= .20) { return 20; }
    else {
        die "get_bin has difference of $diff\n";
    }
}



# compute and return ave score for given
# question set and run tag
sub ave_score {
    my ($qsetref, $measure, $qset_size, $tag) = @_;

    my $ave = 0;

    for (my $q=0; $q<$qset_size; $q++) {
        my $qid = $$qsetref[$q];
	$ave += $scores{$tag}{$measure}[$qid];
    }
    $ave /= $qset_size;

    return ($ave);
}



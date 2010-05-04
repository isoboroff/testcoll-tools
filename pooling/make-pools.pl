#!/usr/bin/perl

=head1 NAME

make-pools.pl

=head1 SYNOPSIS

make-pools.pl [-bsvh] <pool-desc>

=head1 DESCRIPTION

Build pools by taking a uniform random sample of the full pool.
Optionally, output the full pool down to a given depth and sample the
rest.  Sample probability can be constant, or set per topic based on 
an overall pool size budget.

=head1 OPTIONS

The pooling plan may be specified in the script below (using the
variable @pool_plan), or given on the command line.  Specifications
are ordered, with earlier ones taking precedence over later ones.
Thus, a directive to pool to rank 100 will override a later directive
to draw a 10% sample starting at rank 20.  The default plan is to pool
to depth 100, but this default is overridden by any given plan.

Every specifications includes a maximum depth to pool to.  The runs
are only read to the largest depth given.

=over

=item B<-s> I<from>:I<to>:I<rate>

A pool sampling specification.  This instructs the script to draw a
uniform random sample at the given I<rate>, starting at depth I<from>
up to and including depth I<to>.

=item B<-s> I<from>:I<to>

Draw a uniform random sample starting at rank I<from> up to and
including depth I<to>.  If a budget is specified (see the B<-b>
option), the sampling rate is dictated by the remaining budget.  If no
budget is specified, draw a 100% sample (i.e., a pool).

=item B<-s> I<pool-depth>

A shorthand for C<-s 1:I<pool-depth>:1>, that is, draw a traditional
pool to depth I<pool-depth>.

=item B<-s> I<topic>:I<from>:I<to>:I<rate>

Specify a topic-specific sampling scheme.  This overrides earlier
plans for the given topic only. NOT YET IMPLEMENTED.

=item B<-b> I<number-of-docs-to-pool>

Specify a "budget", a maximum number of documents to pool for each
topic.  This is a global specification and overrides any other pooling
specifications.

=item B<-v>

Verbose output.

=item B<-h>

Help and usage information.

=back

=head1 EXAMPLES

To create a traditional depth-100 pool:

=over

C<make-pools.pl -s 1:100:1>
C<make-pools.pl -s 100>

=back

To pool to depth 50, then skip to rank 400 and pool to a maximum of
1000 documents per topic: (the ending rank of 2000 is needed to tell
the script when to stop reading runs.)

=over

C<make-pools.pl -b 1000 -s 1:50:1 -s 400:2000:1>

=back

To pool the top 20, then randomly sample to fill a budget of 800 documents,
up to a rank of 1500:

=over

C<make-pools.pl -b 800 -s 1:20:1 -s 21:1500>

=back

=head1 FILES

Information on what runs to pool is read from the file I<pool-desc>.
This can have one of three formats.  The simplest is a list of runs,
one per line.  The second is a list of runs with depths.  In this
case, the depth given per run is used as a maximum depth per run and
overrides any pooling specifications.  Lastly, the format can be a
shell script for running the traditional sortmerge scripts.

Runs are in TREC format, and must be properly sorted and broken into
per-topic files.  The sort incantation is 'sort -k1n -k5nr -k3'.

Locations for these scripts are set in variables below... change for
each TREC and track.

=head1 BUGS

You can't specify depths or sampling rates per topic yet.

Per-run depths don't work currently.

Not very well tested yet.  Count the output carefully.  Verbose mode is handy.

=cut

use strict;
use IO::File;
use Getopt::Long;
use Pod::Usage;

# Modify these variables to match your TREC situation

my $root = "/trec/trec15/terabyte";
my $runs_root = "$root/results";
my @topics = (801 .. 850);

# Default pooling plan.  Leave as is and change using the command-line
# switches, or encode your pooling plan here as documentation.  The
# syntax is a list of hashes, where each hash has the fields 'from',
# 'to', and 'rate'.  If 'rate' is missing, it is computed based on the
# budget allotment.

my @pool_desc = 
    ( { from => 1, to => 100, rate => 1.0 },
    );


# Shouldn't need to change stuff below here.

my %pool_runs;
my @pool_runs_list;
my %qrel;

my $verbose = 0;
my $max_to_pool = 0;
my $help = 0;
my $max_depth = 0;
my $include_unsampled = 1;
my @spec = ();
GetOptions('verbose!' => \$verbose,
	   'help!' => \$help,
	   'spec=s' => \@spec,
	   'budget=i' => \$max_to_pool,
	   'unsampled!' => \$include_unsampled,
	   ) or pod2usage(2);

pod2usage(1) if $help;
pod2usage(2) if $max_to_pool < 0;

my $pool_runs_desc = shift or die pod2usage(1);

my $oldfh = select(STDERR); $| = 1; select($oldfh);

#
# Parse pooling specs from the command line
#

if (scalar @spec > 0) {
    @pool_desc = ();
    for my $s (@spec) {
	my @fields = split /:/, $s;
	if (scalar @fields == 1) {
	    push @pool_desc, { from => 1, to => $fields[0], rate => 1 };
	    $max_depth = $fields[0] if $fields[0] > $max_depth;

	} elsif (scalar @fields == 2) {
	    if ($max_to_pool == 0) {
		die "Spec '$s' needs a budget to compute the rate\n";
	    }
	    push @pool_desc, { from => $fields[0],
			       to => $fields[1],
			     };
	    $max_depth = $fields[1] if $fields[1] > $max_depth;

	} elsif (scalar @fields == 3) {
	    push @pool_desc, { from => $fields[0],
			       to => $fields[1],
			       rate => $fields[2],
			     };
	    $max_depth = $fields[1] if $fields[1] > $max_depth;
	}
    }
}

#
# Read pool_runs to get pool construction details
#
open(POOL_RUNS, $pool_runs_desc) or die "Can't read $pool_runs_desc: $!\n";
while (<POOL_RUNS>) {
    next if /^#/;
    next if /^$/;

    chomp;
    my @fields = split;
    if (scalar @fields == 1) {
	$pool_runs{$fields[0]} = 100;
	push @pool_runs_list, $fields[0];

    } elsif (scalar @fields == 2) {
	$pool_runs{$fields[0]} = $fields[1];
	push @pool_runs_list, $fields[0];

    } elsif (/sortmerge/) {
	my ($prog, $track, $depth, $run, $run2) = @fields;
	$pool_runs{$run} = $depth;
	push @pool_runs_list, $run;
	if ($prog =~ /sortmerge_first/) {
	    $pool_runs{$run2} = $depth;
	    push @pool_runs_list, $run2;
	}
    }
}
close(POOL_RUNS);

for my $topic (@topics) {
    print STDERR "Pooling for $topic... ";
    my %fhash;

    # Clear out ndocs and real_rate in each spec
    for my $spec (@pool_desc) {
	delete $spec->{real_rate};
	delete $spec->{ndocs};
    }
    
    # Set up pool queue (no pun intended)
    for my $run (@pool_runs_list) {
	my $fh = new IO::File;
	$fh->open("$runs_root/$run/t$topic")
	    or die "Can't read $runs_root/$run/t$topic: $!\n";
	$fhash{$run} = $fh;
    }

    my %pool;
    my %runtag;
    my %poolrank;
    my $total_pool_docs = 0;
    my $current_depth = 1;
    my $cur_spec;
    my @tmp_runs_list = (@pool_runs_list, "END_OF_LIST");

    for my $spec (@pool_desc) {
	if ($current_depth >= $spec->{from} and
	    $current_depth <= $spec->{to}) {
	    $cur_spec = $spec;
	}
    }

    while (my $run = shift @tmp_runs_list) {
	if ($run eq "END_OF_LIST") {
	    last if (scalar @tmp_runs_list == 0);
	    $current_depth++;
	    @{$poolrank{$current_depth}} = ();
	    push @tmp_runs_list, $run;

	    $cur_spec = undef;
	    for my $spec (@pool_desc) {
		if ($current_depth >= $spec->{from} and
		    $current_depth <= $spec->{to}) {
		    $cur_spec = $spec;
		}
	    }
	    next;
	}

	my $fh = $fhash{$run};
	# Skip a run if we've used all its documents.
	if ($fh->eof or
	    ($max_depth > 0 and $current_depth > $max_depth) or
	    ($max_depth == 0 and $current_depth > $pool_runs{$run})) {
	    $fh->close;
	    next;
	}

	# Read a document from the run at the head of the queue.
	my $line = $fh->getline;
	my ($top, undef, $docid, $rank, $sim, $tag) = split " ", $line;

	if (defined $cur_spec and !exists $pool{$docid}) {
	    $pool{$docid} = $sim;
	    $runtag{$docid} = $tag;
	    push @{$poolrank{$current_depth}}, $docid;
	    $total_pool_docs++;
	    $cur_spec->{ndocs}++;

	    print STDERR "$total_pool_docs: $docid from run $tag ($current_depth; $sim; $rank)\n" if $verbose;
	}
	
	push @tmp_runs_list, $run;
    }

    print STDERR "total $total_pool_docs (",
	join(",", map { $_->{ndocs} } @pool_desc), ")\n";

    my $budget = $max_to_pool ? $max_to_pool : $total_pool_docs;

    for my $i (0 .. $current_depth) {
	next if !exists $poolrank{$i};
	my $target_prob;

	SPEC: for my $spec (@pool_desc) {
	    if ($i >= $spec->{from} and $i <= $spec->{to}) {
		if (exists $spec->{real_rate}) {
		    $target_prob = $spec->{real_rate};
		}

		elsif (exists $spec->{rate}) {
		    $spec->{real_rate} = $spec->{rate};
		    $target_prob = $spec->{real_rate};

		} else {
		    $target_prob = $budget / $spec->{ndocs};
		    $spec->{real_rate} = $target_prob;
		}
		last SPEC;
	    }
	}
	
	for my $docid (@{ $poolrank{$i} }) {
	    if ($budget > 0 and rand() <= $target_prob) {
		if ($verbose) {
		    print "$topic Q0 0 terabyte $docid $i $pool{$docid} -1 $runtag{$docid} $i $budget $target_prob\n";
		} else {
		    print "$topic Q0 0 terabyte $docid $i $pool{$docid} -1 $runtag{$docid}\n";
		}
		$budget--;
	    }
	}
    }
}



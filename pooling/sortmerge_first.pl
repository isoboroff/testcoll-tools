#!/usr/bin/perl -w

#  Create a judging pool merging the first two runs
#  Usage: sortmerge_first.pl run-type depth sysdir1 sysdir2 
#	where run-type is the type of the runs 
#	depth is the number of documents from each run to add to the pools and
#	sysdir is the run tag (and name of the directory in which the
#		runs' documents appear)
# ASSUMES ALL TOPIC IDS ARE NUMBERS AND THAT THE INDIVIDUAL
# TOPIC RESULTS FILES HAVE BEEN SORTED BY SIM

# Make sure both runs have all topics defined

# check to make sure that command line arguments are correct
if ( $#ARGV != 3) {
   die "Usage: sortmerge_first.pl <run_type> <depth> <SYSDIR1> <SYSDIR2>\n";
}
$rtype = $ARGV[0];
$depth = $ARGV[1];
$sysdir1 = $ARGV[2];
$sysdir2 = $ARGV[3];

# make sure pool type known
if ($rtype =~ "qa") {
    $pool = "qa";
    $task = "";
}
elsif ($rtype eq "HARD") { 
    $pool = "HARD";
    $task = "";
}
elsif ($rtype eq "robust") { 
    $pool = "HARD";
    $task = "";
}
elsif ($rtype eq "genomics") {
    $pool = "genomics";
    $task = "adhoc";
}
elsif ($rtype eq "enterprise") {
    $pool = $rtype;
    $task = "discussion-search";
}
elsif ($rtype eq "terabyte") {
    $pool = "terabyte";
    $task = "namedpage";
}
#elsif ($rtype eq "qa" || $rtype eq "HARD" || $rtype eq "novelty") {
#    die "$rtype track runs evaluated differently.  Can't use sortmerge\n";
#}
else {
   die "sortmerge_first.pl: Unknown run type $rtype\n"
}
$rdir = "/trec/trec14/$rtype/results";
$pooldir = "/trec/trec14/$pool/results";

open TOPICS, "</trec/trec14/aux/valid.topics.$rtype-$task" ||
	die "sortmerge_first.pl: Can't open topics file valid.topics.$rtype-$task\n";
while (<TOPICS>) {
    next if $_ !~ /([0-9.]+)/ ;
    push @topics, $1;
}
close TOPICS || die "Can't close topic file\n";


open LOG, ">$pooldir/logs/log.$sysdir1.$sysdir2" || 
	die "Can't open log file logs/log.$sysdir1.$sysdir2\n";

# Merge the data files into a file of unique document numbers
print LOG  "Merging $sysdir1 and $sysdir2\n"; 

foreach $topic (sort numerically @topics) {
    undef @run1_docnos;  undef %run1_sims; undef %run1_ranks; undef %pool_docs;
    undef @run2_docnos;  undef %run2_sims; undef %run2_ranks;

    if ( (! -e "$rdir/$sysdir1/t$topic") ||
		(! open TFILE, "<$rdir/$sysdir1/t$topic") )  {
	die "Missing/unreadable file for topic $topic for $sysdir1\n";
    }

    $num_docs1 = 0;
    while ($line = <TFILE>) {
	chomp $line;
	($t,$q0,$docno,$rank,$sim,$tag1) = split " ", $line;
	$run1_docnos[$num_docs1++] = $docno;
	$run1_sims{$docno} = $sim;
	$run1_ranks{$docno} = $rank;
    }
    close TFILE || die "Can't close file t$topic: $!\n";
    @run1_docnos = sort by_sim1_docno @run1_docnos;

    if ( (! -e "$rdir/$sysdir2/t$topic") ||
		(! open TFILE, "<$rdir/$sysdir2/t$topic") )  {
	print LOG "Missing/unreadable file for topic $topic for $sysdir2\n";
	next;
    }

    $num_docs2 = 0;
    while ($line = <TFILE>) {
	chomp $line;
	($t,$q0,$docno,$rank,$sim,$tag2) = split " ", $line;
	$run2_docnos[$num_docs2++] = $docno;
	$run2_sims{$docno} = $sim;
	$run2_ranks{$docno} = $rank;
    }
    close TFILE || die "Can't close file t$topic: $!\n";
    @run2_docnos = sort by_sim2_docno @run2_docnos;

    if (! open POOL, ">$pooldir/mastermerge/t$topic") {
	die "Can't open pool file for topic $topic: $!\n";
    }
    $num_added = 0;
    for ($i=0; $i<$depth; $i++) {
	last if ($i >= $num_docs1);	  # at end of retrieved docs; quit
	$docno = $run1_docnos[$i];
	$pool_docs{$docno} = 1;
	$num_added++;

   	# mapping of docnos to appropriate database names
   	# no longer required by web assess
	$db = $pool;

   	print POOL
	"$topic Q0 0 $db $docno $run1_ranks{$docno} $run1_sims{$docno} -1 $tag1\n";
    }
    for ($i=0; $i<$depth; $i++) {
	last if ($i >= $num_docs2);	  # at end of retrieved docs; quit

	$docno = $run2_docnos[$i];
	next if (exists $pool_docs{$docno});# already in pool; continue

	$num_added++;
   	# map docnos no longer needed
        $db = $pool;

   	print POOL
	"$topic Q0 0 $db $docno $run2_ranks{$docno} $run2_sims{$docno} -1 $tag2\n";
    }
    close POOL || die "Can't close pool for topic $topic after writing: $!\n";
    `sort +4 -5 $pooldir/mastermerge/t$topic > foo$$`;
    if ($?) {
	die "sort failed for $topic: $!\n";
    }
    `mv foo$$ $pooldir/mastermerge/t$topic`;
    if ($?) {
	die "mv of foo$$ failed for $topic: $!\n";
    }
    print LOG "Topic $topic: Added $num_added to pool\n"; 
}
close LOG || die "Can't close log file: $!\n";

sub numerically { $a <=> $b; }

# Sort a run's documents by descending similarity and descending docno
sub by_sim1_docno {
    $run1_sims{$b} <=> $run1_sims{$a}
    or
    $b cmp $a
}
sub by_sim2_docno {
    $run2_sims{$b} <=> $run2_sims{$a}
    or
    $b cmp $a
}

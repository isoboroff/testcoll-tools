#!/usr/bin/perl -w

#  Add documents from a run to the existing (unjudged) pool 
#  Usage: sortmerge.pl run_type depth sysdir 
#	where run_type is the type of run
#	depth is the number of documents from each run to add to the pool and
#	sysdir is the run tag (and name of the directory in which the
#		run's documents appear)
#  If the pool doesn't yet exist, use sortmerge_first.pl
# ASSUMES ALL TOPIC IDS ARE NUMBERS AND THAT THE INDIVIDUAL
# TOPIC RESULTS FILES HAVE BEEN SORTED BY SIM

# check to make sure that command line arguments are correct
if ( $#ARGV != 2) {
   die "Usage: sortmerge.pl <run_type> <depth> <SYSDIR>\n";
}
$rtype = $ARGV[0];
$depth = $ARGV[1];
$sysdir = $ARGV[2];

# make sure run type known
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
#elsif ($rtype eq "qa" || $rtype eq "novelty" || $rtype eq "HARD") {  
#    die "$rtype track runs evaluated differently.  Can't use sortmerge\n";
#}
else {
   die "sortmerge.pl: Unknown run type $rtype\n"
}
$rdir = "/trec/trec14/$rtype/results";
$pooldir = "/trec/trec14/$pool/results";

open TOPICS, "</trec/trec14/aux/valid.topics.$pool-$task" ||
        die "sortmerge.pl: Can't open topics file valid.topics.$pool-$task\n";
while ($line = <TOPICS>) {
    next if ($line !~ /([0-9.]+)/) ;
    push @topics, $1;
}
close TOPICS || die "Can't close topic file\n";


open LOG, ">/trec/trec14/$pool/results/logs/log.master.$sysdir" || 
	die "Can't open log file logs/log.$sysdir\n";

# Merge the data files into a file of unique document numbers
print LOG  "Merging master and $sysdir\n"; 

foreach $topic (sort numerically @topics) {
    undef @run_docnos;  undef %run_sims; undef %run_ranks; undef %pool_docs;

    if ( (! -e "$rdir/$sysdir/t$topic") ||
		(! open TFILE, "<$rdir/$sysdir/t$topic") )  {
	die "Missing/unreadable file for topic $topic\n";
    }

    $num_docs = 0;
    while ($line = <TFILE>) {
	chomp $line;
	($t,undef,$docno,$rank,$sim,$tag) = split " ", $line; 
	$run_docnos[$num_docs++] = $docno;
	$run_sims{$docno} = $sim;
	$run_ranks{$docno} = $rank;
    }
    close TFILE || die "Can't close file $topic: $!\n";
    @run_docnos = sort by_sim_docno @run_docnos;

    if ( (! -e "$pooldir/mastermerge/t$topic")  ||
	 (! open POOL, "<$pooldir/mastermerge/t$topic") ) {
	die "Can't open pool file for topic $topic: $!\n";
    }
    while ($line = <POOL>) {
	($t,undef,undef,$db,$docno,undef) = split " ", $line, 6;
	$pool_docs{$docno} = 1;
    }
    close POOL || die "Can't close pool for topic $topic after reading: $!\n";

    if (! open POOL, ">>$pooldir/mastermerge/t$topic") {
	die "Can't open pool for topic $topic for appending: $!\n";
    }
    $num_added = 0;
    for ($i=0; $i<$depth; $i++) {
	last if ($i >= $num_docs);	  # at end of retrieved docs; quit

	$docno = $run_docnos[$i];
	next if (exists $pool_docs{$docno});  # already in pool; continue

	$num_added++;
   	# map docnos to appropriate database names
	# no longer required by web assess
	$db = $pool;

   	print POOL
	"$topic Q0 0 $db $docno $run_ranks{$docno} $run_sims{$docno} -1 $tag\n";
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
close LOG || die "Can't close logfile: $!\n";

sub numerically { $a <=> $b; }

# Sort a run's documents by descending similarity and descending docno
sub by_sim_docno {
    $run_sims{$b} <=> $run_sims{$a}
    or
    $b cmp $a
}

use strict;
use warnings;
use feature ':5.16';

# varijable za citanje i pisanje fajlova
my ($file_in, $file_out, $file_tbl, $file_log);
my ($file_count, $line_count);

$file_in  = "";
$file_out = "output";
$file_tbl = "tablice";
$file_log = "errLog";

$file_count = $line_count = 0;

# polja za export 
    my ($t_stamp, $launch, $full_SQL, $SQL_cmd, $plan);
    my ($state_no, $fetched, $ms, $reads, $writes, $fetches, $marks);
    my @tbls; # (table, natural, index, update, insert, delete, backup, purge, expunge)

# pomocni flag   
my $progress = 0;

open(OUT, ">", $file_out) or die "Datoteku $file_out se ne moze otvoriti za pisanje: $!"; 
open(TBL, ">", $file_tbl) or die "Datoteku $file_tbl se ne moze otvoriti za pisanje: $!"; 
open(LOG, ">", $file_log) or die "Datoteku $file_log se ne moze otvoriti za loganje: $!"; 
    
$t_stamp = $launch = $full_SQL = $SQL_cmd = $plan = undef;
$state_no = $fetched = $ms = $reads = $writes = $fetches = $marks = 0;

while (<>) {
    # ovo mi i ne treba ako mi ne zanima naziv fajla i broj linija
    if ($file_in ne $ARGV) {
	say $file_count, " ", ,$line_count, " ", $file_in if $file_count; 
    	$file_count++;
	$file_in = $ARGV;
	$line_count = 0;
    }

    $line_count++;
    
    START:                                             # 1 pocetak sekvence
    if ($progress == 0) {
	if (/EXECUTE_STATEMENT_FINISH/) {
	    $t_stamp = substr $_, 0, 24;
	    $progress++;
       }
       next;
    }
    
    LAUNCH:                                            # 2 tko je izveo SQL
    if ($progress == 1) {
	if (/^.+((\/opt.+)|(\/home.+)):/) {
	    $launch = $1;
	    $progress++;
	} elsif (/^Statement \-?(\d+):/) {             # 3 nekad ne postoji redak  prorun|isql
	    $launch = "internal";
	    $state_no = $1;
	    $progress++;
	    $progress++;
	}
	next;
    }

    STATEMENT:                                         # 3 extract statement number 
    if ($progress == 2) {
	if (/^Statement \-?(\d+):/) {
	    $state_no = $1;
	    $progress++;
	}
	next;
    }    

    SQL:                                               # 4 SQL statement
    if ($progress == 3) {
	if (/^PLAN/) { 
	    chomp;
	    $plan = $_;
	    $progress++;
	} elsif (/^(\d+) records fetched/) {
	    $plan = "err";
	    $fetched = $1;
	    $progress++;
	    $progress++;
	    $progress++;
	} elsif (/^\-{5,}/) {
	} elsif (/^\^{5,}/) {
	    $progress++
	} else {
	    chomp; s/\h+/ /g;
	    $full_SQL .= $_;
	}	
	next;
    }

    PLAN:                                              # 5 PLAN statement 
    if ($progress == 4) {
	if (/^PLAN/) {
	    chomp; s/\h+/ /g;	
	    if ($plan)     { $plan .= " && " }
	    $plan .= $_ ;
	    next;
	} else { 
	    unless ($plan) { $plan = "err" }           # plana nije ni bilo 
	    $progress++;			       # propadni dolje	
	}
    }

    FETCHED:                                           # 6 records fetched
    if ($progress == 5) {
	if (/^(\d+) records fetched/) {
	    $fetched = $1;
	    $progress++;
	}
	next;   
    }
    
    REKAP:                                             # 7 trajanje i broj operacija
    if ($progress == 6) {
	if (/(\d+) ms/)    { $ms      = $1} 
	if (/(\d+) read/)  { $reads   = $1} 
	if (/(\d+) write/) { $writes  = $1} 
	if (/(\d+) fetch/) { $fetches = $1} 
	if (/(\d+) mark/)  { $marks   = $1}
	$progress++;
	next;
    }

    STUB:                                              # 8 zvjezdice
    if ($progress == 7) {
	(/^\*{5,}/) and $progress++;
	next;
    }

    TABLICE:                                           # 9 tablice
    if ($progress == 8) {
	if (/^(\w+)/) { 
	    @tbls = /(.{31})(.{10})(.{10})(.{10})(.{10})(.{10})(.{10})(.{10})(.{10})/ ;
	    s/\h+//g for @tbls;
	    for my $i (1..8) {
		if ($tbls[$i]) {
		    $tbls[$i] += 0
		} else {
		    $tbls[$i] = 0
		}
	    }
	    say(TBL "$t_stamp;", (join ";",@tbls));
	    undef $_ for @tbls;
	    next;
	} else {
	    $progress++;
	}	
	#next;
    }

    END:                                               # kraj sekvence
    if ($progress == 9) {
	if ($full_SQL =~ /^(\w+)/) { 
	   $SQL_cmd = uc $1 
	} else {
	   $SQL_cmd = "N/A"
	}

	say(OUT
	   "$t_stamp;$launch;$state_no;$SQL_cmd;$full_SQL;$plan",
	   ";$fetched;$ms;$reads;$writes;$fetches;$marks");
	
	$t_stamp = $launch = $full_SQL = $SQL_cmd = $plan = undef;
	$state_no = $fetched = $ms = $reads = $writes = $fetches = $marks = 0;
	$progress = 0;
    }
} #ARGV
say ($file_count, " ", ,$line_count, " ", $file_in) if $file_count; 
close OUT or die "$file_out: $!";
close TBL or die "$file_tbl: $!";
close LOG or die "$file_log: $!";

__END__

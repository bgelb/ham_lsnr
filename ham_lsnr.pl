#!/usr/bin/perl -w
use strict;

# ham_lsnr7.pl
#
# ChangeLog:
# V6, August 2, 2006, Ben Gelb:
# When SIG{CHLD} handler is called, the blocking
# socket->accept() is interrupted.
# Since there isn't actually a new incoming connection, this
# breaks the main loop
# and kills the parent process. This was causing the
# crashing and hangups last year
# on race day. The main loop has been changed to make sure that socket->accept()

# actually returns a valid socket. The other major change
# was to pull out all of the
# aid station, disp, diagnosis codes into definitions at the
# top of the file for easy
# modification without touching the main body of the code.

# V7, October 19, 2007, Ben Gelb:
# - Updated all disp/diag codes
# - Added prompt for transport barcode
# - Added listrunner and listaid commands

# TODO:
# - Implement SQL calls (currently commented out/out of date)
# - Actually do something with the $other_destination data
#   (stick in a comment field in db somwhere) and transport barcode
# - Implement SQL calls and prints for listrunner and listaid commands

# V7.1, October 22, 2007, Brad Chick:
# - Updated db connection to connect to Oracle (8.1.7.4)
# - changed event_id to be sub_event_id (hard-coded to be 2941)
# - Changed SQL to reflect new schema
# - Notable change: bib is no longer primary key, but we moved to an internal id - athlete_id, assigned
#   by a trigger in the main athlete table: medical_athlete
# - Dropped the schema (medical.sql) into ben's home dir for reference.

# V7.2, October 23, 2008, Ben Gelb:
# - update valid aid stations list

# V7.3, October 10, 2009, Ben Gelb:
# - update valid aid stations list

use IO::Socket;

use DBI;
use Switch;

my $data_source =
  "dbi:Oracle:host=iad1-srv01.championchipus.com;sid=ora8;port=1521";

# changed the sub_event_id to match new system (brad)
my $sub_event_id = 2941;

open( ERROR, ">/tmp/temp.$$.txt" ) or die "Unable to open: $!";

BEGIN {
    $SIG{__DIE__} = sub { print ERROR @_; };
}

my $transport_to_other_disposition_code = "TOF";

$SIG{CHLD} = sub { wait() };
my $main_sock = new IO::Socket::INET(
    LocalPort => 7890,
    Listen    => 50,
    Proto     => 'tcp',
    Reuse     => 1,
) or die "Socket could not be created. Reason: $!\n";

while (1) {
    my $new_sock = $main_sock->accept();

    #make sure we unblocked because we actually got a connection,
    #not because something interrupted the accept call
    next if ( !defined($new_sock) );

    my $pid = fork();
    die "Cannot fork: $!" unless defined($pid);
    if ( $pid == 0 ) {

        # Child process

        my $dbh = DBI->connect( $data_source, "doitreg", "doitregrules" )
          or die "ERR: Couldn't open connection: " . $DBI::errstr . "\n";

        # following hash is unnecessary; used for testing
        my %valid_aid_stations = (
            '1'  => 'AS 1/2',
            '3'  => 'AS 3/5',
            '4'  => 'AS 4',
            '6'  => 'AS 6',
            '7'  => 'AS 7',
            '8'  => 'AS 8/9',
            '10'  => 'AS 10',
            '20' => 'Medical Alpha',
            '30' => 'Medical Bravo'
        );

     # location_ids
     # =====================
     # drop all the location_id values into hash of arrays with the ham input as
     # the key
        my %location_ids = ();
        my $sth =
          $dbh->prepare(
"SELECT ham_input, location_id, location_code FROM medical_location where sub_event_id = $sub_event_id"
          ) || die $dbh->errstr;
        $sth->execute() || die $sth->errstr;
        while ( my @row = $sth->fetchrow_array ) {
            $location_ids{ $row[0] } = [ $row[1], $row[2] ];

#print "................$location_ids{$row[0]}[0] ====== $location_ids{$row[0]}[1]\n";
        }
        $sth->finish;

        # valid_diagnosis_codes
        # =====================
        # hash map with the diagnosis code as the key
        # diagnosis_id as the value
        my %valid_diagnosis_codes = ();
        $sth =
          $dbh->prepare(
"SELECT diagnosis_code, diagnosis_id FROM medical_diagnosis where sub_event_id = $sub_event_id"
          ) || die $dbh->errstr;
        $sth->execute() || die $sth->errstr;
        while ( my @row = $sth->fetchrow_array ) {
            $valid_diagnosis_codes{ $row[0] } = $row[1];
        }
        $sth->finish;

        my ( $day, $month, $year ) = (localtime)[ 3, 4, 5 ];

       # valid_disposition_codes
       # =====================
       # the values in the following hashes are now used...
       # the key is the ham_input, the value is the dispostion_id needed for the
       # table: medical_visit
        my %valid_disposition_codes = ();
        $sth =
          $dbh->prepare(
"SELECT disposition_code, disposition_id FROM medical_disposition where sub_event_id = $sub_event_id"
          ) || die $dbh->errstr;
        $sth->execute() || die $sth->errstr;
        while ( my @row = $sth->fetchrow_array ) {
            $valid_disposition_codes{ $row[0] } = $row[1];
        }
        $sth->finish;
        my $insert_sql;
        my $this_diag_code;
        my $numrows;
        print $new_sock "Aid Station: ";
        my $got_good_station = "f";

        my $station = "";
        while ( defined( my $buf = <$new_sock> ) ) {
            $buf =~ s/\s//g;
            if ( $got_good_station eq "f" ) {

                # Ask what aid station they are at
                $station = $buf;

                if ( exists( $valid_aid_stations{$station} ) ) {
                    $got_good_station = "t";
                    print $new_sock "Data OK.\n-> ";
                }
                else {
                    print $new_sock
                      "ERROR: Invalid Aid Station!\nAid Station: ";
                }
            }
            else {

 #Patient Check-In:
 # Runner number, time in <ENTER>
 #Patient Check-Out:
 # Runner number, time in (opt), time out, disposition, diag1, diag2,... <ENTER>
                if (   uc $buf eq "Q"
                    or uc $buf eq "QUIT"
                    or uc $buf eq "B"
                    or uc $buf eq "BYE" )
                {
                    close($new_sock);
                    exit(0);
                }
                my @a = split ',', $buf;
                my $num_elements = $#a + 1;
###
                # INPUT VALIDATION
                my $update_type = undef;
                my $insert_sql  = undef;
###
                if ( $num_elements == 2 ) {
                    ## 'LA' command followed by number gets aid station status
                    ## 'LR' command gets runner status
                    if ( uc $a[0] eq 'LA' ) {
                        if ( !exists( $valid_aid_stations{ $a[1] } ) ) {
                            print $new_sock "ERROR: Invalid Aid Station!\n->";
                            next;
                        }
                        $update_type = "listaid";
                    }
                    elsif ( uc $a[0] eq 'LR' ) {
                        $update_type = "listrunner";
                    }
                    else {
                        $update_type = "checkin";

                        $insert_sql =
" insert into medical_visit( sub_event_id, athlete_id, location_id, checkin_time) values( ?, ?, ?, to_date(?, 'HHMI') ) ";
                    }
                }
                elsif ( $num_elements >= 5 ) { $update_type = "checkout"; }
                else {
                    print $new_sock " ERROR: Invalid Field Count !\n->";
                    next;
                }

                #
                # number of diagnosis codes
                my $num_diag_codes = $num_elements - 4;

                # list commands
                if ( $update_type eq 'listaid' ) {

                    # PUT SQL AND PRINT STATEMENTS FOR LIST AID STATION
                    # PATIENTS HERE

                    print $new_sock "LIST AID: $a[1]\n";
                    print $new_sock "->";
                    next;
                }

                if ( $update_type eq 'listrunner' ) {

                    # PUT SQL AND PRINT STATEMENTS FOR LIST RUNNER DATA HERE

                    print $new_sock "LIST RUNNER: $a[1]\n";
                    print $new_sock "->";
                    next;
                }

                # check time-in/out:
                #
                my $in_out = undef;
                if ( $update_type eq 'checkin' ) {
                    $in_out = " In ";
                }
                else { $in_out = " Out "; }
                my $inhours    = undef;
                my $inminutes  = undef;
                my $outhours   = undef;
                my $outminutes = undef;
                if ( $update_type eq 'checkin'
                    or ( $update_type eq 'checkout' and length $a[1] > 0 ) )
                {
                    if (   length $a[1] < 3
                        or $a[1] < 0
                        or $a[1] > 2359 )
                    {
                        print $new_sock " ERROR: Invalid Time In !\n->";
                        next;
                    }
                    if ( length $a[1] == 3 ) {
                        $a[1] = "0" . $a[1];
                    }
                    $inhours   = substr( $a[1], 0, 2 );
                    $inminutes = substr( $a[1], 2, 2 );
                    if (   $inhours < 0
                        or $inhours > 23
                        or $inminutes < 0
                        or $inminutes > 59 )
                    {
                        print $new_sock " ERROR: Invalid Time In !\n->";
                        next;
                    }
                }
                if ( $update_type eq 'checkout' ) {
                    if (   length $a[2] < 3
                        or $a[2] < 0
                        or $a[2] > 2359 )
                    {
                        print $new_sock " ERROR: Invalid Time Out !\n->";
                        next;
                    }
                    if ( length $a[2] == 3 ) {
                        $a[2] = "0" . $a[2];
                    }
                    $outhours   = substr( $a[2], 0, 2 );
                    $outminutes = substr( $a[2], 2, 2 );
                    if (   $outhours < 0
                        or $outhours > 23
                        or $outminutes < 0
                        or $outminutes > 59 )
                    {
                        print $new_sock " ERROR: Invalid Time Out !\n->";
                        next;
                    }
                }
                my $disposition_id = undef;

                my $transport_barcode = "";
                my $other_destination = "";
                if ( $update_type eq 'checkout' ) {

                    # check time-out:
                    # make sure time-in is not later than time-out
                    #if (get_from_db > $a[3]) {
                    # print $new_sock " ERROR: Time In After Time Out !\n->";
                    # next;
                    #}
                    my $Ud = uc $a[3];
                    $a[3] = $Ud;

                    if ( !exists( $valid_disposition_codes{$Ud} ) ) {
                        print $new_sock
                          " ERROR: Invalid Disposition Code !\n->";
                        next;
                    }
                    if ( $Ud eq $transport_to_other_disposition_code ) {
                        print $new_sock " Other Destination:->";
                        $other_destination = <$new_sock>;
                    }
                    #if ( length $Ud == 3 ) {
                    #    print $new_sock " Transport Barcode:->";
                    #    $transport_barcode = <$new_sock>;
                    #}
                    $disposition_id = $valid_disposition_codes{$Ud};
                    my $i = 1;

                    #print $new_sock " num_diag_codes: $num_diag_codes \n->";
                    my $bad_diag_code = "f";
                    while ( $i <= $num_diag_codes ) {
                        $this_diag_code = $i + 3;
                        my $Uc = uc $a[$this_diag_code];
                        $i++;

                        $a[$this_diag_code] = $Uc;

                        if ( !exists( $valid_diagnosis_codes{$Uc} ) ) {
                            print $new_sock
                              " ERROR: Invalid Diagnosis Code !\n->";

                            $bad_diag_code = "t";
                            next;
                        }
                    }
                    if ( $bad_diag_code eq "t" ) {
                        next;
                    }
                }
                my ( $athlete_id, $first_names );
                my $visit_id;
                my $locale = $location_ids{$station}[0];

                # print "locale-------------------> $locale\n";
                if ( $update_type eq 'checkin' or $update_type eq 'checkout' ) {

                    # check runner bib: is it in the db?
                    #

                    my $sth =
                      $dbh->prepare(
"SELECT athlete_id, first_names from medical_athlete WHERE sub_event_id = $sub_event_id AND bib_number = '$a[0]' "
                      );

                    $sth->execute;
                    $sth->bind_columns( undef, \$athlete_id, \$first_names );
                    my $num_records = 0;
                    while ( $sth->fetch ) {
                        $num_records++;

                        #FOR DEBUGGING ONLY(brad);
                        #print "name => $first_names, ath_id => $athlete_id\n";
                    }
                    if ( $num_records == 0 ) {
                        print $new_sock " ERROR: Invalid Runner Number !\n->";
                        next;
                    }
                    elsif ( $num_records > 1 ) {
                        print $new_sock
" ERROR: That Bib ($a[0]) in DB $num_records times!\n->";
                        next;
                    }
                    $sth->finish;

                    # grab the next sequence id for insertions
                    # should have been a trigger on primary key, but gave up
                    # cuz i couldn't figure out how to get the value out
                    my $sth2 =
                      $dbh->prepare(
"SELECT medial_visit_sequence.nextval as visit_id from dual"
                      );
                    $sth2->bind_columns( undef, \$visit_id );
                    $sth2->execute;
                    while ( $sth2->fetch ) {
                    }
                    $sth2->finish;

                }
                my $notes = "";
                if ( $update_type eq 'checkin' ) {

                    if ( $transport_barcode || $other_destination ) {
                        #$notes = "transport barcode: $transport_barcode";
                        $notes = "";
			$notes .= ("other destination: $other_destination") if($other_destination);
                    }
                    my $sth3 = $dbh->prepare(
                        "insert into medical_visit (visit_id,
                          athlete_id, location_id,
                          checkin_time, notes
                      ) values ($visit_id,
                          $athlete_id,
                          $locale,
                          to_date(
                              '2007.10.28 $a[1]',
                              'yyyy.mm.dd HH24MI'
                          ), '$notes'
                      )"
                    );
                    $sth3->execute
                      or die "Couldn't execute statement: " . $sth3->errstr;

                    #$numrows = $dbh->do( $insert_sql );
                    #$numrows = 1;

                }
                elsif ( $update_type eq 'checkout' ) {

# check-in is implied, checkin time is in the 3rd position of array of inputs
# must do an insert for each of the codes provided
#
# input string: bib number, [check in time], check out time, disposition code, diagnosis  code 1,diagnosis code 2, diagnosis code 3, ...
#
# new schema requires to update the medical_visit record,
# then, for each diagnosis code to insert a record into the mapping table
#
                    if ( $transport_barcode || $other_destination ) {
                        $notes = "transport barcode: $transport_barcode";
			$notes .= ("\nother destination: $other_destination") if($other_destination);
                    }
                    my $primary_insert_sql =
"insert into medical_visit (visit_id, athlete_id, location_id, checkout_time, disposition_id, record_timestamp";
                    if ( length $a[1] > 0 ) {
                        $primary_insert_sql .= ", checkin_time";
                    }
                    $primary_insert_sql .= ", notes" if ($notes);
                    $primary_insert_sql .=
") values ($visit_id, '$athlete_id','$locale',to_date('2007.10.28 $a[2]','yyyy.mm.dd HH24MI'), '$disposition_id', sysdate";
                    if ( length $a[1] > 0 ) {
                        $primary_insert_sql .=
                          ", to_date('2007.10.30 $a[1]','yyyy.mm.dd HH24MI')";
                    }
                    $primary_insert_sql .= ",'$notes' " if ($notes);
                    $primary_insert_sql .= ")";

                    #$sth->bind_param_inout(":id", \my $visit_id, 99999);
                    $dbh->do($primary_insert_sql);

                    my $n = 1;
                    while ( $n <= $num_diag_codes ) {
                        $this_diag_code = $n + 3;
                        $n++;

                        #grab the diagnosis_id from the hash
                        my $diagnosis_id =
                          $valid_diagnosis_codes{ $a[$this_diag_code] };

                        $insert_sql =
"insert into medical_visit_to_diagnosis_map (visit_id, diagnosis_id) values ($visit_id, $diagnosis_id)";

                        my $numrows = $dbh->do( $insert_sql, undef );

                        #$numrows = 1;

                        #FOR DEBUGGING ONLY (bgelb)
                        if ( !$numrows ) {
                            die "Failed to insert: " . $dbh->errstr . "\n-> ";
                        }
                    }
                }
                print $new_sock "Data OK. <$first_names>\n-> ";
            }
        }
        $dbh->disconnect;
        exit(0);    # Child process exits when it is done.;
    }    # else 'tis the parent process, which goes back to accept()
}

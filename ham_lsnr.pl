#!/usr/bin/perl -w

#
# ham_lsnr.pl
#
# Accepts TCP connections and provides interactive interface to
# MCM medical tracker database
#

use strict;

use IO::Socket;
use DBI;
use Switch;

# Database connection parameters
my $data_source      = "dbi:Oracle:host=127.0.0.1;sid=orcl;port=1521";
my $data_source_user = "doitreg";
my $data_source_pass = "doitregrules";

# Port to listen for incoming TCP connections
my $incoming_tcp_port = 7890;

# Event ID (2941 = MCM)
my $sub_event_id = 2941;

# Hardcode "other" disposition code, so we can prompt for extra info
# TODO: encode this in database somehow
my $transport_to_other_disposition_code = "TOF";

open( ERROR, ">logs/ham_lsnr.ERROR.$$.txt" ) or die "Unable to open: $!";

$SIG{__DIE__} = sub { print ERROR @_; };
$SIG{CHLD} = sub { wait() };

sub is_time_valid {
	return ($_[0] =~ m/^((0?[0-9])|(1[0-9])|(2[0-3]))[0-5][0-9]$/);
}

my $main_sock = new IO::Socket::INET(
    LocalPort => $incoming_tcp_port,
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

        my $dbh = DBI->connect( $data_source, $data_source_user, $data_source_pass )
          or die "ERR: Couldn't open connection: " . $DBI::errstr . "\n";

        # valid_location_ids
        # =====================
        # drop all the location_id values into hash of arrays with the ham input as
        # the key
        my %valid_location_ids = ();
        my $sth = $dbh->prepare("SELECT ham_input, location_id, location_code, prompt_for_more_info_p FROM medical_location where sub_event_id = $sub_event_id") || die $dbh->errstr;
        $sth->execute() || die $sth->errstr;
        while ( my @row = $sth->fetchrow_array ) {
            $valid_location_ids{ $row[0] } = [ $row[1], $row[2] ];
        }
        $sth->finish;

        # valid_diagnosis_codes
        # =====================
        # hash map with the diagnosis code as the key
        # diagnosis_id as the value
        my %valid_diagnosis_codes = ();
        $sth = $dbh->prepare("SELECT diagnosis_code, diagnosis_id FROM medical_diagnosis where sub_event_id = $sub_event_id") || die $dbh->errstr;
        $sth->execute() || die $sth->errstr;
        while ( my @row = $sth->fetchrow_array ) {
            $valid_diagnosis_codes{ $row[0] } = $row[1];
        }
        $sth->finish;

        # valid_disposition_codes
        # =====================
        # the values in the following hashes are now used...
        # the key is the ham_input, the value is the dispostion_id needed for the
        # table: medical_visit
        my %valid_disposition_codes = ();
        $sth = $dbh->prepare("SELECT disposition_code, disposition_id FROM medical_disposition where sub_event_id = $sub_event_id") || die $dbh->errstr;
        $sth->execute() || die $sth->errstr;
        while ( my @row = $sth->fetchrow_array ) {
            $valid_disposition_codes{ $row[0] } = $row[1];
        }
        $sth->finish;
        my $insert_sql;
        my $this_diag_code;
        my $numrows;

        my $station;
        my $buf;

        #
        # Handle Login
        #

        while (1) {
            print $new_sock "Aid Station: ";
            if(defined($buf = <$new_sock>)) {
                $buf =~ s/\s//g; # strip whitespace
                if($buf eq "?") {
                    print $new_sock "Valid locations:\n";
                    foreach (sort { $a <=> $b }(keys %valid_location_ids)) {
                        print $new_sock "  $_ $valid_location_ids{$_}[1]\n";
                    }
                }
                elsif(exists($valid_location_ids{$buf})) {
                    $station = $buf;
                    print $new_sock "Hello $valid_location_ids{$station}[1]!\n";
                    print $new_sock "\n->";
                    last;
                }
                else {
                    print $new_sock "ERROR: Invalid Aid Station!\n";
                }
            }
            else {
                $dbh->disconnect();
                close($new_sock);
                exit(0);
            }
        }

        #
        # Main Input Loop
        #

        while ( defined( $buf = <$new_sock> ) ) {
            $buf =~ s/\s//g;

            #Patient Check-In:
            # Runner number, time in <ENTER>
            #Patient Check-Out:
            # Runner number, time in (opt), time out, disposition, diag1, diag2,... <ENTER>
            if (   uc $buf eq "Q"
                or uc $buf eq "QUIT"
                or uc $buf eq "B"
                or uc $buf eq "BYE"
                or uc $buf eq "D"
                or uc $buf eq "DISCONNECT")
            {
                $dbh->disconnect();
                close($new_sock);
                exit(0);
            }
            my @a = split ',', $buf;
            my $num_elements = $#a + 1;

            # INPUT VALIDATION
            my $update_type = undef;
            my $insert_sql  = undef;

            if ( $num_elements == 2 ) {
                ## 'LA' command followed by number gets aid station status
                ## 'LR' command gets runner status
                if ( uc $a[0] eq 'LA' ) {
                    if ( !exists( $valid_location_ids{ $a[1] } ) ) {
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

                    $insert_sql = " insert into medical_visit( sub_event_id, athlete_id, location_id, checkin_time) values( ?, ?, ?, to_date(?, 'HHMI') ) ";
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
            if ( $update_type eq 'checkin'
                or ( $update_type eq 'checkout' and length $a[1] > 0 ) )
            {
                if (!(&is_time_valid($a[1])))
                {
                    print $new_sock " ERROR: Invalid Time In !\n->";
                    next;
                }
            }
            if ( $update_type eq 'checkout' ) {
                if (!(&is_time_valid($a[2])))
                {
                    print $new_sock " ERROR: Invalid Time Out !\n->";
                    next;
                }
            }
            my $disposition_id = undef;

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
                $disposition_id = $valid_disposition_codes{$Ud};
                my $i = 1;

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
            my $locale = $valid_location_ids{$station}[0];

            if ( $update_type eq 'checkin' or $update_type eq 'checkout' ) {

                # check runner bib: is it in the db?
                #

                my $sth =
                  $dbh->prepare("SELECT athlete_id, first_names from medical_athlete WHERE sub_event_id = $sub_event_id AND bib_number = '$a[0]' ");

                $sth->execute;
                $sth->bind_columns( undef, \$athlete_id, \$first_names );
                my $num_records = 0;
                while ( $sth->fetch ) {
                    $num_records++;
                }
                if ( $num_records == 0 ) {
                    print $new_sock " ERROR: Invalid Runner Number !\n->";
                    next;
                }
                elsif ( $num_records > 1 ) {
                    print $new_sock " ERROR: That Bib ($a[0]) in DB $num_records times!\n->";
                    next;
                }
                $sth->finish;

                # grab the next sequence id for insertions
                # should have been a trigger on primary key, but gave up
                # cuz i couldn't figure out how to get the value out
                my $sth2 =
                  $dbh->prepare("SELECT medial_visit_sequence.nextval as visit_id from dual");
                $sth2->bind_columns( undef, \$visit_id );
                $sth2->execute;
                while ( $sth2->fetch ) {
                }
                $sth2->finish;

            }
            my $notes = "";
            if ( $update_type eq 'checkin' ) {

                if ( $other_destination ) {
					$notes = ("other destination: $other_destination") if($other_destination);
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
            }
            elsif ( $update_type eq 'checkout' ) {

                # check-in is implied, checkin time is in the 3rd position of array of inputs
                # must do an insert for each of the codes provided
                #
                # input string: bib number, [check in time], check out time, disposition code,
				# diagnosis code 1,diagnosis code 2, diagnosis code 3, ...
                #
                # new schema requires to update the medical_visit record,
                # then, for each diagnosis code to insert a record into the mapping table
                #
                if ( $other_destination ) {
					$notes = ("\nother destination: $other_destination") if($other_destination);
                }
                my $primary_insert_sql = "insert into medical_visit (visit_id, athlete_id, location_id, checkout_time, disposition_id, record_timestamp";
                if ( length $a[1] > 0 ) {
                    $primary_insert_sql .= ", checkin_time";
                }
                $primary_insert_sql .= ", notes" if ($notes);
                $primary_insert_sql .= ") values ($visit_id, '$athlete_id','$locale',to_date('2007.10.28 $a[2]','yyyy.mm.dd HH24MI'), '$disposition_id', sysdate";
                if ( length $a[1] > 0 ) {
                    $primary_insert_sql .=
                      ", to_date('2007.10.30 $a[1]','yyyy.mm.dd HH24MI')";
                }
                $primary_insert_sql .= ",'$notes' " if ($notes);
                $primary_insert_sql .= ")";

                $dbh->do($primary_insert_sql);

                my $n = 1;
                while ( $n <= $num_diag_codes ) {
                    $this_diag_code = $n + 3;
                    $n++;

                    #grab the diagnosis_id from the hash
                    my $diagnosis_id =
                      $valid_diagnosis_codes{ $a[$this_diag_code] };

                    $insert_sql = "insert into medical_visit_to_diagnosis_map (visit_id, diagnosis_id) values ($visit_id, $diagnosis_id)";

                    $dbh->do( $insert_sql, undef );
                }
            }
            print $new_sock "Data OK. <$first_names>\n-> ";
        }
        close($new_sock);
        $dbh->disconnect;
        exit(0);    # Child process exits when it is done.;
    }    # else 'tis the parent process, which goes back to accept()
}

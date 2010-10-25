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

open( ERROR, ">logs/ham_lsnr.ERROR.$$.txt" ) or die "Unable to open: $!";

$SIG{__DIE__} = sub { print ERROR @_; };
$SIG{CHLD} = sub { wait() };

sub is_time_valid {
    return ($_[0] =~ m/^((0?[0-9])|(1[0-9])|(2[0-3]))[0-5][0-9]$/);
}

sub get_athlete_id { # dbh, bib, athlete_id, first_name, err_str
    my $sth;
    my $num_records;

    $sth = $_[0]->prepare("SELECT athlete_id, first_names from medical_athlete WHERE sub_event_id = $sub_event_id AND bib_number = '$_[1]' ");

    $sth->execute;
    $sth->bind_columns( undef, \$_[2], \$_[3] );
    $num_records = 0;
    while ( $sth->fetch ) {
        $num_records++;
    }
    if ( $num_records == 0 ) {
        $_[4] = "Invalid Runner Number !";
        return 0;
    }
    elsif ( $num_records > 1 ) {
        $_[4] = "That Bib ($_[1]) in DB $num_records times!";
        return 0;
    }
    return 1;
}

sub get_next_visit_id { # dbh
    # grab the next sequence id for insertions
    # should have been a trigger on primary key, but gave up
    # cuz i couldn't figure out how to get the value out
    my $sth;
    my $visit_id;

    $sth = $_[0]->prepare("SELECT medial_visit_sequence.nextval as visit_id from dual");
    $sth->bind_columns( undef, \$visit_id );
    $sth->execute;
    while ( $sth->fetch ) {}
    return $visit_id;
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

        # valid_disposition_codes
        # =====================
        # the values in the following hashes are now used...
        # the key is the ham_input, the value is the dispostion_id needed for the
        # table: medical_visit
        my %valid_disposition_codes = ();
        $sth = $dbh->prepare("SELECT disposition_code, disposition_id, prompt_for_more_info_p FROM medical_disposition where sub_event_id = $sub_event_id") || die $dbh->errstr;
        $sth->execute() || die $sth->errstr;
        while ( my @row = $sth->fetchrow_array ) {
            $valid_disposition_codes{ $row[0] } = [ $row[1], $row[2] ];
        }

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
            if($buf =~ m/^\s*$/g) {
                # no input, so just spit out a new prompt
            }
            elsif($buf =~ m/^[A-za-z]+\s/g) {
                # its a command (not a checkin or checkout)
                my @cmd_vec = split ' ', $buf, 2;
                my $cmd = $cmd_vec[0];
                my $args = $cmd_vec[1];

                if (   uc $cmd eq "Q"
                    or uc $cmd eq "QUIT"
                    or uc $cmd eq "B"
                    or uc $cmd eq "BYE"
                    or uc $cmd eq "D"
                    or uc $cmd eq "DISCONNECT" ) {
                    $dbh->disconnect();
                    close($new_sock);
                    exit(0);
                }
                elsif(uc $cmd eq "LR") {
                }
                elsif(uc $cmd eq "LA") {
                }
                elsif(uc $cmd eq "EC") {
                    my @comment_vec = split ',', $args, 2;
                    $comment_vec[0] =~ s/\s//g;
                    my $err_str;
                    my $athlete_id;
                    my $first_name;
                    my $visit_id;

                    if(!&get_athlete_id($dbh, $comment_vec[0], $athlete_id, $first_name, $err_str)) {
                        print $new_sock " ERROR: $err_str\n->";
                        next;
                    }

                    $visit_id = &get_next_visit_id($dbh);
                    my $sth = $dbh->prepare("insert into medical_visit (visit_id, athlete_id, location_id, notes) values (?,?,?,?)");
                    if(!$sth->execute($visit_id, $athlete_id, $valid_location_ids{$station}[0], $comment_vec[1])) {
                      print $new_sock " ERROR: Database insert failed.\n->";
                      next;
                    }
                    print $new_sock "Data OK. <$first_name>\n";
                }
                else {
                    print $new_sock " ERROR: Invalid input!\n";
                }
            }
            elsif($buf =~ m/^[0-9]{1,5}\s*,/g) {
                $buf =~ s/\s//g;

                #Patient Check-In:
                # Runner number, time in <ENTER>
                #Patient Check-Out:
                # Runner number, time in (opt), time out, disposition, diag1, diag2,... <ENTER>
                my @a = split ',', $buf;
                my $num_elements = $#a + 1;

                # INPUT VALIDATION
                my $update_type = undef;

                if ( $num_elements == 2 ) {
                    $update_type = "checkin";
                }
                elsif ( $num_elements >= 5 ) {
                    $update_type = "checkout";
                }
                else {
                    print $new_sock " ERROR: Invalid Field Count !\n->";
                    next;
                }

                #
                # number of diagnosis codes
                my $num_diag_codes = $num_elements - 4;

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

                    if ( !exists( $valid_disposition_codes{$Ud}[0] ) ) {
                        print $new_sock
                          " ERROR: Invalid Disposition Code !\n->";
                        next;
                    }
                    if ( $valid_disposition_codes{$Ud}[1] ) {
                        print $new_sock " Other Destination:->";
                        $other_destination = <$new_sock>;
                    }
                    $disposition_id = $valid_disposition_codes{$Ud}[0];
                    my $i = 1;

                    my $bad_diag_code = "f";
                    while ( $i <= $num_diag_codes ) {
                        my $this_diag_code = $i + 3;
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
                my ( $athlete_id, $first_name );
                my $visit_id;
                my $locale = $valid_location_ids{$station}[0];

                if ( $update_type eq 'checkin' or $update_type eq 'checkout' ) {

                    # check runner bib: is it in the db?
                    #
                    my $err_str;

                    if(!&get_athlete_id($dbh, $a[0], $athlete_id, $first_name, $err_str)) {
                        print $new_sock " ERROR: $err_str\n->";
                        next;
                    }

                    $visit_id = &get_next_visit_id($dbh);
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
                              concat(to_char(sysdate, 'YYYY.MM.DD'),' $a[1]'),
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
                    $primary_insert_sql .= ") values ($visit_id, '$athlete_id','$locale',to_date(concat(to_char(sysdate, 'YYYY.MM.DD'),' $a[2]'),'yyyy.mm.dd HH24MI'), '$disposition_id', sysdate";
                    if ( length $a[1] > 0 ) {
                        $primary_insert_sql .=
                          ", to_date('2007.10.30 $a[1]','yyyy.mm.dd HH24MI')";
                    }
                    $primary_insert_sql .= ",'$notes' " if ($notes);
                    $primary_insert_sql .= ")";

                    $dbh->do($primary_insert_sql);

                    my $n = 1;
                    while ( $n <= $num_diag_codes ) {
                        my $this_diag_code = $n + 3;
                        $n++;

                        #grab the diagnosis_id from the hash
                        my $diagnosis_id =
                          $valid_diagnosis_codes{ $a[$this_diag_code] };

                        my $insert_sql = "insert into medical_visit_to_diagnosis_map (visit_id, diagnosis_id) values ($visit_id, $diagnosis_id)";

                        $dbh->do( $insert_sql, undef );
                    }
                }
                print $new_sock "Data OK. <$first_name>\n";
            }
            else {
                print $new_sock " ERROR: Invalid input!\n";
            }
            print $new_sock "->";
        }
        close($new_sock);
        $dbh->disconnect;
        exit(0);    # Child process exits when it is done.;
    }    # else 'tis the parent process, which goes back to accept()
}

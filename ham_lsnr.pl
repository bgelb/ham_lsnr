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
my $data_source      = "dbi:Pg:host=192.168.5.248;port=5432;dbname=xact";
my $data_source_user = "radude";
my $data_source_pass = "raR0cks";

# Port to listen for incoming TCP connections
my $incoming_tcp_port = 7890;

# Event ID
my $event_id = 1896;

# to be populated from DB
my @subevent_ids;
my $subevent_string;

open( ERROR, ">logs/ham_lsnr.ERROR.$$.txt" ) or die "Unable to open: $!";

$SIG{__DIE__} = sub { print ERROR @_; };
$SIG{CHLD} = sub { wait() };

sub is_time_valid {
    return ($_[0] =~ m/^((0?[0-9])|(1[0-9])|(2[0-3]))[0-5][0-9]$/);
}

sub time_add_sep {
    return $_[0];

    # as of 2017, no separator is desired anymore
    # return ($_[0] =~ s/^([0-9]{1,2})([0-5][0-9])/$1:$2/g);
}

sub get_athlete_id { # dbh, bib, athlete_id, first_name, err_str
    my $sth;
    my $num_records;

    $sth = $_[0]->prepare("SELECT id, firstname from entrant WHERE subevent_id IN ($subevent_string) AND bib = ? ");

    $sth->execute($_[1]);
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

    $sth = $_[0]->prepare("SELECT nextval('medvisits_id_seq') as visit_id");
    $sth->execute;
    $sth->bind_columns( undef, \$visit_id );
    while ( $sth->fetch ) {}
    return $visit_id;
}

my $main_sock = new IO::Socket::INET(
    LocalPort => $incoming_tcp_port,
    Listen    => 50,
    Proto     => 'tcp',
    Reuse     => 1,
) or die "Socket could not be created. Reason: $!\r\n";

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
          or die "ERR: Couldn't open connection: " . $DBI::errstr . "\r\n";

        # valid_aid_stations
        # =====================
        # drop all the location_id values into hash of arrays with the ham input as
        # the key
        my %valid_aid_stations = ();
        my %valid_location_ids = ();
        my $sth = $dbh->prepare("SELECT haminput, id, name FROM medlocations WHERE event_id = $event_id") || die $dbh->errstr;
        $sth->execute() || die $sth->errstr;
        while ( my @row = $sth->fetchrow_array ) {
            $valid_aid_stations{ $row[0] } = [ $row[1], $row[2], 0 ];
            $valid_location_ids{ $row[1] } = [ $row[0], $row[2], 0 ];
        }

        # valid_diagnosis_codes
        # =====================
        # hash map with the diagnosis code as the key
        # diagnosis_id as the value
        my %valid_diagnosis_codes = ();
        my %valid_diagnosis_ids = ();
        $sth = $dbh->prepare("SELECT upper(code) as code, id, name FROM medcomplaints where event_id = $event_id") || die $dbh->errstr;
        $sth->execute() || die $sth->errstr;
        while ( my @row = $sth->fetchrow_array ) {
            $valid_diagnosis_codes{ $row[0] } = [ $row[1], $row[2] ];
            $valid_diagnosis_ids{ $row[1] } = [ $row[0], $row[2] ];
        }

        # valid_disposition_codes
        # =====================
        # the values in the following hashes are now used...
        # the key is the ham_input, the value is the dispostion_id needed for the
        # table: medical_visit
        my %valid_disposition_codes = ();
        my %valid_disposition_ids = ();
        $sth = $dbh->prepare("SELECT code, id, prompt_for_more_info, name FROM meddispositions where event_id = $event_id") || die $dbh->errstr;
        $sth->execute() || die $sth->errstr;
        while ( my @row = $sth->fetchrow_array ) {
            $valid_disposition_codes{ $row[0] } = [ $row[1], $row[2], $row[3] ];
            $valid_disposition_ids{ $row[1] } = [ $row[0], $row[2], $row[3] ];
        }

        # subevent_ids
        my $ii;

        $sth = $dbh->prepare("SELECT id FROM subevent WHERE event_id = $event_id") || die $dbh->errstr;
        $sth->execute() || die $sth->errstr;
        $subevent_string = "";
        $ii = 0;
        while ( my @row = $sth->fetchrow_array ) {
            $subevent_ids[$ii] = $row[0];
            if($ii != 0) {
                $subevent_string .= ", ";
            }
            $subevent_string .= $row[0];
            $ii++;
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
                    print $new_sock "Valid locations:\r\n";
                    foreach (sort { $a <=> $b }(keys %valid_aid_stations)) {
                        print $new_sock "  $_ $valid_aid_stations{$_}[1]\r\n";
                    }
                }
                elsif(exists($valid_aid_stations{$buf})) {
                    $station = $buf;
                    print $new_sock "Hello $valid_aid_stations{$station}[1]!\r\n";
                    print $new_sock "\r\n->";
                    last;
                }
                else {
                    print $new_sock "ERROR: Invalid Aid Station!\r\n";
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
                    my $err_str;
                    my $athlete_id;
                    my $first_name;
                    my $visit_id;

                    $args =~ s/\s//g;
                    if(!&get_athlete_id($dbh, $args, $athlete_id, $first_name, $err_str)) {
                        print $new_sock " ERROR: $err_str\r\n->";
                        next;
                    }
                    print $new_sock "Record for bib $args ($first_name)\r\n\r\n";
                    print $new_sock "Check In/Check Out History:\r\n";

                    my $sth = $dbh->prepare("SELECT to_char(vis.lastupdated, 'HH24:MI.ss') AS ts,
                                                    vis.medlocation_id,
                                                    replace(vis.checkintime, ':', '') as checkin,
                                                    replace(vis.checkouttime, ':', '') as checkout,
                                                    vis.meddisposition_id,
                                                    map.medcomplaint_id
                                                    FROM medvisits vis LEFT OUTER JOIN medvisitcomplaints map
                                                    ON vis.id=map.medvisit_id
                                                    WHERE entrant_id=? ORDER BY ts ASC");
                    $sth->execute($athlete_id);
                    my $count = 0;
                    while ( my @row = $sth->fetchrow_array ) {
                        $count++;
                        if(defined($row[2]) || defined($row[3])) {
                            print $new_sock "  ".$valid_location_ids{$row[1]}[1]." In: ".(defined($row[2]) ? $row[2] : "----")." Out: ".(defined($row[3]) ? $row[3] : "----") ."\r\n";
                            if(defined($row[5]) && exists($valid_diagnosis_ids{$row[5]})) {
                                print $new_sock "    Diagnosis: ".$valid_diagnosis_ids{$row[5]}[1]."\r\n";
                            }
                            if(defined($row[4]) && exists($valid_disposition_ids{$row[4]})) {
                                print $new_sock "    Disposition: ".$valid_disposition_ids{$row[4]}[2]."\r\n";
                            }
                            print $new_sock "\r\n";
                        }
                    }

                    $sth = $dbh->prepare("SELECT to_char(lastupdated, 'HH24:MI.ss') AS ts,
                                            medlocation_id,
                                            notes
                                            FROM medvisits
                                            WHERE notes IS NOT NULL AND entrant_id=?
                                            ORDER BY ts ASC");

                    $sth->execute($athlete_id);
                    print $new_sock "Notes:\r\n";
                    while ( my @row = $sth->fetchrow_array ) {
                        print $new_sock " ".$row[0]." ".$valid_location_ids{$row[1]}[1].": ".$row[2]."\r\n";
                    }
                    if($count == 0) {
                        print $new_sock "No records found for bib $args ($first_name).\r\n";
                    }

                }
                elsif(uc $cmd eq "LA") {
                    my $count = 0;
                    my $loc;
                    my $sth = $dbh->prepare("SELECT entrant.bib,
                                                COUNT(NULLIF(medvisits.checkintime, '')),
                                                COUNT(NULLIF(medvisits.checkouttime, '')),
                                                MIN(to_number(replace(medvisits.checkintime, ':', ''), '9999'))
                                                FROM medvisits, entrant
                                                WHERE medvisits.medlocation_id = ?
                                                AND medvisits.entrant_id = entrant.id
                                                GROUP BY entrant.bib");

                    $args =~ s/\s//g;
                    if(exists($valid_aid_stations{$args})) {
                        $loc = $args;
                    }
                    else {
                        $loc = $station;
                    }
                        $sth->execute($valid_aid_stations{$loc}[0]);
                    print $new_sock "\r\nPatients currently checked in to ".$valid_aid_stations{$loc}[1]."\r\n\r\n";
                    print $new_sock " bib @ time\r\n\r\n";
                    while ( my @row = $sth->fetchrow_array ) {
                        if($row[1] > 0 && $row[2] == 0) {
                            $count++;
                            print $new_sock " ".$row[0]." @ ".$row[3]."\r\n";
                        }
                    }
                    print $new_sock "\r\n$count bib(s) checked in.\r\n";
                }
                elsif(uc $cmd eq "RC") {
                    my @comment_vec = split ',', $args, 2;
                    $comment_vec[0] =~ s/\s//g;
                    my $err_str;
                    my $athlete_id;
                    my $first_name;
                    my $visit_id;

                    if(!&get_athlete_id($dbh, $comment_vec[0], $athlete_id, $first_name, $err_str)) {
                        print $new_sock " ERROR: $err_str\r\n->";
                        next;
                    }

                    $visit_id = &get_next_visit_id($dbh);
                    my $sth = $dbh->prepare("insert into medvisits (id, entrant_id, medlocation_id, notes, checkintime, checkouttime) values (?,?,?,?,NULL,NULL)");
                    if(!$sth->execute($visit_id, $athlete_id, $valid_aid_stations{$station}[0], $comment_vec[1])) {
                      print $new_sock " ERROR: Database insert failed.\r\n->";
                      next;
                    }
                    print $new_sock "Data OK. <$first_name>\r\n";
                }
                else {
                    print $new_sock " ERROR: Invalid input!\r\n";
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
                    print $new_sock " ERROR: Invalid Field Count !\r\n->";
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
                        print $new_sock " ERROR: Invalid Time In !\r\n->";
                        next;
                    }
                }
                if ( $update_type eq 'checkout' ) {
                    if (!(&is_time_valid($a[2])))
                    {
                        print $new_sock " ERROR: Invalid Time Out !\r\n->";
                        next;
                    }
                }
                my $disposition_id = undef;

                my $other_destination = "";
                if ( $update_type eq 'checkout' ) {

                    # check time-out:
                    # make sure time-in is not later than time-out
                    #if (get_from_db > $a[3]) {
                    # print $new_sock " ERROR: Time In After Time Out !\r\n->";
                    # next;
                    #}
                    my $Ud = uc $a[3];
                    $a[3] = $Ud;

                    if ( !exists( $valid_disposition_codes{$Ud}[0] ) ) {
                        print $new_sock
                          " ERROR: Invalid Disposition Code !\r\n->";
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
                              " ERROR: Invalid Diagnosis Code !\r\n->";

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
                my $locale = $valid_aid_stations{$station}[0];

                if ( $update_type eq 'checkin' or $update_type eq 'checkout' ) {

                    # check runner bib: is it in the db?
                    #
                    my $err_str;

                    if(!&get_athlete_id($dbh, $a[0], $athlete_id, $first_name, $err_str)) {
                        print $new_sock " ERROR: $err_str\r\n->";
                        next;
                    }

                    $visit_id = &get_next_visit_id($dbh);
                }
                my $notes = "";
                if ( $update_type eq 'checkin' ) {

                    if ( $other_destination ) {
                        $notes = ("other destination: $other_destination") if($other_destination);
                    }
                    &time_add_sep($a[1]);
                    my $sth3 = $dbh->prepare(
                        "insert into medvisits (id,
                          entrant_id, medlocation_id,
                          checkintime, notes
                      ) values ($visit_id,
                            $athlete_id,
                            $locale,
                            '$a[1]',
                            '$notes'
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
                        $notes = ("\r\nother destination: $other_destination") if($other_destination);
                    }
                    my $primary_insert_sql = "insert into medvisits (id, entrant_id, medlocation_id, checkouttime, meddisposition_id, lastupdated, checkintime";
                    $primary_insert_sql .= ", notes" if ($notes);
                    &time_add_sep($a[2]);
                    $primary_insert_sql .= ") values ($visit_id, '$athlete_id','$locale','$a[2]', '$disposition_id', now()";
                    if ( length $a[1] > 0 ) {
                    &time_add_sep($a[1]);
                        $primary_insert_sql .=
                          ", '$a[1]'";
                    }
                    else {
                        $primary_insert_sql .= ", NULL";
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
                          $valid_diagnosis_codes{ $a[$this_diag_code] }[0];

                        my $insert_sql = "insert into medvisitcomplaints (medvisit_id, medcomplaint_id) values ($visit_id, $diagnosis_id)";

                        $dbh->do( $insert_sql, undef );
                    }
                }
                print $new_sock "Data OK. <$first_name>\r\n";
            }
            else {
                print $new_sock " ERROR: Invalid input!\r\n";
            }
            print $new_sock "->";
        }
        close($new_sock);
        $dbh->disconnect;
        exit(0);    # Child process exits when it is done.;
    }    # else 'tis the parent process, which goes back to accept()
}

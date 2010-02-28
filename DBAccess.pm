# Copyright (c) 2008-01-19, mliang
# 
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
# 
#  * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer 
#    in the documentation and/or other materials provided with the distribution.
#  * Neither the name of the <ORGANIZATION> nor the names of its contributors may be used to endorse or promote products derived from 
#    this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS # "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR # A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
# COPYRIGHT OWNER OR # CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, # EXEMPLARY, OR CONSEQUENTIAL DAMAGES 
# (INCLUDING, BUT NOT LIMITED TO, # PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR # PROFITS; OR BUSINESS INTERRUPTION) 
# HOWEVER CAUSED AND ON ANY THEORY OF # LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING # NEGLIGENCE OR OTHERWISE) 
# ARISING IN ANY WAY OUT OF THE USE OF THIS # SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package BDE::DBAccess;
use strict;

use Exporter;
use vars qw /@ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $VERSION/;

# inherits Exporter module
@ISA = qw /Exporter/;

# default export symbol
@EXPORT = qw /Initial_SQL_Stmt/;

# requires physically exported symbol
@EXPORT_OK = qw /
                 GetDBIConnection      CloseDBIConnection
                 SQL_Prepare_Statement SQL_Execute_Query
                 SQL_Select_OneRow
                /;

# version number
$VERSION = 1.00;

# some modules used NSM::DBAccess
use Carp;
use Cwd qw /getcwd chdir/;
use DBI qw /:sql_types/;
use File::Spec;

use lib '.';
use BDE::Misc;
# ######################################################################################################################## #
#                                                                                                                          #
# package name   : DBAccess                                                                                                #
# purpose        : Provide general database access routines                                                                #
# created by     : a-cliang                                                                                                #
#                                                                                                                          #
# related moudles:                                                                                                         #
#      Carp       -- for showing up a meaningful message from developer's perspective                                      #
#      Cwd        -- for simple directory manipulation                                                                     #
#      DBI        -- database access api via DBD::ODBC                                                                     #
#      File::Spec -- for handling directory path and file stuffs                                                           #
#      MLDBM      -- routines for storing/retriving a complex hash table into/from a file                                  #
#                                                                                                                          #
# Function exports:                                                                                                        #
#       GetDBIConnection($)  - Get a database connection via DBD::ODBC. One parameter, $Config, is accepted.               #
#                              A section name [DBIODBC] and the following parameters need to be defined.                   #
#                                                                                                                          #
#                                DSN - an ODBC file dsn created by ODBC administration program in the machine              #
#                                      that script runs                                                                    #
#                                                                                                                          #
#                                UserName - a user that can access the database in a particular database server            #
#                                                                                                                          #
#                                Password - a password that is used by the user                                            #
#                                                                                                                          #
#                              The function will return a database handle when sucessfully connect to the database defined #
#                              in DSN                                                                                      #
#                                                                                                                          #
#       CloseDBIConnection($)  - Close the passed in connection handle                                                     #
#                                                                                                                          #
#       SQL_Prepare_Statement($$) - Prepare a sql statement to be executed later. The function accepts the following       #
#       parameters:                                                                                                        #
#                                                                                                                          #
#                                         $dbh      - a database handle                                                    #
#                                         $sql_stmt - a sql statement                                                      #
#                                                                                                                          #
#                                       Once the statement is successfully prepared, the $sth, statement handle will be    #
#                                       returned                                                                           #
#                                                                                                                          #
#       SQL_Execute_Query($;\@) - Execute a prepared sql statement. Accepts the following parameters                       #
#                                                                                                                          #
#                                         $sth - a statement handle that is returned by SQL_Prepare_Statement $params -    #
#                                                an optional parameters array. It                                          #
#                                                   has to be a reference of an array                                      #
#       SQL_Select_OneRow($$;$) - Execute a sql statement and return one row back. The function accepts the                #
#                                 following parameters:                                                                    #
#                                                                                                                          #
#                                         $dbh - a database handle                                                         #
#                                         $sql_stmt - a sql statment                                                       #
#                                         $flag - an optional parameter that can be used to determined to return a         #
#                                                 reference of array or a reference                                        #
#                                                 of hash. If not provided, an array reference will be returned.           #
#                                                 To get a hash reference, give the                                        #
#                                                 hashref to it.                                                           #
#                                                                                                                          #
#       Init_SQL_Stmt(@)        - Prepare each SQL statement fed in. The function accepts one or more of references.       #
#                                 Each reference of array needs to contains the following elements:                        #
#                                                                                                                          #
#                                  function name, stmt handle, sql_stmt, col_wants_in_arrayref,                            #
#                                                 return_as_hash_or_not, 'want_result_or_not',                             #
#                                                                                                                          #
#                                 Once the function gets a list of reference arrays it will prepare the statement          #
#                                 by calling SQL_Prepare_Statement to prepare it and return the parepared statement        #
#                                 handle, sth. Finally, Init_SQL_Stmt will return a hash reference that                    #
#                                 contains a reference of each correspond function. For calling each function,             #
#                                 suppose we call                                                                          #
#                                 Init_SQL_Stmt as this:                                                                   #
#                                                                                                                          #
#                                      $sp_func = Init_SQL_Stmt                                                            #
#                                         ['spName', 'Name_sth', 'exec spName', sql_stmt, [c1, c2, ...],                   #
#                                          'want_hash_or_undef', 'no_result_if_dml_stmt' ],                                #
#                                         [ ... ],                                                                         #
#                                         [ ... ],                                                                         #
#                                         ...;                                                                             #
#                                                                                                                          #
#                                 Then the method of calling spName will be like                                           #
#                                                                                                                          #
#                                      $data = $sp_func->{spName}{Name_sth}->()                                            #
#                                                                                                                          #
#                                 The return data will be an array reference of reference arrays                           #
#                                                                                                                          #
# ######################################################################################################################## #

# function prototypes

# === a statement constructor
sub Initial_SQL_Stmt($@);

# === connection related routines
sub GetDBIConnection($;$$$); 
sub CloseDBIConnection($);

# === data handling routines
sub Execute_Query_Stmt($$$$;@); 
sub SQL_Prepare_Statement($$); 
sub SQL_Execute_Query($;$); 
sub SQL_Select_OneRow($$;$);

# === DBI error handling and debugging related variables 
our $TRACE_LEVEL = 0;      # have DBI to print out debug info.  The higher the number, the detail the information will be print
our $raiseError  = 0;      # should DBI raise an error?
our $printError  = 1;      # print out error message
our $printWarn   = 1;      # print out warning message
our $DRIVER      = 'ODBC'; # drive to use, by default we use odbc, but change it as you need

#===  FUNCTION  ================================================================
#         NAME:  GetDBIConnection
#      PURPOSE:  create a connection between client and sql server and return 
#                database handle.
#   PARAMETERS:  $DSN      -- database source name - a connecting string used to 
#                             connect to a give sql server
#                $UserName -- a user name for connecting to a given sql server.  
#                             This is an optional parameters
#                $Password -- a password associate with a given user.
#                             If $UserName and $Password are not provide, then 
#                             asssume this is either a trusted connection or 
#                             they are provided from $DSN string
#                $driver   -- a drive that uses to connect to a given database 
#                             server.  By default this is ODBC, but you can 
#                             change this to use different database drive, i.e 
#                             MySQL or Sqlite.
#      RETURNS:  $dbh -- a database handle
#  DESCRIPTION:  
#       THROWS:  no exceptions
#     COMMENTS:  none
#     SEE ALSO:  n/a
#===============================================================================
sub GetDBIConnection($;$$$) {
    my $DSN      = shift;
    my $UserName = shift;
    my $Password = shift;
    my $driver   = shift || $DRIVER;

    # --- environment variable DBI_DRIVER will overwrite default driver
    $driver = $ENV{DBI_DRIVER} if ( defined $ENV{DBI_DRIVER} );
    # Get ODBC dsn;
    my $dsn = join(':', "dbi:$DRIVER", $DSN);

    # create database handle here
    my $dbh;
    if ( !$UserName && !$Password ) {
        $dbh = DBI->connect($dsn, undef, undef, { RaiseError => $raiseError,
                                                  PrintError => $printError,
                                                  PrintWarn  => $printWarn } )
            or croak "Cannot create database handle!" . $DBI::errstr . "\n";
    }
    else {
        $dbh = DBI->connect( $dsn, $UserName, $Password, { RaiseError => $raiseError } )
            or croak "Cannot create database handle!" . $DBI::errstr . "\n";
    }
    $dbh->{odbc_default_bind_type} = SQL_VARCHAR; # change the default character bind type to VARCHAR
    # if nothing wrong, return the handle

    # return database handle
    return $dbh;

}

#===  FUNCTION  ================================================================
#         NAME:  Initial_SQL_Stmt
#      PURPOSE:  create a closure object for a particular database that 
#                contains sql statement handlers.  
#   PARAMETERS:  $init_array is an array reference that contains 6 elements,
#                array elements		explanation
#                  0				sub_name - name of sub-routine
#                  1                sth_name - name of the statement handle
#                  2                sql_stmt - the actual sql statement that is 
#                                              being sent to a target sql server
#                  3				col_idx  - which columns do you want to 
#                                              retrieve? the columns you want 
#                                              to retrieve need to be in an 
#                                              array references [0, 1, ..., n]; 
#                                              if you want all columns to be 
#                                              returned, use 0 instead.
#                  4				wanted_hash - if this is defined, dbi 
#                                                 return a hash of hash instead 
#                                                 of an array of array
#                  5				no_result - if this is defined, then it 
#                                               tells dbi that the statement
#                                               is a non-select statement (i.e.
#                                               insert, delete, and update)
#
#      RETURNS:  a clouse object \%handle, which is a hash reference
#
#  DESCRIPTION:  the Initial_SQL_Stmt accepts a list of array references that 
#                contains various sql statments (they can be stored procedures, 
#                select statment or DML statements), it then uses dbi prepare 
#                method to prepare them, and return a hash object \%handler to 
#                the caller.  To access each statement, use
#
#                    $db->{sub_name}{sth_name}->()   # provide necessary 
#                                                    # parameter if sql 
#                                                    # statement has them defined
#                
#                To access original database handle, use
#
#                    $db->{dbh}
#
#                This can be handy to access some dbi database handle 
#                attributes
#
#                To access original statement handle use, 
#
#                $db->{GET_STH}->( 'sth_name' );
#
#       THROWS:  dbi exceptions if any
#     COMMENTS:  none
#     SEE ALSO:  DBI, DBD::ODBC
#===============================================================================
sub Initial_SQL_Stmt($@) {
    my $dsn        = shift;
    my $init_array = \@_;

    die 'the passed in parameter must be an array reference!' unless ref $init_array eq 'ARRAY';

    # trun off the ref so that we can use symbolic reference
    no strict qw /refs vars/;
    my $dbh = GetDBIConnection $dsn;  # get database handler
    $dbh->trace($TRACE_LEVEL);        # set trace level if debug is in need

    # a hash for storing methods and ...
    my %handlers;

    # store database handle
    $handlers{dbh} = $dbh;

    # processing $init_array, and stores a function reference to a hash 
    # reference
    my $sub_name; my $sth_name; my $sql_stmt; my $col_idx; my $wanted_hash; my $no_result;
    foreach ( @{$init_array} ) {
        # get all the parameters
        $sub_name    = $_->[0];
        $sth_name    = $_->[1];
        $sql_stmt    = $_->[2];
        $col_idx     = $_->[3];
        $wanted_hash = $_->[4];
        $no_result   = $_->[5];

        my $sth = SQL_Prepare_Statement $dbh, $sql_stmt;
        $handlers{sth}{$sth_name} = $sth;

        # # store a function reference within %handlers
        $handlers{$sub_name}{$sth_name} = sub { Execute_Query_Stmt $sth, $col_idx, $wanted_hash, $no_result, @_; }; 
    }

    # METHOD EXEC: execute prepared statement
    $handlers{EXEC} = sub {
        my $sth_name = shift;
        my $params   = \@_;

        my $sth  = $handlers{sth}{$sth_name};
        my $data = ref $col_idx ? Execute_Query_Stmt $sth, $col_idx, $wanted_hash, $no_result, $param
                                : Execute_Query_Stmt $sth, undef, $wanted_hash, $no_result, $param;

        return $data;

    };

    # METHOD CLOSE: close database handle
    $handlers{CLOSE} = sub { $dbh->disconnect };

    # METHOD GET_STH: get DBI statment handle
    $handlers{GET_STH} = sub { return $handlers{sth}{$_[0]} };

    # return a reference of hash back to caller
    return \%handlers;

}

#===  FUNCTION  ================================================================
#         NAME:  Execute_Query_Stmt
#      PURPOSE:  execute a query and fetch a result as either an array of 
#                arrays or a hash of hashes
#   PARAMETERS:  $sth       -- a statement handle
#                $col_idx   -- which columns to be fetched if provided.
#                $want_hash -- return a hash of hashes if this is defined
#                $no_result -- if defined then statment returns no result back 
#                              but rows affected
#      RETURNS:  if statement will return a result, this can be either a hash 
#                of hashes or an array of arrayes depending $want_hash is 
#                defined or not; otherwise, return a number of rows affected if 
#                this is insert, deleted or update statement.
#  DESCRIPTION:  ????
#       THROWS:  no exceptions
#     COMMENTS:  none
#     SEE ALSO:  n/a
#===============================================================================
sub Execute_Query_Stmt($$$$;@) {
    my $sth         = shift; # statement handle
    my $col_idx     = shift; # column to be retrieved if provided
    my $wanted_hash = shift; # require a hash reference 
    my $no_result   = shift; # the statement will not return any result set backup
    my $param       = \@_ if @_; # parameters pass to statement handle

    my $rs;
    my $rows;

    # statement will return a result set back 
    if ( !defined $no_result ) {
        SQL_Execute_Query $sth, $param ? \@$param : undef;
        if ( !$wanted_hash ) { # return an array reference back
            $rs = ref $col_idx eq 'ARRAY' ? $sth->fetchall_arrayref( $col_idx )
                                          : $sth->fetchall_arrayref;
        }
        else { # return a hash reference
            $rs = ref $col_idx eq 'ARRAY' ? $sth->fetchall_hashref( $col_idx )
                                          : $sth->fetchall_hashref;
        }
    }
    # statement is one of the insert, delete, update query
    # so no result set return but just number of row being affected
    else {
        if ( ref $param eq 'ARRAY' ) {
            $rs = $sth->execute( @$param );
        }
        else {
            $rs = $sth->execute;
        }
        $rows = $sth->rows;
    }
    
    # need to close the statment handler, this makes the closure function can 
    # be call repeatly
    $sth->finish;

    return ($rs, $rows);
}

#===  FUNCTION  ================================================================
#         NAME:  SQL_Prepare_Statement
#      PURPOSE:  prepare a sql statment
#   PARAMETERS:  $dbh      -- database handle
#                $sql_stmt -- a sql statement to be parepared
#      RETURNS:  a statment handle
#  DESCRIPTION:  SQL_Prepare_Statement calls DBI prepare method to submit a sql 
#                statment to the backend database to prepare.  Most of the 
#                backend sql servers support prepare.
#       THROWS:  no exceptions
#     COMMENTS:  none
#     SEE ALSO:  n/a
#===============================================================================
sub SQL_Prepare_Statement($$) {
    my $dbh      = shift;
    my $sql_stmt = shift;

    my $sth = $dbh->prepare($sql_stmt)
        or croak "cannot prepare a sql statement, $sql_stmt for " . $dbh->errstr . "\n";

    return $sth;
}

#===  FUNCTION  ================================================================
#         NAME:  SQL_Select_OneRow
#      PURPOSE:  return one row data back to the caller
#   PARAMETERS:  $dbh      -- database handle
#                $sql_stmt -- a DML sql statment (any sql statment that will
#                             return result back)
#                $flag     -- arrayref or hashref to return either array or hash
#                         reference
#      RETURNS:  either an array reference or hash reference that contains one 
#                row of data
#  DESCRIPTION:  SQL_Select_OneRow use DBI function
#       THROWS:  no exceptions
#     COMMENTS:  none
#     SEE ALSO:  n/a
#===============================================================================
sub SQL_Select_OneRow($$;$) {
    my $dbh      = shift;
    my $sql_stmt = shift;
    my $flag     = shift || 'arrayref';

    my $data;
    if ( $flag eq 'arrayref') {
        $data = $dbh->selectrow_arrayref( $sql_stmt );
    }
    elsif ( $flag eq 'hashref' ) {
        $data = $dbh->selectrow_hashref( $sql_stmt );
    }
    else { croak "unknow flag passed in\n" }

    return $data;

}

#===  FUNCTION  ================================================================
#         NAME:  SQL_Execute_Query
#      PURPOSE:  execute a statement and return its stament handle
#   PARAMETERS:  $sth    -- statement handle
#                $params -- parameters that needs to pass to statement handle 
#                           if any
#      RETURNS:  ????
#  DESCRIPTION:  ????
#       THROWS:  no exceptions
#     COMMENTS:  none
#     SEE ALSO:  n/a
#===============================================================================
sub SQL_Execute_Query($;$) {
    my $sth       = shift;
    my $params    = shift;

    # determined if a user has passed in any parameters
    if ( ref $params eq 'ARRAY' ) {
        return $sth->execute( @{ $params } ) or croak 'cannot execute statement:' . $DBI::errstr . "\n";
    }
    else {
        return $sth->execute or croak 'cannot execute statement:' . $DBI::errstr . "\n";
    }
}

#===  FUNCTION  ================================================================
#         NAME:  CloseDBIConnection
#      PURPOSE:  close the connection to a given database
#   PARAMETERS:  a database handle
#      RETURNS:  N/A
#  DESCRIPTION:  pass in dbh (database handle created by DBI) and call the 
#                disconnect method to close the connection to that particular 
#                database
#       THROWS:  
#     COMMENTS:  none
#     SEE ALSO:  n/a
#===============================================================================
sub CloseDBIConnection($) {
    $_[0]->disconnect;
}

1;
__END__

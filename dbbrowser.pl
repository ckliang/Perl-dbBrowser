#!/usr/bin/perl
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

use strict;
use warnings;

use FindBin qw/$Bin/;
use lib "$Bin/../lib";
use BDE::SQLServer;
$BDE::DBAccess::TRACE_LEVEL = 0;

# === main ===
my $option = getOptions();

my $db = initDBObject( $option->{dsn} );

# --- list all stored procedures
if ( $option->{'list-sp'} ) {
    listStoredProcedures();
    exit 0;
}

# --- list all views
if ( $option->{'list-view'} ) {
    listViews();
    exit 0;
}

# --- list all tables
if ( $option->{'list-table'} ) {
    listTables();
    exit 0;
}

# --- list all databases
if ( $option->{'list-db'} ) {
    listDatabases();
}

# --- list all columns for a given table
if ( $option->{'help-object'} ) {
    use Data::Dumper;

    my $table = shift @ARGV;
    my ($row) = help_object( $db, $table );
    print Dumper $row;
    exit 0;
}

# --- list all table functions
if ( $option->{'list-tf'} ) {
    my ($functions) = list_objects( $db, 'tf' );
    foreach my $tf (@$functions) {
        print $tf->[0], "\n";
    }
    exit 0;
}

# --- list all scalar functions
if ( $option->{'list-sf'} ) {
    my ($functions) = list_objects( $db, 'fn' );
    foreach my $sf (@$functions) {
        print $sf->[0], "\n";
    }
    exit 0;
}

# --- output sql highlight code to various format (this uses external program highlight)
if ( defined $option->{'output-code'} && $option->{'output-code'} ) {
    my $obj_name      = shift @ARGV;
    die "you have to provide an object name when specify --output-code option!\n" unless defined $obj_name;
    my $filename      = outputcode( $db->{sqlserver}{sp_helptext}, $obj_name );
    my $output_format = $option->{'output-code-format'} if defined $option->{'output-code-format'};
    highlight( $filename, $obj_name, $output_format );
    unlink $filename if -e $filename;
    exit 0;
}

# --- execute a query
if ( $option->{query} ) {
    submit_query( $db, $option->{query} );
    exit 0;
}

# === user defined function ===


#===  FUNCTION  ================================================================
#         NAME:  outputcode
#      PURPOSE:  output stored procedure/functions/views code
#   PARAMETERS:  $obj_name - name of the above objects
#      RETURNS:  a temp filename that contains code
#  DESCRIPTION:  outputcode uses sp_helptext to retrives code object and write 
#                to temprary file, and return the filename to caller
#       THROWS:  no exceptions
#     COMMENTS:  In order for highlight exteranl program to use, outputcode 
#                actually write text to a temp file and return that temp filename back
#     SEE ALSO:  n/a
#===============================================================================
use File::Temp qw/ tempfile /;
sub outputcode {
	my $sp_help  = shift;
    my $obj_name = shift;
   
    # TODO: we may need to consider different database server need to use 
    # different method to output code!!
    my ($fh, $filename) = tempfile();
    my ($text_obj)      = $sp_help->( $obj_name );

    foreach my $code ( @$text_obj ) {
       print $fh $code->[0];
    }
    close $fh;

    return $filename;
}

#===  FUNCTION  ================================================================
#         NAME:  highlight
#      PURPOSE:  generate a color code for a given code object
#   PARAMETERS:  $filename - name of the code file
#                $output   - output format, default to console
#      RETURNS:  N/A
#  DESCRIPTION:  highlight wraps an external program 'highlight' to generate a 
#                color code
#===============================================================================
sub highlight {
    my $filename      = shift;
    my $obj_name      = shift;
    my $output_format = shift || 'console';

    # transfer text into highlight command line option
    my $output_option;
    if ( $output_format eq 'console' ) {
        $output_option = '-A';
    }
    elsif ( $output_format eq 'xhtml' ) {
        $output_option = '-X';
    }
    elsif ( $output_format eq 'xml' ) {
        $output_option = '-Z';
    }

    # try to locate highlight
    my $highlight = qx/which highlight/;
    if ( !defined $highlight ) {
        $highlight = $ENV{HIGHLIGHT};
    }
    die "cannot find highlight!\n" unless defined $highlight;

    # generate a highlight command line options and execute it
    my $cmd = qq{highlight --syntax sql -d $obj_name $output_option $filename};
    system( $cmd );
}

#===  FUNCTION  ================================================================
#         NAME:  getOptions
#      PURPOSE:  provide a command line options
#   PARAMETERS:  N/A
#      RETURNS:  a hash reference that contains valid options
#  DESCRIPTION:  configure uses Getopt::Long to parse command line options, and 
#                Pod::Usage to provide help messages
#       THROWS:  no exceptions
#     COMMENTS:  configure use Getopt::Long and Pod::Usage to provide a better 
#                command line parsing and help messages
#     SEE ALSO:  Getopt::Long and Pod::Usage
#=======z=======================================================================
use Getopt::Long qw/:config auto_help/;
use Pod::Usage;
use constant DSN => 'DRIVER={SQL Server};Server=%s;Database=%s;%s';
sub getOptions {
    pod2usage(1) unless @ARGV; # force help message if no arguments

    # --- parse the command line options ---
    my $option = {};
    my @envs   = ();
    # Getopt::Long::Configure('bundling');
    
    # -- initialize variables
    $option->{'list-db'} = $option->{'list-view'} = $option->{'list-sp'} =
    $option->{'list-tf'} = $option->{'list-sf'}   = $option->{'trusted'} = 0;

    # setup options here
    GetOptions($option,
      'man!',                 # display man page
      'dsn=s',                # odbc dsn or dsnless string
      'server|S=s',           # sql server
      'database|D=s',         # database
      'username|U=s',         # user name
      'password|P=s',         # password
      'trusted|T!',           # trusted connection if on windows environment
      'dbi-driver|r=s',       # dbi driver
      'list-db!',             # list all the databases for a given sql server
      'list-view!',           # list all views for a given database
      'list-table!',          # list all tables for a given database
      'list-sp!',             # list all stored procedures
      'list-tf!',             # list all table functions
      'list-sf!',             # list all all scalar functions
      'help-object!',         # provide details information regarding to a sql server object
      'output-code!',         # output code for stored procedures, views, functions (table or scalar function code)
      'output-code-format=s', # output code format
      'query=s',              # execute a query
      'execute=s',            # execute a stored procedure
      'output-format|o=s',    # output format (html_table|csv|xml|console)
      'header|h!'             # no header
    );

    # print out man page
    pod2usage( -exitstatus => 0, -verbose => 2 ) if defined $option->{man} && $option->{man};

    # set default value for some command line options
    $option->{'output-code-format'} ||= 'console'; # default output to console
    $option->{'limit-query-result'} ||= 300;       # by default only return first 300 rows

    # make sure output-code-format is what we support
    die "unknown format $option->{'output-code-format'}" if ( $option->{'output-code-format'} !~ /console|xhtml|xml/i );

    # overwrite them if command line options are given
    my $server   = $option->{server}   if defined $option->{server}   && length( $option->{server}   ) > 0;
    my $username = $option->{username} if defined $option->{username} && length( $option->{username} ) > 0;
    my $password = $option->{password} if defined $option->{password} && length( $option->{password} ) > 0;
    my $database = $option->{database} if defined $option->{database} && length( $option->{database} ) > 0;

    # if trusted option is given; otherwise use username and password pair
    if ( $option->{trusted} ) {
        $option->{dsn} = sprintf( DSN, $server, $database, 'Trusted_Connection=Yes' );
    }
    elsif ( !defined $option->{dsn} ) {
        $option->{dsn} = sprintf( DSN, $server, $database, "uid=$username;pwd=$password" );
    }
       
    # return $option hash reference back
    return $option;
}

__END__
=head1 dbbrowser.pl

sample - Using dbbrowser.pl

=head1 SYNOPSIS

dbbrowser.pl --dsn --output-format sql_statement

    Options:
      --help               print this help message
      --dsn                odbc dsn or a dsnless string
      --server   | -S      sql server name
      --database | -D      name of database
      --username | -U      login name for a given sqlserver and database
      --password | -P      password associates with login name
      --trusted  | -T      use trusted connection (only in windows environment)
      --dbi-driver | -d    a DBI drive that connects to a specific database server, i.e mysql, sqlite, ...
      --list-db            list all databases for a given database
      --list-view          list all the views for a given database
      --list-table         list all tables for a given database
      --list-sp            list all the stored procedures for a given database
      --list-tf            list all the table functions for a given database
      --list-sf            list all the scalar functions for a given database
      --help-object        provide a details information regarding to a gvien sql server object
      --output-code        output code for stored procedures, views, and functions (table/scalar functions)
      --output-code-format output code format
      --query              execute a query
      --output-format      what output you want dbbrowser.pl to generate

=head1 OPTIONS

=over 4

=item B<--help>

    print out this help message

=item B<--dsn>

    an ODBC dsn or dsnless string

=item B<--output-format>

    a format that dbbrowser will take and generated for output to screen, it
    accepts the following styles:

      * html_table - output an html table
      * csv        - output a comma sperate value 
      * xml        - output an xml

=item B<--list-db>

    list all the databases for a given database

=item B<--server|-S>

    a sql server you want to connect to

=item B<--database|-D>

    database you want to access

=item B<--username|-U>

    login for a given sql server and database

=item B<--password|-P>

    password associates with a given user

=item B<--trusted|-T>

    use trusted connection for windows environment 

=item B<--list-view>

    list all the views for a given database

=item B<--list-table>

    list all table for a given database

=item B<--list-tf>

    list all the table functions for a given database

=item B<--list-sf>

    list all the scalar functions for a given database

=item B<--help-object>

    provide a detail information regarding to a given sql server object by using sp_help stored procedure

=item B<--output-code>

    output code for stored procedures, views, and functions (table/scalar functions)

=item B<--output-code-format=format>

    output code format, the support formats are list below:

    * console - a 16 colors console output
    * xhtml   - an xhtml format
    * xml     - an xml format

=item B<--query>

    execute a query

=back

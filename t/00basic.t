#!/usr/bin/perl -w

use Test; BEGIN { plan tests => 1 };

use lib '../lib'; if (-d 't') { chdir 't'; }
use IPC::DirQueue;

mkdir ("log");
mkdir ("log/qdir");
my $bq = IPC::DirQueue->new({ dir => 'log/qdir' });
ok ($bq);

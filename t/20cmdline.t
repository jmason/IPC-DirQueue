#!/usr/bin/perl -w

use Test; BEGIN { plan tests => 7 };

use lib '../lib'; if (-d 't') { chdir 't'; }
use IPC::DirQueue;

mkdir ("log");
mkdir ("log/qdir");

open (OUT, ">log/test.dat"); print OUT "This is a test\n"; close OUT;

run ("../dq-submit --dir log/qdir log/test.dat");
ok ($? >> 8 == 0);
run ("../dq-submit --dir log/qdir log/test.dat");
ok ($? >> 8 == 0);
run ("../dq-submit --dir log/qdir log/test.dat");
ok ($? >> 8 == 0);
run ("../dq-submit --dir log/qdir log/test.dat");
ok ($? >> 8 == 0);

run ("../dq-list --dir log/qdir");
ok ($? >> 8 == 0);

run ("../dq-deque --dir log/qdir cat");
ok ($? >> 8 == 0);

run ("../dq-server --njobs 3 --dir log/qdir cat");
ok ($? >> 8 == 0);

exit;

sub run {
  print "[".join(' ',@_)."]\n";
  system @_;
}

#!/usr/bin/perl -w

use constant RUNNING_ON_WINDOWS => ($^O =~ /^(?:mswin|dos|os2)/oi);

use Test; BEGIN { plan tests => (RUNNING_ON_WINDOWS ? 0 : 7); };
exit if RUNNING_ON_WINDOWS;

use lib '../lib'; if (-d 't') { chdir 't'; }
use IPC::DirQueue;

use Config;
my $perl_path;
{
  if ($config{PERL_PATH}) {
    $perl_path = $config{PERL_PATH};
  }
  elsif ($^X =~ m|^/|) {
    $perl_path = $^X;
  }
  else {
    $perl_path = $Config{perlpath};
    $perl_path =~ s|/[^/]*$|/$^X|;
  }
}

mkdir ("log");
mkdir ("log/qdir");

open (OUT, ">log/test.dat"); print OUT "This is a test\n"; close OUT;

run ("$perl_path ../dq-submit --dir log/qdir log/test.dat");
ok ($? >> 8 == 0);
run ("$perl_path ../dq-submit --dir log/qdir log/test.dat");
ok ($? >> 8 == 0);
run ("$perl_path ../dq-submit --dir log/qdir log/test.dat");
ok ($? >> 8 == 0);
run ("$perl_path ../dq-submit --dir log/qdir log/test.dat");
ok ($? >> 8 == 0);

run ("$perl_path ../dq-list --dir log/qdir");
ok ($? >> 8 == 0);

run ("$perl_path ../dq-deque --dir log/qdir cat");
ok ($? >> 8 == 0);

run ("$perl_path ../dq-server --njobs 3 --dir log/qdir cat");
ok ($? >> 8 == 0);

exit;

sub run {
  print "[".join(' ',@_)."]\n";
  system @_;
}

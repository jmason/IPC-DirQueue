#!/usr/bin/perl -w

our $SKIP = ($^O =~ /^(mswin|dos|os2)/oi);

use Test; BEGIN { plan tests => 19 };

use lib '../lib'; if (-d 't') { chdir 't'; }
use IPC::DirQueue;

use File::Path;

rmtree ("log");
ok mkdir ("log");
ok mkdir ("log/qdir");
my $bq = IPC::DirQueue->new({ dir => 'log/qdir' });
ok ($bq);

unlink ("log/counter");

start_writer();
sleep 1;

my @pids = ();
for my $i (0 .. 1) {
  my $pid = fork();
  if ($pid) {
    push (@pids, $pid);
    ok (1);
  }
  else {
    start_worker();
  }
}

for my $i (0 .. 10) {
  sleep 1;
  my $count = (-s "log/counter");
  if (!defined $count) {
    warn "log/counter disappeared: $@ $!";
    system ("ls -l log/counter");
    die;
  }
  if ($count && $count == 10) {
    last;
  }
  print "count: $count\n";
}

ok (1);
kill (15, @pids);

for my $i (0 .. 1) {
  waitpid ($pids[$i], 0) or die "waitpid failed";
  ok (1);
}
ok (1);
exit;

use Time::HiRes qw(sleep);

sub start_worker {
  my $k = 0;
  print "worker $$: forked\n";
  while (1) {
    my $job = $bq->wait_for_queued_job();
    if (!$job) { next; }
    $k++;

    print "starting $k in $$: data: ".$job->get_data_path()."\n";
    sleep 0.1;
    open (COUNT, ">>log/counter"); print COUNT "."; close COUNT;
    $job->finish();
    print "finished $k in $$\n";
  }
}

sub start_writer {
  for my $j (1 .. 10) {
    ok ($bq->enqueue_string ("hello world! $$", { foo => "bar $$" }));
    print "cli in $$: sent $j\n";
    sleep 0.05;
  }
  print "cli in $$: finished\n";
}


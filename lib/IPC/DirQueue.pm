=head1 NAME

IPC::DirQueue - disk-based many-to-many task queue

=head1 SYNOPSIS

    my $dq = IPC::DirQueue->new({ dir => "/path/to/queue" });
    $dq->enqueue_file("filename");

    my $dq = IPC::DirQueue->new({ dir => "/path/to/queue" });
    my $job = $dq->pickup_queued_job();
    if (!$job) { print "no jobs left\n"; exit; }
    # ...do something interesting with $job->get_data_path() ...
    $job->finish();

=head1 DESCRIPTION

This module implements a FIFO queueing infrastructure, using a directory
as the communications and storage media.  No daemon process is required to
manage the queue; all communication takes place via the filesystem.

A common UNIX system design pattern is to use a tool like C<lpr> as a task
queueing system; for example,
C<http://patrick.wagstrom.net/old/weblog/archives/000128.html> describes the
use of C<lpr> as an MP3 jukebox.

However, C<lpr> isn't as efficient as it could be.  When used in this way, you
have to restart each task processor for every new task.  If you have a lot of
startup overhead, this can be very inefficient.   With C<IPC::DirQueue>, a
processing server can run persistently and cache data needed across multiple
tasks efficiently; it will not be restarted unless you restart it.

Multiple enqueueing and dequeueing processes on multiple hosts (NFS-safe
locking is used) can run simultaneously, and safely, on the same queue.

Since multiple dequeuers can run simultaneously, this provides a good way
to process a variable level of incoming tasks using a pre-defined number
of worker processes.

If you need more CPU power working on a queue, you can simply start
another dequeuer to help out.  If you need less, kill off a few dequeuers.

If you need to take down the server to perform some maintainance or
upgrades, just kill the dequeuer processes, perform the work, and start up
new ones. Since there's no 'socket' or similar point of failure aside from
the directory itself, the queue will just quietly fill with waiting jobs
until the new dequeuer is ready.

Arbitrary 'name = value' string-pair metadata can be transferred alongside data
files.   In fact, in some cases, you may find it easier to send unused and
empty data files, and just use the 'metadata' fields to transfer the details of
what will be worked on.

=head1 METHODS

=over 4

=cut

package IPC::DirQueue;
use strict;
use bytes;
use Time::HiRes qw();
use Fcntl qw(O_WRONLY O_CREAT O_EXCL O_RDONLY);
use IPC::DirQueue::Job;

our @ISA = ();

our $VERSION = 0.03;

use constant SLASH => '/';

###########################################################################

=item $dq->new ($opts);

Create a new queue object, suitable for either enqueueing jobs
or picking up already-queued jobs for processing.

C<$opts> is a reference to a hash, which may contain the following
options:

=over 4

=item dir => $path_to_directory (no default)

Name the directory where the queue files are stored.  This is required.

=item data_file_mode => $mode (default: 0666)

The C<chmod>-style file mode for data files.  This should be specified
as a string with a leading 0.  It will be affected by the current
process C<umask>.

=item queue_file_mode => $mode (default: 0666)

The C<chmod>-style file mode for queue control files.  This should be
specified as a string with a leading 0.  It will be affected by the
current process C<umask>.

=item ordered => { 0 | 1 } (default: 1)

Whether the jobs should be processed in order of submission, or
in no particular order.

=item buf_size => $number (default: 65536)

The buffer size to use when copying files, in bytes.

=back

=cut

sub new {
  my $class = shift;
  my $opts = shift;
  $opts ||= { };
  $class = ref($class) || $class;
  my $self = $opts;
  bless ($self, $class);

  die "no 'dir' specified" unless $self->{dir};
  $self->{data_file_mode} ||= '0666';
  $self->{data_file_mode} = oct ($self->{data_file_mode});
  $self->{queue_file_mode} ||= '0666';
  $self->{queue_file_mode} = oct ($self->{queue_file_mode});

  if (!defined $self->{ordered}) {
    $self->{ordered} = 1;
  }

  $self->{buf_size} ||= 65536;

  $self->{ensured_dir_exists} = { };
  $self->ensure_dir_exists ($self->{dir});

  $self;
}

###########################################################################

=item $dq->enqueue_file ($filename [, $metadata [, $pri] ] );

Enqueue a new job for processing. Returns C<1> if the job was enqueued, or
C<undef> on failure.

C<$metadata> is an optional hash reference; every item of metadata will be
available to worker processes on the C<IPC::DirQueue::Job> object, in the
C<$job-E<gt>{metadata}> hashref.  Note that using this channel for metadata
brings with it several restrictions:

=over 4

=item 1. it requires that the metadata be stored as 'name' => 'value' string pairs

=item 2. neither 'name' nor 'value' may contain newline (\r) or NUL (\0) characters

=item 3. 'name' cannot contain colon characters

=item 4. 'name' cannot start with a capital 'Q' and be 4 characters in length

=back

If those restrictions are broken, that metadatum will be silently dropped.

An optional priority can be specified; lower priorities are run first.
Priorities range from 0 to 99, and 50 is default.

=cut

sub enqueue_file {
  my ($self, $file, $metadata, $pri) = @_;
  if (!open (IN, "<$file")) {
    warn "IPC::DirQueue: cannot open $file for read: $!";
    return;
  }
  my $ret = $self->enqueue_backend ($metadata, $pri, \*IN);
  close IN;
  return $ret;
}

=item $dq->enqueue_fh ($filename [, $metadata [, $pri] ] );

Enqueue a new job for processing. Returns C<1> if the job was enqueued, or
C<undef> on failure. C<$pri> and C<$metadata> are as described in
C<$dq-E<gt>enqueue_file()>.

=cut

sub enqueue_fh {
  my ($self, $fhin, $metadata, $pri) = @_;
  my $ret = $self->enqueue_backend ($metadata, $pri, $fhin);
  close $fhin;
  return $ret;
}

=item $dq->enqueue_string ($string [, $metadata [, $pri] ] );

Enqueue a new job for processing.  The job data is entirely read from
C<$string>. Returns C<1> if the job was enqueued, or C<undef> on failure.
C<$pri> and C<$metadata> are as described in C<$dq-E<gt>enqueue_file()>.

=cut

sub enqueue_string {
  my ($self, $string, $metadata, $pri) = @_;
  return $self->enqueue_backend ($metadata, $pri, undef,
        sub {
          return $string;
        });
}

# private implementation.
sub enqueue_backend {
  my ($self, $metadata, $pri, $fhin, $callbackin) = @_;

  if (!defined($pri)) { $pri = 50; }
  if ($pri < 0 || $pri > 99) {
    warn "IPC::DirQueue: bad priority $pri is > 99 or < 0";
    return;
  }

  my ($now, $nowmsecs) = Time::HiRes::gettimeofday;

  my $job = {
    pri => $pri,
    metadata => $metadata,
    time_submitted_secs => $now,
    time_submitted_msecs => $nowmsecs
  };

  # NOTE: this can change until the moment we've renamed the ctrl file
  # into 'queue'!
  my $qfnametmp = $self->new_q_filename($job);

  my $pathtmp = $self->q_subdir('tmp');
  $self->ensure_dir_exists ($pathtmp);

  my $pathtmpctrl = $pathtmp.SLASH.$qfnametmp.".ctrl";
  my $pathtmpdata = $pathtmp.SLASH.$qfnametmp.".data";

  if (!sysopen (OUT, $pathtmpdata, O_WRONLY|O_CREAT|O_EXCL,
      $self->{data_file_mode}))
  {
    warn "IPC::DirQueue: cannot open $pathtmpdata for write: $!";
    return;
  }
  my $pathtmpdata_created = 1;

  my $siz = $self->copy_in_to_out_fh ($fhin, $callbackin,
                              \*OUT, $pathtmpdata);
  if (!defined $siz) {
    goto failure;
  }
  $job->{size_bytes} = $siz;

  # get the data dir
  my $pathdatadir = $self->q_subdir('data');

  # hashing the data dir, using 2 levels of directory hashing.  This has a tiny
  # effect on speed in all cases up to 10k queued files, but has good results
  # in terms of the usability of those dirs for users doing direct access, so
  # enabled by default.
  if (1) {
    # take the last two chars for the hashname.  In most cases, this will
    # be the last 2 chars of a hash of (hostname, pid), so effectively
    # random.  Remove it from the filename entirely, since it's redundant
    # to have it both in the dir name and the filename.
    $qfnametmp =~ s/([A-Za-z0-9+_])([A-Za-z0-9+_])$//;
    my $hash1 = $1 || '0';
    my $hash2 = $2 || '0';
    my $origdatadir = $pathdatadir;
    $pathdatadir = "$pathdatadir/$hash1/$hash2";
    # check to see if that hashdir exists... build it up if req'd
    if (!-d $pathdatadir) {
      foreach my $dir ($origdatadir, "$origdatadir/$hash1", $pathdatadir)
      {
        (-d $dir) or mkdir ($dir);
      }
    }
  }

  # now link(2) the data tmpfile into the 'data' dir.
  my $pathdata = $self->link_into_dir ($job, $pathtmpdata,
                                    $pathdatadir, $qfnametmp);
  if (!$pathdata) {
    goto failure;
  }
  my $pathdata_created = 1;
  $job->{pathdata} = $pathdata;

  # ok, write a control file now that data is safe and we know it's
  # new filename...
  if (!$self->create_control_file ($job, $pathtmpdata, $pathtmpctrl)) {
    goto failure;
  }
  my $pathtmpctrl_created = 1;

  # now link(2) that into the 'queue' dir.
  my $pathqueuedir = $self->q_subdir('queue');
  my $pathqueue = $self->link_into_dir ($job, $pathtmpctrl,
                                    $pathqueuedir, $qfnametmp);
  if (!$pathqueue) {
    goto failure;
  }

  # my $pathqueue_created = 1;     # not required, we're done!
  return 1;

failure:
  if ($pathtmpctrl_created) {
    unlink $pathtmpctrl or warn "IPC::DirQueue: cannot unlink $pathtmpctrl";
  }
  if ($pathtmpdata_created) {
    unlink $pathtmpdata or warn "IPC::DirQueue: cannot unlink $pathtmpdata";
  }
  if ($pathdata_created) {
    unlink $pathdata or warn "IPC::DirQueue: cannot unlink $pathdata";
  }
  return;
}

###########################################################################

=item $job = $dq->pickup_queued_job();

Pick up the next job in the queue, so that it can be processed.

If no job is available for processing, either because the queue is
empty or because other worker processes are already working on
them, C<undef> is returned; otherwise, a new instance of C<IPC::DirQueue::Job>
is returned.

Note that the job is marked as I<active> until C<$job-E<gt>finish()>
is called.

=cut

sub pickup_queued_job {
  my ($self) = @_;

  my $pathqueuedir = $self->q_subdir('queue');
  my $pathactivedir = $self->q_subdir('active');
  $self->ensure_dir_exists ($pathactivedir);

  my $ordered = $self->{ordered};
  my @files;
  my $dirfh;

  if ($ordered) {
    @files = $self->get_dir_filelist_sorted($pathqueuedir);
    if (scalar @files <= 0) {
      return if $self->queuedir_is_bad($pathqueuedir);
    }
  } else {
    if (!opendir ($dirfh, $pathqueuedir)) {
      return if $self->queuedir_is_bad($pathqueuedir);
      if (!opendir ($dirfh, $pathqueuedir)) {
        warn "oops? pathqueuedir bad"; return;
      }
    }
  }

  my $nextfile;
  while (1) {
    my $pathtmpactive;

    if ($ordered) {
      $nextfile = shift @files;
    } else {
      $nextfile = readdir($dirfh);
    }

    if (!$nextfile) {
      # no more files in the queue, return empty
      last;
    }

    next if ($nextfile !~ /^\d/);
    my $pathactive = $pathactivedir.SLASH.$nextfile;

    my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
        $atime,$mtime,$ctime,$blksize,$blocks) = lstat($pathactive);

    if (defined $mtime) {
      # *DO* call time() here.  In extremely large dirs, it may take
      # several seconds to traverse the entire listing from start
      # to finish!
      if (time() - $mtime < 600) {
        # active lockfile; it's being worked on.  skip this file
        next;
      }

      if ($self->worker_still_working($pathactive)) {
        # worker is still alive, although not updating the lock
        next;
      }

      # now, we want to try to avoid 2 or 3 dequeuers removing
      # the lockfile simultaneously, as that could cause this race:
      #
      # dqproc1: [checks file]        [unlinks] [starts work]
      # dqproc2:        [checks file]                         [unlinks]
      #
      # ie. the second process unlinks the first process' brand-new
      # lockfile!
      #
      # to avoid this, use a random "fudge" on the timeout, so
      # that dqproc2 will wait for possibly much longer than
      # dqproc1 before it decides to unlink it.
      #
      # this isn't perfect.  TODO: is there a "rename this fd" syscall?

      my $fudge = get_random_int() % 255;
      if (time() - $mtime < 600+$fudge) {
        # within the fudge zone.  don't unlink it in this process.
        next;
      }

      # else, we can kill the stale lockfile
      unlink $pathactive;
      warn "IPC::DirQueue: killed stale lockfile: $pathactive";
    }

    # ok, we're free to get cracking on this file.
    my $pathtmp = $self->q_subdir('tmp');
    $self->ensure_dir_exists ($pathtmp);

    # use the name of the queue file itself, plus a tmp prefix, plus active
    $pathtmpactive = $pathtmp.SLASH.
                $nextfile.".".$self->new_lock_filename().".active";

    if (!sysopen (LOCK, $pathtmpactive, O_WRONLY|O_CREAT|O_EXCL,
        $self->{queue_file_mode}))
    {
      # could have contention; skip this file
      # warn "IPC::DirQueue: cannot open $pathtmpactive for write: $!";
      next;
    }
    print LOCK $self->gethostname(), "\n", $$, "\n";
    close LOCK;

    my $pathqueue = $pathqueuedir.SLASH.$nextfile;

    my $job = IPC::DirQueue::Job->new ($self, {
      jobid => $nextfile,
      pathqueue => $pathqueue,
      pathactive => $pathactive
    });

    my $pathnewactive = $self->link_into_dir_no_retry ($job,
                        $pathtmpactive, $pathactivedir, $nextfile);
    if (!$pathnewactive) {
      # link failed; another worker got it before we did
      goto nextfile;
    }

    if ($pathactive ne $pathnewactive) {
      die "oops! active paths differ: $pathactive $pathnewactive";
    }

    if (!open (IN, "<".$pathqueue)) {
      # warn "IPC::DirQueue: cannot open $pathqueue for read: $!";
      goto nextfile;
    }

    if (!$self->read_control_file ($job, \*IN)) {
      close IN;
      next;
    }
    close IN;

    closedir($dirfh) unless $ordered;
    return $job;

nextfile:
    unlink ($pathtmpactive);  # remove the tmp file
  }

  closedir($dirfh) unless $ordered;
  return;   # empty
}

###########################################################################

=item $job = $dq->wait_for_queued_job ([ $timeout [, $pollinterval] ]);

Wait for a job to be queued within the next C<$timeout> seconds.

If there is already a job ready for processing, this will return immediately.
If one is not available, it will sleep, wake up periodically, check for job
availabilty, and either carry on sleeping or return the new job if one
is now available.

If a job becomes available, a new instance of C<IPC::DirQueue::Job> is
returned. If the timeout is reached, C<undef> is returned.

If C<$timeout> is not specified, or is less than 1, this function will wait
indefinitely.

The optional parameter C<$pollinterval> indicates how frequently to wake
up and check for new jobs.  It is specified in seconds, and floating-point
precision is supported.  The default is C<1>.

Note that if C<$timeout> is not a round multiple of C<$pollinterval>,
the nearest round multiple of C<$pollinterval> greater than C<$timeout>
will be used instead.  Also note that C<$timeout> is used as an integer.

=cut

sub wait_for_queued_job {
  my ($self, $timeout, $pollintvl) = @_;

  my $finishtime;
  if ($timeout && $timeout > 0) {
    $finishtime = time + int ($timeout);
  }

  if ($pollintvl) {
    $pollintvl *= 1000000;  # from secs to usecs
  } else {
    $pollintvl = 1000000;   # default: 1 sec
  }

  my $pathqueuedir = $self->q_subdir('queue');
  $self->ensure_dir_exists ($pathqueuedir);

  # TODO: would be nice to use fam for this, where available.  But
  # no biggie...

  while (1) {
    # check the stat time on the queue dir *before* we call pickup,
    # to avoid a race condition where a job is added while we're
    # checking in that function.
    my @stat = stat ($pathqueuedir);
    my $qdirlaststat = $stat[9];

    my $job = $self->pickup_queued_job();
    if ($job) { return $job; }

    # sleep until the directory's mtime changes from what it was when
    # we ran pickup_queued_job() last.
    while (1) {
      my $now = time;
      if ($finishtime && $now >= $finishtime) {
        return undef;           # out of time
      }

      Time::HiRes::usleep ($pollintvl);

      my @stat = stat ($pathqueuedir);
      if ($stat[9] != $qdirlaststat) {
        last;                   # activity! try a pickup_queued_job() call
      }
    }
  }
}

###########################################################################

=item $job = $dq->visit_all_jobs($visitor, $visitcontext);

Visit all the jobs in the queue, in a read-only mode.  Used to list
the entire queue.

The callback function C<$visitor> will be called for each job in
the queue, like so:

  &$visitor ($visitcontext, $job);

C<$visitcontext> is whatever you pass in that variable above.
C<$job> is a new, read-only instance of C<IPC::DirQueue::Job> representing
that job.

If a job is active (being processed), the C<$job> object also contains the
following additional data:

  'active_host': the hostname on which the job is active
  'active_pid': the process ID of the process which picked up the job

=cut

sub visit_all_jobs {
  my ($self, $visitor, $visitcontext) = @_;

  my $pathqueuedir = $self->q_subdir('queue');
  my $pathactivedir = $self->q_subdir('active');

  my $ordered = $self->{ordered};
  my @files;
  my $dirfh;

  if ($ordered) {
    @files = $self->get_dir_filelist_sorted($pathqueuedir);
    if (scalar @files <= 0) {
      return if $self->queuedir_is_bad($pathqueuedir);
    }
  } else {
    if (!opendir ($dirfh, $pathqueuedir)) {
      return if $self->queuedir_is_bad($pathqueuedir);
      if (!opendir ($dirfh, $pathqueuedir)) {
        warn "oops? pathqueuedir bad"; return;
      }
    }
  }

  my $nextfile;
  while (1) {
    if ($ordered) {
      $nextfile = shift @files;
    } else {
      $nextfile = readdir($dirfh);
    }

    if (!$nextfile) {
      # no more files in the queue, return empty
      last;
    }

    next if ($nextfile !~ /^\d/);
    my $pathqueue = $pathqueuedir.SLASH.$nextfile;
    my $pathactive = $pathactivedir.SLASH.$nextfile;

    my $acthost;
    my $actpid;
    if (open (IN, "<$pathactive")) {
      $acthost = <IN>; chomp $acthost;
      $actpid = <IN>; chomp $actpid;
      close IN;
    }

    my $job = IPC::DirQueue::Job->new ($self, {
      is_readonly => 1,     # means finish() will not rm files
      jobid => $nextfile,
      active_host => $acthost,
      active_pid => $actpid,
      pathqueue => $pathqueue,
      pathactive => $pathactive
    });

    if (!open (IN, "<".$pathqueue)) {
      next;      # queue file has disappeared (job finished)
    }

    if (!$self->read_control_file ($job, \*IN)) {
      warn "IPC::DirQueue: cannot read control file: $pathqueue";
      close IN; next;
    }
    close IN;

    &$visitor ($visitcontext, $job);
  }

  closedir($dirfh) unless $ordered;
}

###########################################################################

# private API: performs logic of IPC::DirQueue::Job::finish().
sub finish_job {
  my ($self, $job, $isdone) = @_;

  if ($job->{is_readonly}) {
    return;
  }

  if ($isdone) {
    unlink ($job->{pathqueue});
    unlink ($job->{QDFN});
  }

  unlink ($job->{pathactive});
}

###########################################################################

sub get_dir_filelist_sorted {
  my ($self, $dir) = @_;

  if (!opendir (DIR, $dir)) {
    return ();          # no dir?  nothing queued
  }
  # have to read the lot, to sort them.
  my @files = sort { $a cmp $b } grep { /^\d/ } readdir(DIR);
  closedir DIR;
  return @files;
}

###########################################################################

sub copy_in_to_out_fh {
  my ($self, $fhin, $callbackin, $fhout, $outfname) = @_;

  my $buf;
  my $len;
  my $siz = 0;

  binmode $fhout;
  if ($callbackin) {
    my $stringin = $callbackin->();
    $len = length ($stringin);
    if (!syswrite ($fhout, $stringin, $len)) {
      warn "IPC::DirQueue: cannot write to $outfname: $!";
      close $fhout;
      return;
    }
    $siz += $len;
  }
  else {
    binmode $fhin;
    while (($len = read ($fhin, $buf, $self->{buf_size})) > 0) {
      if (!syswrite ($fhout, $buf, $len)) {
        warn "IPC::DirQueue: cannot write to $outfname: $!";
        close $fhin; close $fhout;
        return;
      }
      $siz += $len;
    }
    close $fhin;
  }

  if (!close $fhout) {
    warn "IPC::DirQueue: cannot close $outfname";
    return;
  }
  return $siz;
}

sub link_into_dir {
  my ($self, $job, $pathtmp, $pathlinkdir, $qfname) = @_;
  $self->ensure_dir_exists ($pathlinkdir);
  my $path;

  # retry 10 times; add a random few digits on link(2) failure
  my $maxretries = 10;
  for my $retry (1 .. $maxretries) {
    $path = $pathlinkdir.SLASH.$qfname;

    if (link ($pathtmp, $path)) {
      last; # got it
    }

    # link() may return failure, even if it succeeded.
    # use lstat() to verify that link() really failed.
    my ($dev,$ino,$mode,$nlink,$uid) = lstat($pathtmp);
    if ($nlink == 2) {
      last; # got it
    }

    # failed.  check for retry limit first
    if ($retry == $maxretries) {
      warn "IPC::DirQueue: cannot link $pathtmp to $path";
      return;
    }

    # try a new q_filename, use randomness to avoid
    # further collisions
    $qfname = $self->new_q_filename($job, 1);

    Time::HiRes::usleep (250 * $retry);
  }

  # got it! unlink(2) the tmp file, since we don't need it.
  if (!unlink ($pathtmp)) {
    warn "IPC::DirQueue: cannot unlink $pathtmp";
    # non-fatal, we can still continue anyway
  }

  return $path;
}

sub link_into_dir_no_retry {
  my ($self, $job, $pathtmp, $pathlinkdir, $qfname) = @_;
  $self->ensure_dir_exists ($pathlinkdir);

  my $path = $pathlinkdir.SLASH.$qfname;
  if (!link ($pathtmp, $path))
  {
    # link() may return failure, even if it succeeded.
    # use lstat() to verify that link() really failed.
    my ($dev,$ino,$mode,$nlink,$uid) = lstat($pathtmp);
    if ($nlink != 2) {
      return;   # ok, it really failed
    }
  }

  # got it! unlink(2) the tmp file, since we don't need it.
  if (!unlink ($pathtmp)) {
    warn "IPC::DirQueue: cannot unlink $pathtmp";
    # non-fatal, we can still continue anyway
  }

  return $path;
}

sub create_control_file {
  my ($self, $job, $pathtmpdata, $pathtmpctrl) = @_;

  if (!sysopen (OUT, $pathtmpctrl, O_WRONLY|O_CREAT|O_EXCL,
      $self->{queue_file_mode}))
  {
    warn "IPC::DirQueue: cannot open $pathtmpctrl for write: $!";
    return;
  }

  print OUT "QDFN: ", $job->{pathdata}, "\n";
  print OUT "QDSB: ", $job->{size_bytes}, "\n";
  print OUT "QSTT: ", $job->{time_submitted_secs}, "\n";
  print OUT "QSTM: ", $job->{time_submitted_msecs}, "\n";
  print OUT "QSHN: ", $self->gethostname(), "\n";

  my $md = $job->{metadata};
  foreach my $k (keys %{$md}) {
    my $v = $md->{$k};
    next if ($k =~ /^Q...$/);
    next if ($k =~ /[:\0\n]/);
    next if ($v =~ /[\0\n]/);
    print OUT $k, ": ", $v, "\n";
  }

  if (!close (OUT)) {
    warn "IPC::DirQueue: cannot close $pathtmpctrl for write: $!";
    return;
  }

  return 1;
}

sub read_control_file {
  my ($self, $job) = @_;
  local ($_);

  if (!open (IN, "<".$job->{pathqueue})) {
    warn "IPC::DirQueue: cannot open $job->{pathqueue} for read: $!";
    return;
  }

  while (<IN>) {
    my ($k, $value) = split (/: /, $_, 2);
    chop $value;
    if ($k =~ /^Q[A-Z]{3}$/) {
      $job->{$k} = $value;
    }
    else {
      $job->{metadata}->{$k} = $value;
    }
  }
  close IN;

  # all jobs must have a datafile (even if it's empty)
  if (!$job->{QDFN} || !-f $job->{QDFN}) {
    return;
  }

  return $job;
  # print OUT "QDFN: ", $job->{pathdata}, "\n";
  # print OUT "QDSB: ", $job->{size_bytes}, "\n";
  # print OUT "QSTT: ", $job->{time_submitted_secs}, "\n";
  # print OUT "QSTM: ", $job->{time_submitted_msecs}, "\n";
  # print OUT "QSHN: ", $self->gethostname(), "\n";
}

sub worker_still_working {
  my ($self, $fname);
  if (!$fname) {
    return;
  }
  if (!open (IN, "<".$fname)) {
    return;
  }
  my $hname = <IN>; chomp $hname;
  my $wpid = <IN>; chomp $wpid;
  close IN;
  if ($hname eq $self->gethostname()) {
    if (kill (0, $wpid)) {
      return 1;         # pid is local, and still running.
    }
  }
  return;               # pid is no longer running, or remote
}

###########################################################################

sub q_dir {
  my ($self) = @_;
  return $self->{dir};
}

sub q_subdir {
  my ($self, $subdir) = @_;
  return $self->q_dir().SLASH.$subdir;
}

sub new_q_filename {
  my ($self, $job, $addextra) = @_;

  my @gmt = gmtime ($job->{time_submitted_secs});

  # NN20040718140300MMMM.hash(hostname.$$)[.rand]
  #
  # NN = priority, default 50
  # MMMM = microseconds from Time::HiRes::gettimeofday()
  # hostname = current hostname

  my $buf = sprintf ("%02d.%04d%02d%02d%02d%02d%02d%06d.%s",
        $job->{pri},
        $gmt[5]+1900, $gmt[4]+1, $gmt[3], $gmt[2], $gmt[1], $gmt[0],
        $job->{time_submitted_msecs},
        hash_string_to_filename ($self->gethostname().$$));

  # normally, this isn't used.  but if there's a collision,
  # all retries after that will do this; in this case, the
  # extra anti-collision stuff is useful
  if ($addextra) {
    $buf .= ".".$$.".".$self->get_random_int();
  }

  return $buf;
}

sub hash_string_to_filename {
  my ($str) = @_;
  # get a 16-bit checksum of the input, then uuencode that string
  $str = pack ("u*", unpack ("%16C*", $str));
  # transcode from uuencode-space into safe, base64-ish space
  $str =~ y/ -_/A-Za-z0-9+_/;
  # and remove the stuff that wasn't in that "safe" range
  $str =~ y/A-Za-z0-9+_//cd;
  return $str;
}

sub new_lock_filename {
  my ($self) = @_;
  return sprintf ("%d.%s.%d", time, $self->gethostname(), $$);
}

sub get_random_int {
  my ($self) = @_;

  # we try to use /dev/random first, as that's globally random for all PIDs on
  # the system.  this avoids brokenness if the caller has called srand(), then
  # forked multiple enqueueing procs, as they will all share the same seed and
  # will all return the same "random" output.
  my $buf;
  if (sysopen (IN, "</dev/random", O_RDONLY) && read (IN, $buf, 2)) {
    my ($hi, $lo) = unpack ("C2", $buf);
    return ($hi << 8) | $lo;
  } else {
    # fall back to plain old rand(), use perl's implicit srand() call,
    # and hope caller hasn't called srand() yet in a parent process.
    return int rand (65536);
  }
}

sub gethostname {
  my ($self) = @_;

  my $hname = $self->{myhostname};
  return $hname if $hname;

  # try using Sys::Hostname. may fail on non-UNIX platforms
  eval '
    use Sys::Hostname;
    $self->{myhostname} = hostname;     # cache the result
  ';

  # could have failed.  supply a default in that case
  $self->{myhostname} ||= 'nohost';

  return $self->{myhostname};
}

sub ensure_dir_exists {
  my ($self, $dir) = @_;
  return if exists ($self->{ensured_dir_exists}->{$dir});
  $self->{ensured_dir_exists}->{$dir} = 1;
  (-d $dir) or mkdir($dir);
}

sub queuedir_is_bad {
  my ($self, $pathqueuedir) = @_;

  # try creating the dir; it may not exist yet
  $self->ensure_dir_exists ($pathqueuedir);
  if (!opendir (RETRY, $pathqueuedir)) {
    # still can't open it! problem
    warn "IPC::DirQueue: cannot open queue dir \"$pathqueuedir\": $!\n";
    return 1;
  }
  # otherwise, we could open it -- it just needed to be created.
  closedir RETRY;
  return 0;
}

###########################################################################

1;

=back

=head1 STALE LOCKS AND SIGNAL HANDLING

If interrupted or terminated, dequeueing processes should be careful to
either call C<$job-E<gt>finish()> or C<$job-E<gt>return_to_queue()> on any active
tasks before exiting -- otherwise those jobs will remain marked I<active>.

Stale locks are normally dealt with automatically.  If a lock is still
I<active> after about 10 minutes of inactivity, the other dequeuers on
that machine will probe the process ID listed in that lock file using
C<kill(0)>.  If that process ID is no longer running, the lock is presumed
likely to be stale. If a given timeout (10 minutes plus a random value
between 0 and 256 seconds) has elapsed since the lock file was last
modified, the lock file is deleted.

Note: this means that if the dequeueing processes are spread among
multiple machines, and there is no longer a dequeuer running on the
machine that initially 'locked' the task, it will never be unlocked.

=head1 SEE ALSO

C<IPC::DirQueue::Job>

=head1 AUTHOR

Justin Mason E<lt>dq /at/ jmason.orgE<gt>

=head1 COPYRIGHT

C<IPC::DirQueue> is distributed under the same license as perl itself.

=head1 AVAILABILITY

The latest version of this library is likely to be available from CPAN.

